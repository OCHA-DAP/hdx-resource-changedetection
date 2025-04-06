import json
import time

import pytest
import pytest_asyncio

from hdx.resource.changedetection.task_manager import TaskManager


@pytest_asyncio.fixture
async def task_manager() -> TaskManager:
    # Create a TaskManager instance.
    manager = TaskManager(task_length=1)
    # Flush the Redis DB to start fresh.
    await manager.redis_client.flushall()
    yield manager
    # Clean up: flush and close connection.
    await manager.redis_client.flushall()
    await manager.redis_client.close()


@pytest.mark.needs_redis
@pytest.mark.asyncio
async def test_generate_tasks():
    tasks = TaskManager.generate_tasks(2)
    # Expect 256 tasks from 00 to ff.
    assert tasks[0] == "00"
    assert tasks[-1] == "ff"
    assert len(tasks) == 256


@pytest.mark.needs_redis
@pytest.mark.asyncio
async def test_acquire_task_available(task_manager: TaskManager):
    await task_manager.redis_client.flushall()
    task = await task_manager.acquire_task()
    assert task is not None
    key = f"task:{task}"
    data = await task_manager.redis_client.hgetall(key)
    # The lock should be set to the manager's instance_id.
    assert data.get("lock") == task_manager.instance_id


@pytest.mark.needs_redis
@pytest.mark.asyncio
async def test_acquire_task_finished(task_manager: TaskManager):
    await task_manager.redis_client.flushall()
    now = int(time.time())
    # Mark every task as finished.
    for task in task_manager.tasks:
        key = f"task:{task}"
        await task_manager.redis_client.hset(key, mapping={"finish_time": now})
    task = await task_manager.acquire_task()
    # No task should be available.
    assert task is None


@pytest.mark.needs_redis
@pytest.mark.asyncio
async def test_update_and_finish(task_manager: TaskManager):
    await task_manager.redis_client.flushall()
    task = await task_manager.acquire_task()
    assert task is not None

    # Test progress update.
    await task_manager.update_progress(task, {"step": 1})
    key = f"task:{task}"
    data = await task_manager.redis_client.hgetall(key)
    progress = json.loads(data.get("progress", "{}"))
    assert progress.get("step") == 1

    # Test finishing the task.
    await task_manager.finish_task(task)
    data = await task_manager.redis_client.hgetall(key)
    assert "finish_time" in data


#
# @pytest.mark.asyncio
# async def test_perform_task(task_manager: TaskManager):
#     await task_manager.redis_client.flushall()
#     task = await task_manager.acquire_task()
#     assert task is not None
#
#     await task_manager.perform_task(task)
#     key = f"task:{task}"
#     data = await task_manager.redis_client.hgetall(key)
#     # The task should be marked as finished.
#     assert "finish_time" in data
#     # Check progress: perform_task loops 5 times (0 to 4).
#     progress = json.loads(data.get("progress", "{}"))
#     assert progress.get("step") == 4
