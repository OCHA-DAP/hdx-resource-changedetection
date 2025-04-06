import asyncio
import json
import logging
import os
import time
import uuid
from typing import Dict, List, Optional

import redis.asyncio as redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(self, task_length: int = 1):
        redis_url = os.getenv(
            "REDIS_CONNECTION_URL",
            "redis://localhost:6379/0?decode_responses=True",
        )
        self.instance_id: str = str(uuid.uuid4())
        self.redis_client: redis.Redis = redis.from_url(redis_url)
        self.tasks: List[str] = self.generate_tasks(task_length)
        self._event_loop = asyncio.new_event_loop()

    @staticmethod
    def generate_tasks(task_length: int = 1) -> List[str]:
        """Generate a list of task identifiers as hex strings"""
        return [f"{i:0{task_length}x}" for i in range(16**task_length)]

    async def acquire_task(self) -> Optional[str]:
        """Try to acquire a task. Returns the hex code if successful."""
        now = int(time.time())
        for task in self.tasks:
            key = f"task:{task}"
            task_data = await self.redis_client.hgetall(key)
            start_time = int(task_data.get("start_time", 0))
            finish_time = task_data.get("finish_time")
            lock_owner = task_data.get("lock")
            last_progress_time = int(task_data.get("last_progress_time", 0))

            if finish_time:
                continue

            # If no one holds the task, or lock is old, we take it
            if not lock_owner:
                pipeline = self.redis_client.pipeline()
                pipeline.hset(
                    key,
                    mapping={
                        "lock": self.instance_id,
                        "start_time": now,
                        "last_progress_time": now,
                    },
                )
                await pipeline.execute()
                logger.info(
                    f"Instance {self.instance_id} acquired task {task}"
                )
                return task

            # Check if task started more than 1 day ago
            if now - start_time > 24 * 60 * 60:
                pipeline = self.redis_client.pipeline()
                pipeline.hset(
                    key,
                    mapping={
                        "lock": self.instance_id,
                        "start_time": now,
                        "last_progress_time": now,
                    },
                )
                await pipeline.execute()
                logger.info(
                    f"Instance {self.instance_id} stole stale task {task}"
                )
                return task

            # Check if progress is stale (>2h)
            if now - last_progress_time > 2 * 3600:
                pipeline = self.redis_client.pipeline()
                # Keep the original start_time for continuity
                pipeline.hset(
                    key,
                    mapping={
                        "lock": self.instance_id,
                        "last_progress_time": now,
                    },
                )
                await pipeline.execute()
                logger.info(
                    f"Instance {self.instance_id} took over task {task} due to stale progress"
                )
                return task

        return None

    async def update_progress(self, task: str, progress: Dict) -> None:
        key = f"task:{task}"
        now = int(time.time())
        mapping = {"progress": json.dumps(progress), "last_progress_time": now}
        await self.redis_client.hset(key, mapping=mapping)
        logger.info(
            f"Instance {self.instance_id} updated progress for task {task}"
        )

    async def finish_task(self, task: str) -> None:
        key = f"task:{task}"
        now = int(time.time())
        await self.redis_client.hset(key, "finish_time", now)
        logger.info(f"Instance {self.instance_id} finished task {task}")

    # async def perform_task(self, task: str) -> None:
    #     """
    #     Simulate task processing with periodic progress updates.
    #     Replace the loop with the actual work you need to do.
    #     """
    #     progress: Dict = {}
    #     for step in range(5):
    #         progress["step"] = step
    #         await self.update_progress(task, progress)
    #         await asyncio.sleep(1)  # simulate work
    #     await self.finish_task(task)

    def sync_acquire_task(self) -> Optional[str]:
        task_code = self._event_loop.run_until_complete(self.acquire_task())
        return task_code

    def sync_finish_task(self, task: str) -> None:
        self._event_loop.run_until_complete(self.finish_task(task))

    # async def main_loop(self) -> None:
    #     """Continuously acquire and perform tasks."""
    #     while True:
    #         task = await self.acquire_task()
    #         if task:
    #             await self.perform_task(task)
    #         else:
    #             logger.info("No task available, sleeping")
    #             await asyncio.sleep(5)


# if __name__ == "__main__":
#     async def main() -> None:
#         manager = TaskManager(task_length=1)
#         await manager.main_loop()
#
#
#     asyncio.run(main())
