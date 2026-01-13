import asyncio
import json
import logging
import os
import time
from datetime import datetime

import redis.asyncio as redis
from redis.exceptions import WatchError

from hdx.resource.changedetection.name_generator import generate_random_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(self, task_length: int = 1):
        redis_url = os.getenv(
            "REDIS_CONNECTION_URL",
            "redis://localhost:6379/0?decode_responses=True",
        )
        self.instance_id: str = generate_random_id()
        self.redis_client: redis.Redis = redis.from_url(
            redis_url,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30,
            max_connections=5,
        )
        self.tasks: list[str] = self.generate_tasks(task_length)
        self._event_loop = asyncio.new_event_loop()

        logging.info(f"TaskManager initialized with instance_id: {self.instance_id}")

    @staticmethod
    def generate_tasks(task_length: int = 1) -> list[str]:
        """Generate a list of task identifiers as hex strings"""
        return [f"{i:0{task_length}x}" for i in range(16**task_length)]

    @staticmethod
    def _format_timestamp(timestamp: int) -> str:
        """Convert Unix timestamp to human-readable UTC format"""
        return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S UTC")

    async def acquire_task(self) -> str | None:
        """Try to acquire a task atomically using WATCH/MULTI/EXEC. Returns the hex code if successful."""
        now = int(time.time())
        for task in self.tasks:
            key = f"task:{task}"
            pipeline = None

            try:
                # Use WATCH/MULTI/EXEC for atomic task acquisition
                # Create pipeline and watch the key
                pipeline = self.redis_client.pipeline()
                await pipeline.watch(key)
                task_data = await self.redis_client.hgetall(key)
                start_time = int(task_data.get("start_time", 0))
                finish_time = int(task_data.get("finish_time", 0))
                lock_owner = task_data.get("lock")
                # last_progress_time = int(task_data.get("last_progress_time", 0))

                # Skip tasks finished within the last day, but allow reprocessing of older finished tasks
                if finish_time > 0 and now - finish_time < 24 * 60 * 60:
                    continue

                # Determine if we can acquire this task
                can_acquire = False
                mapping = {}
                log_message = ""

                # If no one holds the task, we can take it
                if not lock_owner:
                    can_acquire = True
                    mapping = {
                        "lock": self.instance_id,
                        "start_time": now,
                        "start_time_readable": self._format_timestamp(now),
                        "last_progress_time": now,
                        "last_progress_time_readable": self._format_timestamp(now),
                    }
                    log_message = f"Instance {self.instance_id} acquired task {task}"

                # Check if task started more than 1 day ago (stale task)
                elif now - start_time > 24 * 60 * 60:
                    can_acquire = True
                    mapping = {
                        "lock": self.instance_id,
                        "start_time": now,
                        "start_time_readable": self._format_timestamp(now),
                        "last_progress_time": now,
                        "last_progress_time_readable": self._format_timestamp(now),
                    }
                    log_message = f"Instance {self.instance_id} stole stale task {task}"

                # Check if progress is stale (>2h)
                # elif now - last_progress_time > 2 * 3600:
                #     can_acquire = True
                #     mapping = {
                #         "lock": self.instance_id,
                #         "last_progress_time": now,
                #     }
                #     log_message = f"Instance {self.instance_id} took over task {task} due to stale progress"

                if can_acquire:
                    # Start atomic transaction using the same pipeline
                    pipeline.multi()
                    pipeline.hset(key, mapping=mapping)
                    pipeline.expire(key, 7 * 24 * 60 * 60)  # Set TTL to 1 week
                    _ = await pipeline.execute()

                    # Successfully acquired the task atomically
                    logger.info(log_message)
                    return task

            except WatchError:
                # Transaction was aborted due to concurrent modification
                logger.warning(
                    f"Transaction aborted, concurrent modification for task {task} - moving on"
                )
                continue  # Move to next task
            except Exception as e:
                # Log Redis errors and fail fast
                logger.error(f"Redis error while trying to acquire task {task}: {e}")
                raise
            finally:
                # Always ensure pipeline is properly closed
                if pipeline is not None:
                    try:
                        await pipeline.reset()
                    except Exception:
                        pass

        return None

    async def update_progress(self, task: str, progress: dict) -> None:
        """Update task progress. Since only the task owner calls this, no atomic protection needed."""
        key = f"task:{task}"
        now = int(time.time())
        mapping = {
            "progress": json.dumps(progress),
            "last_progress_time": now,
            "last_progress_time_readable": self._format_timestamp(now),
        }

        try:
            await self.redis_client.hset(key, mapping=mapping)
            await self.redis_client.expire(key, 7 * 24 * 60 * 60)  # Set TTL to 1 week
            logger.info(f"Instance {self.instance_id} updated progress for task {task}")
        except Exception as e:
            logger.error(f"Redis error while updating progress for task {task}: {e}")
            raise

    async def finish_task(self, task: str) -> None:
        """Mark task as finished. Since only the task owner calls this, no atomic protection needed."""
        key = f"task:{task}"
        now = int(time.time())

        try:
            # Optional: Verify ownership before finishing (defensive programming)
            task_data = await self.redis_client.hgetall(key)
            current_owner = task_data.get("lock")

            if current_owner and current_owner != self.instance_id:
                logger.warning(
                    f"Cannot finish task {task} - not owned by this instance"
                )
                return

            mapping = {
                "finish_time": now,
                "finish_time_readable": self._format_timestamp(now),
            }
            await self.redis_client.hset(key, mapping=mapping)
            await self.redis_client.expire(key, 7 * 24 * 60 * 60)  # Set TTL to 1 week
            logger.info(f"Instance {self.instance_id} finished task {task}")

        except Exception as e:
            logger.error(f"Redis error while finishing task {task}: {e}")
            raise

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

    def sync_acquire_task(self) -> str | None:
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
