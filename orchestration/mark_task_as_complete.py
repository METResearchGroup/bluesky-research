"""Manually mark a Prefect task as complete."""

from prefect import get_client
from prefect.states import Completed
import asyncio

async def mark_task_completed(task_run_id):
    async with get_client() as client:
        await client.set_task_run_state(
            task_run_id=task_run_id,
            state=Completed()
        )
        print(f"Task run {task_run_id} has been marked as completed.")

task_run_ids = [
    "a63169de-e5d7-40b7-8585-28a4882c63d4",
    "a6e2471f-4e57-4f08-827d-0bb6810b5a0a"
]

for task_run_id in task_run_ids:
    asyncio.run(mark_task_completed(task_run_id))
