"""Task worker class that manages the execution of a task.

Triggered by a Slurm script run by the dispatch component.
"""

import argparse
import importlib
from typing import Optional

from distributed_job_coordination.coordinator.storage import StorageManager
from distributed_job_coordination.lib import s3_utils
from distributed_job_coordination.lib.job_config import JobConfig
from distributed_job_coordination.lib.job_state import TaskState, TaskStatus
from distributed_job_coordination.lib.dynamodb_utils import TaskStateStore
from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__name__)
task_state_store = TaskStateStore()


class TaskWorker:
    """Worker class that manages the task.

    This includes loading the task state, running the handler corresponding
    to the task, and updating the task state as necessary.
    """

    def __init__(self, task_state: TaskState):
        self.task_state = task_state
        self.task_items = self.load_task_data()
        self.storage_manager = StorageManager(
            job_id=self.task_state.job_id,
            job_name=self.task_state.job_name,
        )
        self.config: JobConfig = s3_utils.download_job_config(
            job_name=self.task_state.job_name,
            job_id=self.task_state.job_id,
        )
        self.handler_kwargs = self.config.handler_kwargs
        self.task_output_queue_prefix = self.config.output.task_output_queue_prefix

    def load_task_data(self) -> list[dict]:
        """Load in the data for the task."""
        items = self.storage_manager.load_batch_from_scratch(
            task_id=self.task_state.id,
            filename_prefix="insert_batch",
        )
        logger.info(
            f"Loaded {len(items)} items for task {self.task_state.id} from input scratch queue."
        )
        return items

    def run(self):
        handler = importlib.import_module(self.task_state.handler)
        task_output_queue: Queue = self.storage_manager.return_temp_task_output_queue(
            task_id=self.task_state.id,
            task_output_queue_prefix=self.task_output_queue_prefix,
            create_new_queue=True,
        )
        try:
            event = {
                "items": self.task_items,
                "task_state": self.task_state.model_dump(),
                "task_output_queue": task_output_queue,
                **self.handler_kwargs,
            }
            handler.handler(event)
            self.update_task_state(status=TaskStatus.COMPLETED)
        except Exception as e:
            logger.error(f"Error running task {self.task_state.id}: {e}")
            self.update_task_state(status=TaskStatus.FAILED, error=str(e))
            raise e

    def update_task_state(self, status: TaskStatus, error: Optional[str] = None):
        """Updates the state of the task."""
        fields_to_update = {"status": status.value}
        if error:
            fields_to_update["error"] = error
        task_state_store.update_task_state(self.task_state.id, fields_to_update)
        logger.info(
            f"Updated task state for task {self.task_state.id} to {status.value}."
        )


def kickoff_task_worker(task_id: str):
    """Kicks off a task worker. Loads the task state and runs the task."""
    task_state = task_state_store.load_task_state(task_id=task_id)
    task_worker = TaskWorker(task_state=task_state)
    task_worker.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Task worker for distributed job coordination"
    )
    parser.add_argument("--job_name", required=True, help="Name of the job")
    parser.add_argument("--job_id", required=True, help="ID of the job")
    parser.add_argument("--task_id", required=True, help="ID of the task to execute")
    args = parser.parse_args()

    kickoff_task_worker(
        job_name=args.job_name, job_id=args.job_id, task_id=args.task_id
    )

    logger.info(
        f"Task worker for job {args.job_id} and task {args.task_id} kicked off."
    )
