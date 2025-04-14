"""Manages temporary data storage during the course of the job.

Manages I/O to/from local scratch directory.
"""

import os

from distributed_job_coordination.coordinator.constants import (
    root_job_export_key,
    root_scratch_dir,
)
from distributed_job_coordination.lib.job_state import TaskState
from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__name__)


class StorageManager:
    """Manages storage operations for temporary data during the course of the job run."""

    def __init__(self, job_id: str, job_name: str):
        self.root_scratch_dir = root_scratch_dir
        self.job_id = job_id
        self.job_name = job_name
        self.job_scratch_path = self.get_scratch_path_for_job(job_name, job_id)

    def get_scratch_path_for_job(self, job_name: str, job_id: str) -> str:
        """Returns the scratch path for a job."""
        return os.path.join(
            self.root_scratch_dir,
            root_job_export_key.format(job_name=job_name, job_id=job_id),
        )

    def get_scratch_path_for_task(self, job_scratch_path: str, task_id: str) -> str:
        return os.path.join(
            job_scratch_path,
            f"task_id={task_id}",
        )

    def create_directory(self, path: str) -> None:
        """Creates a directory if it doesn't exist."""
        os.makedirs(path, exist_ok=True)

    def check_if_directory_exists(self, path: str) -> bool:
        """Checks if a directory exists."""
        return os.path.exists(path)

    def export_batches_to_scratch(
        self,
        task_states: list[TaskState],
        batches: list[dict],
        filename_prefix: str,
    ) -> None:
        """Exports batches to scratch directory.

        Uses a SQLite-based queue to store the batches, to match the rest of
        the codebase.
        """
        self.create_directory(self.job_scratch_path)

        logger.info(
            f"Exporting {len(batches)} batches to scratch directory for job {self.job_id} at path {self.job_scratch_path}."
        )
        for task_state, batch in zip(task_states, batches):
            task_scratch_path = self.get_scratch_path_for_task(
                self.job_scratch_path, task_state.id
            )
            self.create_directory(task_scratch_path)

            queue_name = f"{filename_prefix}_{task_state.id}"
            queue = Queue(
                queue_name=queue_name,
                create_new_queue=True,
                temp_queue=True,
                temp_queue_path=task_scratch_path,
            )
            # we use the default queue batch size instead of the actual size of
            # the batch since the ideal batch size for SQLite is different than
            # whatever we've determined to be the ideal batch size for the job.
            # Doesn't affect anything beyond SQLite I/O performance.
            queue.batch_add_items_to_queue(batch)
        logger.info(
            f"Exported all batches to scratch directory for job {self.job_id} at path {self.job_scratch_path}."
        )

    def load_batch_from_scratch(
        self,
        task_id: str,
        filename_prefix: str,
    ) -> list[dict]:
        """Loads a batch from scratch."""
        task_scratch_path = self.get_scratch_path_for_task(
            self.job_scratch_path, task_id
        )
        queue_name = f"{filename_prefix}_{task_id}"
        queue = Queue(
            queue_name=queue_name, temp_queue=True, temp_queue_path=task_scratch_path
        )
        items = queue.load_dict_items_from_queue()
        return items
