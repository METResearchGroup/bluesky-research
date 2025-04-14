"""
DynamoDB utilities for the distributed job coordination framework.

This module provides a wrapper around the DynamoDB functionality in lib/aws/dynamodb.py,
specific to the needs of the distributed job coordination service.
"""

from distributed_job_coordination.lib.job_state import JobState, TaskState
from lib.aws.dynamodb import DynamoDB
from lib.log.logger import get_logger

logger = get_logger(__name__)


class JobStateStore:
    def __init__(self):
        self.table_name = "distributed_job_coordination_job_state"
        self.dynamodb = DynamoDB()

    def __repr__(self) -> str:
        return f"JobStateStore(table_name={self.table_name})"

    def insert_job_state(self, job_state: JobState) -> None:
        """Insert a job state into the store.

        Checks to see if the item is already in the table, and doesn't write it if
        it is. This is to prevent duplicate jobs from being inserted into the table
        and to prevent race conditions.
        """
        job_state_dict = job_state.model_dump()
        key = {"job_id": {"S": job_state_dict["id"]}}
        if not self.dynamodb.verify_item_exists(key, self.table_name):
            logger.info(f"Inserting job state for job {job_state_dict['id']}")
            self.dynamodb.insert_item_into_table(job_state_dict, self.table_name)
            logger.info(f"Finished inserting job state for job {job_state_dict['id']}")
        else:
            logger.info(
                f"Job state for job {job_state_dict['id']} already exists. Cannot insert, must update instead."
            )

    def update_job_state(self, job_id: str, fields_to_update: dict) -> None:
        """Update a job state in the store.

        Updates an existing item in the table. Item must already exist. We do this
        to ensure that there are no accidental updates/overwrites and that we are
        always updating the latest state.
        """
        key = {"job_id": {"S": job_id}}
        logger.info(
            f"Updating job state for job {job_id} with fields {fields_to_update}"
        )
        self.dynamodb.update_item_in_table(key, fields_to_update, self.table_name)
        logger.info(f"Finished updating job state for job {job_id}")

    def load_job_state(self, job_id: str) -> JobState:
        """Load a job state from the store."""
        key = {"job_id": {"S": job_id}}
        job_state_dict = self.dynamodb.get_item_from_table(key, self.table_name)
        return JobState(**job_state_dict)


class TaskStateStore:
    def __init__(self):
        self.table_name = "distributed_job_coordination_task_state"
        self.dynamodb = DynamoDB()

    def __repr__(self) -> str:
        return f"TaskStateStore(table_name={self.table_name})"

    def insert_task_state(self, task_state: TaskState) -> None:
        """Insert a task state into the store.

        Checks to see if the item is already in the table, and doesn't write it if
        it is. This is to prevent duplicate tasks from being inserted into the table
        and to prevent race conditions.
        """
        task_state_dict = task_state.model_dump()
        key = {"task_id": {"S": task_state_dict["id"]}}
        if not self.dynamodb.verify_item_exists(key, self.table_name):
            logger.info(f"Inserting task state for task {task_state_dict['id']}")
            self.dynamodb.insert_item_into_table(task_state_dict, self.table_name)
            logger.info(
                f"Finished inserting task state for task {task_state_dict['id']}"
            )
        else:
            logger.info(
                f"Task state for task {task_state_dict['id']} already exists. Cannot insert, must update instead."
            )

        logger.info(f"Inserting task state for task {task_state_dict['id']}")

    def update_task_state(self, task_id: str, fields_to_update: dict) -> None:
        """Updates the status of a task.

        Updates an existing item in the table. Item must already exist. We do this
        to ensure that there are no accidental updates/overwrites and that we are
        always updating the latest state.
        """
        key = {"task_id": {"S": task_id}}
        logger.info(
            f"Updating task state for task {task_id} with fields {fields_to_update}"
        )
        self.dynamodb.update_item_in_table(key, fields_to_update, self.table_name)
        logger.info(f"Finished updating task state for task {task_id}")

    def load_task_state(self, task_id: str) -> TaskState:
        """Load a task state from the store.

        Task states are unique across jobs (they include the job ID within them).
        """
        key = {"task_id": {"S": task_id}}
        task_state_dict = self.dynamodb.get_item_from_table(key, self.table_name)
        return TaskState(**task_state_dict)

    def load_task_states_for_job(self, job_id: str) -> list[TaskState]:
        """Load all task states for a job."""
        # TODO: check if I can do this filtering directly in DynamoDB.
        # I know that I couldn't figure this out before when fetching multiple
        # items so I just generally load all the items and then filter locally.
        task_states = self.dynamodb.get_all_items_from_table(self.table_name)
        return [
            TaskState(**task_state)
            for task_state in task_states
            if task_state["job_id"] == job_id
        ]
