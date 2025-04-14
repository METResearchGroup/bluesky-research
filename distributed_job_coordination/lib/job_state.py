"""
Job state models for distributed job coordination.

This module defines data models for representing and tracking job and task states
in the distributed job coordination system.
"""

from enum import Enum
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field

from lib.helper import generate_current_datetime_str


# Define enums for job and task states
class JobStatus(str, Enum):
    """Possible states for a job."""

    PENDING = "pending"
    PREPARING = "preparing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    """Possible states for a task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskState(BaseModel):
    """
    Represents the state of an individual task within a job.

    Attributes:
        id: Unique identifier for the task within the job
        status: Current status of the task
        batch_id: Identifier for the batch this task belongs to
        worker_id: Identifier for the worker processing this task (if assigned)
            Will be assigned by looking at the Slurm ID of the given task.
        started_at: When the task started processing
        completed_at: When the task completed processing
        error: Error message if the task failed
        attempt: Number of times this task has been attempted
        metadata: Additional task metadata
    """

    id: str
    status: TaskStatus = TaskStatus.PENDING
    batch_id: str
    worker_id: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    attempt: int = 0
    metadata: Dict[str, Union[str, int, float, bool]] = Field(default_factory=dict)

    def mark_running(self, worker_id: str) -> None:
        """Mark the task as running."""
        self.status = TaskStatus.RUNNING
        self.worker_id = worker_id
        self.started_at = generate_current_datetime_str()

    def mark_completed(self) -> None:
        """Mark the task as completed."""
        self.status = TaskStatus.COMPLETED
        self.completed_at = generate_current_datetime_str()

    def mark_failed(self, error: str) -> None:
        """Mark the task as failed."""
        self.status = TaskStatus.FAILED
        self.completed_at = generate_current_datetime_str()
        self.error = error

    def mark_cancelled(self) -> None:
        """Mark the task as cancelled."""
        self.status = TaskStatus.CANCELLED
        self.completed_at = generate_current_datetime_str()

    def increment_attempt(self) -> None:
        """Increment the attempt counter."""
        self.attempt += 1


class BatchState(BaseModel):
    """
    Represents the state of a batch of tasks within a job.

    Attributes:
        id: Unique identifier for the batch
        task_count: Number of tasks in this batch
        completed_tasks: Number of completed tasks in this batch
        failed_tasks: Number of failed tasks in this batch
        metadata: Additional batch metadata
    """

    id: str
    task_count: int
    completed_tasks: int = 0
    failed_tasks: int = 0
    metadata: Dict[str, Union[str, int, float, bool]] = Field(default_factory=dict)


class JobState(BaseModel):
    """
    Represents the state of a job.

    Attributes:
        id: Unique identifier for the job
        name: Human-readable name for the job
        status: Current status of the job
        created_at: When the job was created.
        updated_at: When the job state was last updated
        started_at: When the job started processing.
            Doesn't necessarily equal 'created_at' (e.g., we can create the job
            and then queue it for processing later, in which case the 'started_at'
            will be the time the job was taken off the queue and started processing).
        completed_at: When the job completed processing
        error: Error message if the job failed
        total_worker_tasks: Total number of worker tasks in the job.
            We distinguish between worker tasks (which actually do the computation)
            and system tasks (which handle the coordination of the job as well as
            the aggregation of the results).
        completed_worker_tasks: Number of completed worker tasks
        failed_worker_tasks: Number of failed worker tasks
        batch_ids: Comma-separated list of batch IDs.
        task_ids: Comma-separated list of task IDs.
        metadata: Additional job metadata
    """

    id: str
    name: str
    status: JobStatus = JobStatus.PENDING
    created_at: str = Field(default_factory=generate_current_datetime_str)
    updated_at: str = Field(default_factory=generate_current_datetime_str)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    total_worker_tasks: int = 0
    completed_worker_tasks: int = 0
    failed_worker_tasks: int = 0
    batch_ids: Optional[str] = None  # comma-separated list of batch IDs.
    task_ids: Optional[str] = None  # comma-separated list of task IDs.
    metadata: Dict[str, Union[str, int, float, bool]] = Field(default_factory=dict)

    def update_status(self) -> None:
        """
        Update the job status based on the current state of tasks.
        Also updates the task counts.
        """
        self.updated_at = generate_current_datetime_str()

        # Count tasks by status
        completed = 0
        failed = 0
        running = 0

        for task in self.tasks.values():
            if task.status == TaskStatus.COMPLETED:
                completed += 1
            elif task.status == TaskStatus.FAILED:
                failed += 1
            elif task.status == TaskStatus.RUNNING:
                running += 1

        self.completed_tasks = completed
        self.failed_tasks = failed

        # Update job status based on task states
        if self.status == JobStatus.CANCELLING:
            # If we're cancelling, check if all tasks are stopped
            if running == 0:
                self.status = JobStatus.CANCELLED
                self.completed_at = generate_current_datetime_str()
        elif self.status == JobStatus.PENDING and running > 0:
            # If we were pending and tasks are running, we're running
            self.status = JobStatus.RUNNING
            self.started_at = generate_current_datetime_str()
        elif self.status == JobStatus.RUNNING:
            # If all tasks are done (completed or failed), the job is done
            if completed + failed == self.total_tasks:
                if failed > 0:
                    self.status = JobStatus.FAILED
                else:
                    self.status = JobStatus.COMPLETED
                self.completed_at = generate_current_datetime_str()

    def add_task(self, task: TaskState) -> None:
        """
        Add a task to the job.

        Args:
            task: The task state to add
        """
        self.tasks[task.id] = task
        self.total_tasks += 1
        self.updated_at = generate_current_datetime_str()

    def add_batch(self, batch: BatchState) -> None:
        """
        Add a batch to the job.

        Args:
            batch: The batch state to add
        """
        self.batches[batch.id] = batch
        self.updated_at = generate_current_datetime_str()

    def mark_preparing(self) -> None:
        """Mark the job as preparing (data partitioning phase)."""
        self.status = JobStatus.PREPARING
        self.updated_at = generate_current_datetime_str()

    def mark_running(self) -> None:
        """Mark the job as running."""
        if self.status in [JobStatus.PENDING, JobStatus.PREPARING]:
            self.status = JobStatus.RUNNING
            self.started_at = generate_current_datetime_str()
            self.updated_at = generate_current_datetime_str()

    def mark_cancelling(self) -> None:
        """Mark the job as cancelling."""
        if self.status not in [
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ]:
            self.status = JobStatus.CANCELLING
            self.updated_at = generate_current_datetime_str()

    def mark_failed(self, error: str) -> None:
        """
        Mark the job as failed.

        Args:
            error: Error message explaining the failure
        """
        self.status = JobStatus.FAILED
        self.error = error
        self.completed_at = generate_current_datetime_str()
        self.updated_at = generate_current_datetime_str()

    def get_pending_tasks(self) -> List[TaskState]:
        """
        Get a list of pending tasks.

        Returns:
            List of pending tasks
        """
        return [
            task for task in self.tasks.values() if task.status == TaskStatus.PENDING
        ]

    def get_running_tasks(self) -> List[TaskState]:
        """
        Get a list of running tasks.

        Returns:
            List of running tasks
        """
        return [
            task for task in self.tasks.values() if task.status == TaskStatus.RUNNING
        ]

    def get_failed_tasks(self) -> List[TaskState]:
        """
        Get a list of failed tasks.

        Returns:
            List of failed tasks
        """
        return [
            task for task in self.tasks.values() if task.status == TaskStatus.FAILED
        ]

    def get_completion_percentage(self) -> float:
        """
        Calculate the job completion percentage.

        Returns:
            Percentage of completed tasks (0-100)
        """
        if self.total_tasks == 0:
            return 0.0
        return (self.completed_tasks / self.total_tasks) * 100.0
