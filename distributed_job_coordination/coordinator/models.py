"""
Core data models for the distributed job coordination framework.

This module defines the data models and abstractions for representing jobs,
tasks, batches, and other core components of the job coordination system.
"""

from __future__ import annotations

import json
import enum
import uuid
from typing import Dict, List, Optional, Union, Any
from datetime import datetime


class JobStatus(enum.Enum):
    """Status enum for job state tracking."""

    PENDING = "PENDING"
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    AGGREGATING = "AGGREGATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatus(enum.Enum):
    """Status enum for task state tracking."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRY_PENDING = "RETRY_PENDING"
    RETRY_RUNNING = "RETRY_RUNNING"


class TaskRole(enum.Enum):
    """Role enum for task type classification."""

    WORKER = "worker"
    AGGREGATOR = "aggregator"
    COORDINATOR = "coordinator"
    RETRY_PLANNER = "retry_planner"


class Batch:
    """
    Represents a data partition (batch) to be processed by a worker.

    Batches contain the actual data to be processed and metadata for tracking.
    """

    def __init__(
        self,
        batch_id: str,
        job_id: str,
        items: List[Any],
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a batch with items and metadata.

        Args:
            batch_id: Unique identifier for this batch
            job_id: ID of the parent job
            items: List of data items to be processed
            metadata: Optional metadata dictionary for batch tracking
        """
        self.batch_id = batch_id
        self.job_id = job_id
        self.items = items
        self.metadata = metadata or {}
        self.created_at = datetime.utcnow().isoformat()
        self.item_count = len(items)

    def to_dict(self) -> Dict[str, Any]:
        """Convert batch to a dictionary for serialization."""
        return {
            "batch_id": self.batch_id,
            "job_id": self.job_id,
            "created_at": self.created_at,
            "item_count": self.item_count,
            "metadata": self.metadata,
            "items": self.items,
        }

    def to_json(self) -> str:
        """Convert batch to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Batch:
        """Create batch from dictionary representation."""
        return cls(
            batch_id=data["batch_id"],
            job_id=data["job_id"],
            items=data["items"],
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def from_json(cls, json_str: str) -> Batch:
        """Create batch from JSON string."""
        return cls.from_dict(json.loads(json_str))


class Task:
    """
    Represents a unit of execution within a job.

    Tasks can be worker tasks that process data batches, or system tasks
    like coordinator, aggregator, etc.
    """

    def __init__(
        self,
        task_id: str,
        job_id: str,
        role: TaskRole,
        batch_id: Optional[str] = None,
        group: Optional[str] = None,
        phase: Optional[str] = None,
        status: TaskStatus = TaskStatus.PENDING,
        retries: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a task with role and batch information.

        Args:
            task_id: Unique identifier for this task
            job_id: ID of the parent job
            role: Role of this task (worker, aggregator, etc.)
            batch_id: ID of batch to process (for worker tasks)
            group: Task group identifier for grouping related tasks
            phase: Job phase this task belongs to
            status: Current status of the task
            retries: Number of retry attempts so far
            metadata: Optional metadata dictionary for task tracking
        """
        self.task_id = task_id
        self.job_id = job_id
        self.role = role if isinstance(role, TaskRole) else TaskRole(role)
        self.batch_id = batch_id
        self.group = group
        self.phase = phase
        self.status = status if isinstance(status, TaskStatus) else TaskStatus(status)
        self.retries = retries
        self.metadata = metadata or {}
        self.created_at = datetime.utcnow().isoformat()
        self.started_at = None
        self.completed_at = None
        self.error = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert task to a dictionary for serialization."""
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "role": self.role.value,
            "batch_id": self.batch_id,
            "group": self.group,
            "phase": self.phase,
            "status": self.status.value,
            "retries": self.retries,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }

    def to_json(self) -> str:
        """Convert task to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Task:
        """Create task from dictionary representation."""
        task = cls(
            task_id=data["task_id"],
            job_id=data["job_id"],
            role=TaskRole(data["role"]),
            batch_id=data.get("batch_id"),
            group=data.get("group"),
            phase=data.get("phase"),
            status=TaskStatus(data["status"]),
            retries=data.get("retries", 0),
            metadata=data.get("metadata", {}),
        )
        task.created_at = data.get("created_at", task.created_at)
        task.started_at = data.get("started_at")
        task.completed_at = data.get("completed_at")
        task.error = data.get("error")
        return task

    @classmethod
    def from_json(cls, json_str: str) -> Task:
        """Create task from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def update_status(self, status: Union[TaskStatus, str]) -> None:
        """
        Update task status and timestamp relevant fields.

        Args:
            status: New status to set
        """
        if isinstance(status, str):
            status = TaskStatus(status)

        old_status = self.status
        self.status = status

        # Update timestamps based on status transitions
        now = datetime.utcnow().isoformat()

        if old_status != TaskStatus.RUNNING and status == TaskStatus.RUNNING:
            self.started_at = now

        if status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
            self.completed_at = now

    def add_error(self, error_msg: str, error_type: str = "unknown") -> None:
        """
        Add error information to the task.

        Args:
            error_msg: Error message or description
            error_type: Type of error (e.g., "network", "validation")
        """
        self.error = {
            "message": error_msg,
            "type": error_type,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.update_status(TaskStatus.FAILED)


class TaskGroup:
    """
    Represents a group of related tasks within a job.

    Task groups are used to organize tasks with similar configuration or purpose.
    """

    def __init__(
        self,
        group_id: str,
        job_id: str,
        phase: str,
        role: TaskRole,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a task group for organizing related tasks.

        Args:
            group_id: Unique identifier for this group
            job_id: ID of the parent job
            phase: Job phase this group belongs to
            role: Role of tasks in this group
            config: Configuration parameters for tasks in this group
        """
        self.group_id = group_id
        self.job_id = job_id
        self.phase = phase
        self.role = role if isinstance(role, TaskRole) else TaskRole(role)
        self.config = config or {}
        self.tasks: List[Task] = []

    def add_task(self, task: Task) -> None:
        """
        Add a task to this group.

        Args:
            task: Task to add to the group
        """
        if task.group is None:
            task.group = self.group_id

        if task.phase is None:
            task.phase = self.phase

        self.tasks.append(task)

    def to_dict(self) -> Dict[str, Any]:
        """Convert task group to a dictionary for serialization."""
        return {
            "group_id": self.group_id,
            "job_id": self.job_id,
            "phase": self.phase,
            "role": self.role.value,
            "config": self.config,
            "task_count": len(self.tasks),
        }


class Job:
    """
    Represents a distributed job execution.

    A job is the top-level unit of work, containing tasks organized into groups
    and phases. It maintains overall state and configuration.
    """

    def __init__(
        self,
        job_id: str,
        job_type: str,
        handler: str,
        config: Dict[str, Any],
        input_file: Optional[str] = None,
        git_commit: Optional[str] = None,
    ):
        """
        Initialize a job with configuration and metadata.

        Args:
            job_id: Unique identifier for this job
            job_type: Type of job (e.g., "backfill", "aggregation")
            handler: Handler function path for processing
            config: Configuration parameters for the job
            input_file: Path to input file (S3 or local)
            git_commit: Git commit hash for versioning
        """
        self.job_id = job_id
        self.job_type = job_type
        self.handler = handler
        self.config = config
        self.input_file = input_file
        self.git_commit = git_commit
        self.status = JobStatus.PENDING
        self.created_at = datetime.utcnow().isoformat()
        self.started_at = None
        self.completed_at = None
        self.task_groups: Dict[str, TaskGroup] = {}
        self.phases: List[str] = []
        self.slurm_array_id = None
        self.metadata: Dict[str, Any] = {}

    def add_phase(self, phase: str) -> None:
        """
        Add a job phase for sequential execution.

        Args:
            phase: Phase identifier
        """
        if phase not in self.phases:
            self.phases.append(phase)

    def create_task_group(
        self,
        group_id: str,
        phase: str,
        role: TaskRole,
        config: Optional[Dict[str, Any]] = None,
    ) -> TaskGroup:
        """
        Create a new task group for this job.

        Args:
            group_id: Unique identifier for the group
            phase: Job phase this group belongs to
            role: Role of tasks in this group
            config: Configuration parameters for tasks in this group

        Returns:
            The newly created TaskGroup
        """
        if phase not in self.phases:
            self.add_phase(phase)

        group = TaskGroup(
            group_id=group_id,
            job_id=self.job_id,
            phase=phase,
            role=role,
            config=config,
        )

        self.task_groups[group_id] = group
        return group

    def create_task(
        self,
        group_id: str,
        role: TaskRole,
        batch_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> Task:
        """
        Create a new task within a task group.

        Args:
            group_id: ID of the task group for this task
            role: Role of the task
            batch_id: ID of batch to process (for worker tasks)
            task_id: Optional custom task ID (generated if not provided)

        Returns:
            The newly created Task
        """
        if group_id not in self.task_groups:
            raise ValueError(f"Task group '{group_id}' does not exist")

        group = self.task_groups[group_id]

        if task_id is None:
            task_id = f"task-{str(uuid.uuid4())[:8]}"

        task = Task(
            task_id=task_id,
            job_id=self.job_id,
            role=role,
            batch_id=batch_id,
            group=group_id,
            phase=group.phase,
        )

        group.add_task(task)
        return task

    def update_status(self, status: Union[JobStatus, str]) -> None:
        """
        Update job status and timestamp relevant fields.

        Args:
            status: New status to set
        """
        if isinstance(status, str):
            status = JobStatus(status)

        old_status = self.status
        self.status = status

        # Update timestamps based on status transitions
        now = datetime.utcnow().isoformat()

        if old_status != JobStatus.RUNNING and status == JobStatus.RUNNING:
            self.started_at = now

        if status in (JobStatus.COMPLETED, JobStatus.FAILED):
            self.completed_at = now

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to a dictionary for serialization (manifest)."""
        task_count = sum(len(group.tasks) for group in self.task_groups.values())

        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "handler": self.handler,
            "git_commit": self.git_commit,
            "config_file": self.config.get("config_file"),
            "input_file": self.input_file,
            "batch_size": self.config.get("batch_size"),
            "task_count": task_count,
            "slurm_array_id": self.slurm_array_id,
            "submitted_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "phases": self.phases,
            "status": self.status.value,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Convert job to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Job:
        """Create job from dictionary representation."""
        job = cls(
            job_id=data["job_id"],
            job_type=data["job_type"],
            handler=data["handler"],
            config={
                "config_file": data.get("config_file"),
                "batch_size": data.get("batch_size"),
            },
            input_file=data.get("input_file"),
            git_commit=data.get("git_commit"),
        )

        job.created_at = data.get("submitted_at", job.created_at)
        job.started_at = data.get("started_at")
        job.completed_at = data.get("completed_at")
        job.slurm_array_id = data.get("slurm_array_id")
        job.metadata = data.get("metadata", {})

        if "phases" in data:
            job.phases = data["phases"]

        if "status" in data:
            job.status = JobStatus(data["status"])

        return job

    @classmethod
    def from_json(cls, json_str: str) -> Job:
        """Create job from JSON string."""
        return cls.from_dict(json.loads(json_str))
