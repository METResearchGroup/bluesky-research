"""Class for monitoring the state of a job and its tasks."""

from datetime import datetime
from typing import Optional

from distributed_job_coordination.coordinator.models import TaskStatus
from distributed_job_coordination.lib.job_state import JobState, TaskState
from lib.constants import timestamp_format
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)


class TaskMonitor:
    def __init__(self):
        pass

    def monitor_task(self) -> None:
        pass


class JobMonitor:
    """Monitors the state of a job while its corresponding tasks are running
    via Slurm."""

    def __init__(
        self,
        job_state: JobState,
        task_states: list[TaskState],
        monitoring_interval_minutes: int = 5,
    ):
        self.map_task_id_to_task_monitor: dict[str, TaskMonitor] = {}
        self.monitoring_interval_minutes = monitoring_interval_minutes
        self.job_state = job_state
        self.task_states = task_states

        for task_state in self.task_states:
            self.map_task_id_to_task_monitor[task_state.id] = TaskMonitor()

    def monitor_job(self, job_id: str) -> None:
        # load job status and task statuses from DynamoDB.

        # check how tasks are running on Slurm.

        # if there needs to be any update (e.g., a task finishes), make
        # the necessary update in DynamoDB.

        # check for job completion. If all tasks are completed (even if some failed),
        # update the job as completed.

        # TODO: ideally this would be long-running, and would run in a background thread.
        pass

    def _update_job_state():
        pass

    def _update_task_state():
        pass

    def update_timed_out_tasks(self) -> None:
        task_timeout_minutes: Optional[int] = self.config.advanced.task_timeout_minutes
        if not task_timeout_minutes:
            return  # No timeout specified

        task_timeout_seconds: int = task_timeout_minutes * 60

        current_timestamp: str = generate_current_datetime_str()
        current_ts: datetime = datetime.strptime(current_timestamp, timestamp_format)

        for task in self.task_states:
            if (
                task.status == TaskStatus.RUNNING
                and task.started_at
                and (
                    current_ts - datetime.strptime(task.started_at, timestamp_format)
                ).total_seconds()
                > task_timeout_seconds
            ):
                logger.warning(
                    f"Task {task.task_id} timed out after {task_timeout_minutes} minutes"
                )

                # Mark task as failed due to timeout
                task.status = TaskStatus.FAILED
                task.error = "Task timed out"
                task.completed_at = current_timestamp

    def mark_job_for_cancellation(self) -> None:
        pass

    # TODO: do via dispatch.
    def cancel_job(self) -> None:
        pass

    def mark_task_for_cancellation(self, task_id: str) -> None:
        pass

    # TODO: do via dispatch.
    def cancel_task(self, task_id: str) -> None:
        pass

    def monitor_task(self, task_id: str) -> None:
        if task_id not in self.map_task_id_to_task_monitor:
            self.map_task_id_to_task_monitor[task_id] = TaskMonitor()

        self.map_task_id_to_task_monitor[task_id].monitor_task()
