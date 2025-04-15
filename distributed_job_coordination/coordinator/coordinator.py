"""
Coordinator component for the distributed job coordination system.

This module implements the central orchestration component of the system,
responsible for job preparation, data partitioning, task creation, and job
execution management. The Coordinator serves as the single point of coordination
for all aspects of the job lifecycle.
"""

from concurrent.futures import ThreadPoolExecutor
import json

from distributed_job_coordination.coordinator.storage import StorageManager
from distributed_job_coordination.lib.dynamodb_utils import (
    JobStateStore,
    TaskStateStore,
)
from distributed_job_coordination.lib.job_config import JobConfig
from distributed_job_coordination.lib.job_state import (
    JobState,
    TaskState,
    JobStatus,
    TaskStatus,
)
from distributed_job_coordination.coordinator.dataloader import DataLoader
from distributed_job_coordination.coordinator.dispatch import Dispatch
from distributed_job_coordination.coordinator.monitor import JobMonitor
from distributed_job_coordination.lib.manifest import ManifestWriter
from distributed_job_coordination.lib.s3_utils import S3Utils
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)
s3_utils = S3Utils()
job_state_store = JobStateStore()
task_state_store = TaskStateStore()


class Coordinator:
    """
    Central coordinator for distributed job orchestration.

    The Coordinator handles all aspects of job management:
    1. Parsing and validating job configurations
    2. Partitioning input data into batches
    3. Writing batch data to S3 and DynamoDB
    4. Submitting Slurm jobs to process batches
    5. Tracking job state throughout execution
    6. Handling failures and retries
    7. Coordinating result aggregation
    """

    def __init__(self, job_config: JobConfig):
        """
        Initialize the coordinator with a job configuration.

        Args:
            job_config: The validated job configuration
        """
        self.config = job_config
        self.job_id = None
        self.job_state = None
        self.monitoring_active = False
        self._executor = None
        self.dispatch = Dispatch()
        self.storage_manager = None

    def start_job(self) -> str:
        """
        Start a job following the coordinator workflow.

        This method orchestrates the entire job start process:
        1. Split records into batches
        2. Write batches to S3 and DynamoDB
        3. Start downstream workers
        4. Update job status to RUNNING

        Returns:
            The job ID
        """
        # Generate a unique job ID
        self.job_id = self.config.get_unique_job_id()
        logger.info(f"Starting job {self.job_id}")

        # Initialize job state
        self.job_state = self._initialize_job_state()

        # Execute the coordinator workflow
        self.load_records_into_batches()
        self.update_states("start")

        self.write_batches()
        map_batch_id_to_slurm_id = self.start_downstream_workers()
        self.update_job_and_task_states_after_kickoff(
            map_batch_id_to_slurm_id=map_batch_id_to_slurm_id
        )
        self.write_manifests()
        self.update_states("update")

        # Start monitoring in background thread
        self._start_monitoring()

        return self.job_id

    def load_records_into_batches(self) -> None:
        """Load records into batches and generates task states for each batch."""
        logger.info(f"Loading records into batches for job {self.job_id}")

        dataloader = DataLoader(
            input_path=self.config.input.path,
            batch_size=self.config.input.batch_size,
            job_id=self.job_id,
        )

        self.batches = dataloader.create_batches()

        # Create task states for each batch
        self.task_states: list[TaskState] = []
        batch_ids = []
        task_ids = []
        for i in range(len(self.batches)):
            task_id = f"{self.job_id}-task-{i:06d}"
            task_ids.append(task_id)
            batch_id = self.batches[i]["batch_id"]

            task_state = TaskState(
                id=task_id,
                job_name=self.config.name,
                job_id=self.job_id,
                status=TaskStatus.PENDING,
                batch_id=batch_id,
                task_group="initial_batch",
                role="worker",
                worker_id=None,
                started_at=None,
                completed_at=None,
                error=None,
                attempt=0,
                metadata={},
            )

            self.task_states.append(task_state)

        self.job_state.batch_ids = ",".join(batch_ids)
        self.job_state.task_ids = ",".join(task_ids)
        self.job_state.total_worker_tasks = len(self.task_states)

        logger.info(f"Created {len(self.batches)} batches for job {self.job_id}")

    def write_batches(self) -> None:
        """Write batch records to storage."""
        logger.info(f"Writing batch data for job {self.job_id}")
        self.storage_manager = StorageManager(
            job_id=self.job_id,
            job_name=self.config.name,
        )
        self.storage_manager.export_batches_to_scratch(
            task_states=self.task_states,
            batches=self.batches,
            filename_prefix="insert_batch",
        )
        logger.info(f"Batch data written for job {self.job_id}")

    def start_downstream_workers(self) -> dict:
        """
        Start downstream worker tasks and updates the task states with
        the correct worker IDs.
        """
        logger.info(f"Starting downstream workers for job {self.job_id}")
        map_batch_id_to_slurm_id: dict = self.dispatch.submit_batch_to_slurm(
            compute_config=self.config.compute, task_states=self.task_states
        )
        return map_batch_id_to_slurm_id

    def update_job_and_task_states_after_kickoff(
        self, map_batch_id_to_slurm_id: dict
    ) -> None:
        """Update job and task states and syncs them to DynamoDB."""
        current_timestamp = generate_current_datetime_str()

        for task in self.task_states:
            task.worker_id = map_batch_id_to_slurm_id[task.batch_id]
            task.status = TaskStatus.RUNNING
            task.started_at = current_timestamp

        self.job_state.status = JobStatus.RUNNING
        self.job_state.started_at = current_timestamp
        logger.info(
            f"Job state updated to {self.job_state.status} for job {self.job_id}. Task states also updated to {self.task_states[0].status}."
        )

    def write_manifests(self) -> None:
        """Write job/task manifests to S3."""
        logger.info(f"Writing manifests for job {self.job_id}")
        job_manifest: dict = self._generate_job_manifest()
        task_manifests: list[dict] = self._generate_task_manifests()
        manifests_by_type = {
            "job": [job_manifest],
            "task": task_manifests,
        }
        manifest_writer = ManifestWriter(manifests_by_type=manifests_by_type)
        manifest_writer.write_manifests()
        logger.info(f"Manifests written and exported for job {self.job_id}")

    def _generate_job_manifest(self) -> None:
        config_key: str = s3_utils.get_job_config_key(
            job_name=self.config.name, job_id=self.job_id
        )
        job_manifest: dict = {
            "job_id": self.job_id,
            "job_name": self.config.name,
            "handler": self.config.handler,
            "git_commit": self.config.git_commit,
            "config_file": config_key,
            "input_file": self.config.input.path,
            "max_tasks": self.config.input.max_tasks,
            "batch_size": self.config.input.batch_size,
            "task_count": len(self.task_states),
            "submitted_at": self.job_state.created_at,
            "submitted_by": self.config.contact_email,
            "status": self.job_state.status,
        }
        return job_manifest

    def _generate_task_manifest(self, task_state: TaskState) -> None:
        task_manifest: dict = {
            "task_id": task_state.id,
            "job_name": self.config.name,
            "job_id": self.job_id,
            "batch_id": task_state.batch_id,
            "role": task_state.role,
            "worker_id": task_state.worker_id,
            "started_at": task_state.started_at,
            "completed_at": task_state.completed_at,
            "error": task_state.error,
            "attempt": task_state.attempt,
            "metadata": task_state.metadata,
        }
        return task_manifest

    def _generate_task_manifests(self) -> None:
        task_manifests: list[dict] = [
            self._generate_task_manifest(task_state) for task_state in self.task_states
        ]
        return task_manifests

    def update_states(self, status: str) -> None:
        """Updates the job and task states in DynamoDB."""
        if status == "start":
            logger.info(
                f"Inserting into DynamoDB the job state and task states for job {self.job_id}"
            )
            job_state_store.insert_job_state(self.job_state)
            for task in self.task_states:
                task_state_store.insert_task_state(task)
            logger.info(
                f"Finished inserting into DynamoDB the job state and task states for job {self.job_id}"
            )

        elif status == "update":
            logger.info(
                f"Updating the job state and task states in DynamoDB for job {self.job_id}"
            )
            job_state_store.update_job_state(self.job_state)
            for task in self.task_states:
                task_state_store.update_task_state(task)
            logger.info(
                f"Finished updating the job state and task states in DynamoDB for job {self.job_id}"
            )

        else:
            # NOTE: I know this'll get flagged. This is intentional. I've
            # not thought of how to refactor this function correctly yet.
            raise ValueError(f"Invalid status: {status}")

    def _initialize_job_state(self) -> JobState:
        """
        Initialize the job state.

        Returns:
            A new JobState object
        """
        current_timestamp = generate_current_datetime_str()
        return JobState(
            id=self.job_id,
            name=self.config.name,
            status=JobStatus.PENDING,
            created_at=current_timestamp,
            updated_at=current_timestamp,
            started_at=None,
            completed_at=None,
            error=None,
            # tasks haven't been assigned yet.
            total_worker_tasks=0,
            completed_worker_tasks=0,
            failed_worker_tasks=0,
            batch_ids=None,
            task_ids=None,
            metadata=json.dumps({}),
        )

    def _start_monitoring(self) -> None:
        """
        Start monitoring job progress in a background thread.
        """
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1)

        self.monitor = JobMonitor(
            job_state=self.job_state,
            task_states=self.task_states,
        )

        self.monitoring_active = True
        self._executor.submit(self.monitor.monitor_job)

        logger.info(f"Started monitoring for job {self.job_id}")


def orchestrate_job(config_path: str) -> str:
    """
    Orchestrate a job from a configuration file.

    This function is the main entry point for job orchestration:
    1. Load and validate configuration
    2. Initialize coordinator
    3. Prepare and submit job

    Args:
        config_path: Path to job configuration file

    Returns:
        Job ID
    """
    # Load and validate configuration
    config = JobConfig.from_yaml(config_path)

    # Initialize coordinator
    coordinator = Coordinator(config)

    # Start job using the new workflow
    job_id = coordinator.start_job()

    return job_id
