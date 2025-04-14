"""
Coordinator component for the distributed job coordination system.

This module implements the central orchestration component of the system,
responsible for job preparation, data partitioning, task creation, and job
execution management. The Coordinator serves as the single point of coordination
for all aspects of the job lifecycle.
"""

from concurrent.futures import ThreadPoolExecutor

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
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__name__)


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
        self._persist_job_state()

        # Execute the coordinator workflow
        self.load_records_into_batches()
        self.update_states_to_aws("start")

        self.write_batches()
        map_batch_id_to_slurm_id = self.start_downstream_workers()
        self.update_job_and_task_states_after_kickoff(
            map_batch_id_to_slurm_id=map_batch_id_to_slurm_id
        )
        self.write_manifests()
        self.update_states_to_aws("running")

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
                status=TaskStatus.PENDING,
                batch_id=batch_id,
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
        """
        Write batch records to storage.

        This step writes:
        1. Read-only copy of batches to S3
        2. Write-friendly batch data to DynamoDB
        """
        logger.info(f"Writing batch data for job {self.job_id}")

        # Write to S3 (read-only copies)
        self._write_readonly_copies_to_s3()

        # Write to DynamoDB (write-friendly batches)
        self._write_batches_to_dynamodb()

        logger.info(f"Batch data written for job {self.job_id}")

    def _write_readonly_copies_to_s3(self) -> None:
        """Write read-only copies of batches to S3."""
        logger.info(f"Writing read-only batch copies to S3 for job {self.job_id}")

        # Get storage path from input configuration
        input_type = self.config.input.type
        print(f"Input type: {input_type}")

        # TODO: put in s3_utils.py
        # Example implementation - replace with actual S3 code
        # s3 = boto3.client('s3')
        # for batch in self.batches:
        #     s3.put_object(
        #         Bucket=bucket_name,
        #         Key=f"{self.job_id}/batches/{batch.batch_id}.json",
        #         Body=json.dumps(batch.data)
        #     )

        # For now, just log
        logger.info(f"Read-only copies written to S3 for {len(self.batches)} batches")

    def _write_batches_to_dynamodb(self) -> None:
        """Write write-friendly batch data to DynamoDB."""
        logger.info(f"Writing batch data to DynamoDB for job {self.job_id}")

        # Example implementation - replace with actual DynamoDB code
        # dynamodb = boto3.resource('dynamodb')
        # table = dynamodb.Table('JobBatches')
        # with table.batch_writer() as batch:
        #     for batch_state in self.batches:
        #         batch.put_item(Item=batch_state.dict())

        # Write task states to DynamoDB
        # table = dynamodb.Table('TaskStates')
        # with table.batch_writer() as batch:
        #     for task in self.task_states:
        #         batch.put_item(Item=task.dict())

        # TODO: put in dynamo_utils.py
        # For now, just log
        logger.info(f"Batch data written to DynamoDB for {len(self.batches)} batches")

    def start_downstream_workers(self) -> dict:
        """
        Start downstream worker tasks and updates the task states with
        the correct worker IDs.
        """
        logger.info(f"Starting downstream workers for job {self.job_id}")
        map_batch_id_to_slurm_id: dict = self.dispatch.submit_batch_to_slurm()
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

        # TOOD: update job state.
        self.job_state.status = JobStatus.RUNNING
        self.job_state.started_at = current_timestamp

    def write_manifests(self) -> None:
        logger.info(f"Writing manifests for job {self.job_id}")
        self._write_job_manifest()
        self._write_task_manifests()
        logger.info(f"Manifests written for job {self.job_id}")

    def _write_job_manifest(self) -> None:
        pass

    def _write_task_manifest(self, task_state: TaskState) -> None:
        pass

    def _write_task_manifests(self) -> None:
        for task_state in self.task_states:
            self._write_task_manifest(task_state)

    def update_states_to_aws(self, status: str) -> None:
        job_state = JobStateStore()
        task_state = TaskStateStore()
        print(f"Job state store: {job_state}")
        print(f"Task state store: {task_state}")

        if status == "start":
            pass

        elif status == "running":
            pass

        else:
            # TODO: I know this'll get flagged. This is intentional. I've
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
            metadata={},
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
