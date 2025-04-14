"""Class for generating manifests.

We write manifests to S3 for each job and task. We write
twice, once at the initial creation of the job/task and once at
completion of the job/task. The manifests should track the job
states closely, but we track the actual task states separately and
via DynamoDB. We use the manifests to track the final job/task status
once the job/task has completed, and we use this as our method for
logging and tracking completed job/task statuses (we purge the DynamoDB
table of task states once the job/task has completed).
"""

from typing import Any

from pydantic import BaseModel

from distributed_job_coordination.lib.s3_utils import S3Utils
from lib.log.logger import get_logger

logger = get_logger(__name__)


class JobManifest(BaseModel):
    job_id: str
    job_name: str
    handler: str
    git_commit: str
    config_file: str
    input_file: str
    max_partitions: int
    batch_size: int
    task_count: int
    submitted_at: str
    submitted_by: str  # <email> of user.
    status: str


class TaskManifest(BaseModel):
    task_id: str
    job_name: str
    job_id: str
    batch_id: str
    role: str
    worker_id: str
    started_at: str
    completed_at: str
    error: str
    attempt: int
    metadata: str


class AggregationManifest(BaseModel):
    job_id: str
    job_name: str
    output_service_name: str
    output_format: str
    output_compression: str
    output_partition_keys: list[str]
    completed_at: str
    metadata: str


class ManifestWriter:
    """Class for writing manifests to S3."""

    def __init__(self, manifests_by_type: dict[str, list[dict]]):
        self.s3_utils = S3Utils()
        self.manifests_by_type = manifests_by_type

    def write_manifest(self, type: str, manifest: dict[str, Any]) -> None:
        if type == "job":
            generated_manifest = self._generate_job_manifest(manifest)
            self.s3_utils.upload_job_manifest(generated_manifest)
        elif type == "task":
            generated_manifest = self._generate_task_manifest(manifest)
            self.s3_utils.upload_task_manifest(generated_manifest)
        else:
            raise ValueError(f"Invalid manifest type: {type}")

    def write_manifests(self) -> None:
        for manifest_type, manifests_list in self.manifests_by_type.items():
            logger.info(
                f"Writing {len(manifests_list)} {manifest_type} manifests to S3."
            )
            for manifest in manifests_list:
                self.write_manifest(manifest_type, manifest)

    def _generate_job_manifest(self, manifest: dict) -> dict:
        return JobManifest(**manifest).model_dump()

    def _generate_task_manifest(self, manifest: dict) -> dict:
        return TaskManifest(**manifest).model_dump()

    def _generate_aggregation_manifest(self, manifest: dict) -> dict:
        return AggregationManifest(**manifest).model_dump()
