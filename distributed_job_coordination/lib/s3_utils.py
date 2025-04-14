"""
S3 utilities for distributed job coordination.

This module provides specialized S3 storage functionality for distributed jobs,
including storing and retrieving job manifests, task configs, and results.
"""

import os
from typing import Any

from lib.aws.s3 import S3
from lib.log.logger import get_logger

logger = get_logger(__name__)

root_s3_key = "distributed_job_coordination"


class S3Utils:
    """Wrapper over the lib/aws/s3.py S3 class, with utilities specific
    for the distributed_job_coordination package."""

    def __init__(self):
        self.s3 = S3()

    def upload_job_manifest(self, manifest: dict) -> None:
        """Upload a job manifest to S3."""
        job_name = manifest["job_name"]
        job_id = manifest["job_id"]
        s3_key = os.path.join(
            root_s3_key, f"job_name={job_name}", f"job_id={job_id}", "manifest.json"
        )
        self.s3.write_dict_json_to_s3(manifest, s3_key)
        logger.info(f"Uploaded job manifest to {s3_key}")

    def download_job_manifest(self, job_name: str, job_id: str) -> dict[str, Any]:
        """Download a job manifest from S3."""
        s3_key = os.path.join(
            root_s3_key, f"job_name={job_name}", f"job_id={job_id}", "manifest.json"
        )
        try:
            result = self.s3.read_json_from_s3(s3_key)
            if not result:
                raise ValueError(
                    f"No manifest found for job name {job_name} and job id {job_id}."
                )
            return result
        except Exception as e:
            logger.error(f"Error downloading job manifest from {s3_key}: {e}")
            raise e

    def upload_task_manifest(self, manifest: dict[str, Any]) -> None:
        """Upload a task manifest to S3."""
        job_name = manifest["job_name"]
        job_id = manifest["job_id"]
        task_id = manifest["task_id"]
        s3_key = os.path.join(
            root_s3_key,
            f"job_name={job_name}",
            f"job_id={job_id}",
            f"task_id={task_id}",
            "manifest.json",
        )
        self.s3.write_dict_json_to_s3(manifest, s3_key)
        logger.info(f"Uploaded task manifest to {s3_key}")

    def download_task_manifest(
        self, job_name: str, job_id: str, task_id: str
    ) -> dict[str, Any]:
        """Download a task manifest from S3."""
        s3_key = os.path.join(
            root_s3_key,
            f"job_name={job_name}",
            f"job_id={job_id}",
            f"task_id={task_id}",
            "manifest.json",
        )
        try:
            result = self.s3.read_json_from_s3(s3_key)
            if not result:
                raise ValueError(
                    f"No manifest found for job name {job_name} and job id {job_id} and task id {task_id}."
                )
            return result
        except Exception as e:
            logger.error(f"Error downloading task manifest from {s3_key}: {e}")
            raise e

    def upload_job_config(
        self, job_name: str, job_id: str, config: dict[str, Any]
    ) -> None:
        """Upload a job config to S3."""
        s3_key = os.path.join(
            root_s3_key, f"job_name={job_name}", f"job_id={job_id}", "config.json"
        )
        self.s3.write_dict_json_to_s3(config, s3_key)
        logger.info(f"Uploaded job config to {s3_key}")

    def get_job_config_key(self, job_name: str, job_id: str) -> str:
        """Get the S3 key for a job config."""
        return os.path.join(
            root_s3_key, f"job_name={job_name}", f"job_id={job_id}", "config.json"
        )

    def download_job_config(self, job_name: str, job_id: str) -> dict[str, Any]:
        """Download a job config from S3."""
        s3_key = self.get_job_config_key(job_name, job_id)
        try:
            result = self.s3.read_json_from_s3(s3_key)
            if not result:
                raise ValueError(
                    f"No config found for job name {job_name} and job id {job_id}."
                )
            return result
        except Exception as e:
            logger.error(f"Error downloading job config from {s3_key}: {e}")
