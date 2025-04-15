"""Aggregates the results of a job across the various tasks."""

import gc
import json
import os
import shutil

import pandas as pd

from distributed_job_coordination.aggregator.constants import (
    aggregation_dir,
    aggregator_batch_size,
)
from distributed_job_coordination.coordinator.storage import StorageManager
from distributed_job_coordination.lib.constants import root_scratch_dir
from distributed_job_coordination.lib.dynamodb_utils import (
    JobStateStore,
    TaskStateStore,
)
from distributed_job_coordination.lib.job_config import JobConfig
from distributed_job_coordination.lib.job_state import TaskState
from distributed_job_coordination.lib.manifest import ManifestWriter
from distributed_job_coordination.lib.s3_utils import S3Utils
from lib.db.manage_local_data import export_data_to_local_storage
from lib.helper import create_batches, generate_current_datetime_str
from lib.log.logger import get_logger

aggregation_scratch_path = os.path.join(
    root_scratch_dir,
    aggregation_dir,
)

logger = get_logger(__name__)
s3_utils = S3Utils()


class Aggregator:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.job_state_store = JobStateStore()
        self.job_state = self.job_state_store.load_job_state(self.job_id)
        self.job_name = self.job_state.name

        self.job_config: JobConfig = s3_utils.download_job_config(
            job_name=self.job_name,
            job_id=self.job_id,
        )
        self.output_format = self.job_config.output.format
        self.output_compression = self.job_config.output.compression
        self.output_location = self.job_config.output.output_location
        self.output_partition_keys = self.job_config.output.partition_keys
        self.output_service_name = self.job_config.output.output_service_name
        self.task_output_queue_prefix = self.job_config.output.task_output_queue_prefix

        if self.output_service_name:
            logger.info(
                f"""
                    Service {self.output_service_name} is being used for output.
                    The aggregator will write to the service's configuration as
                    defined in `service_constants.py` and ignore this config's
                    `output_location` and `partition_keys`.
                """,
            )

        self.task_state_store = TaskStateStore()
        self.task_states = self.task_state_store.load_task_states_for_job(self.job_id)
        self.batched_task_states: list[list[TaskState]] = create_batches(
            self.task_states, aggregator_batch_size
        )
        self.storage_manager = StorageManager(
            job_name=self.job_name,
            job_id=self.job_id,
        )
        self.aggregation_scratch_path: str = (
            self.storage_manager.get_scratch_path_for_aggregation(
                job_name=self.job_name,
                job_id=self.job_id,
            )
        )

    def load_task_output(self, task_id: str) -> list[dict]:
        """Loads the output for a task."""
        return self.storage_manager.load_task_results_from_scratch(
            task_id=task_id,
            task_output_queue_prefix=self.task_output_queue_prefix,
        )

    def validate_task_outputs(self, task_outputs: list[dict]) -> list[dict]:
        """Validates the task outputs."""
        return task_outputs

    def export_aggregated_batch_result(
        self, task_outputs: list[dict], results_prefix: str
    ):
        """Exports the aggregated batch result."""
        # NOTE: check if pandas is the most efficient way to do this. Likely not.
        df = pd.DataFrame(task_outputs)
        output_path = self.storage_manager.write_aggregation_results_to_scratch(
            results=df,
            results_prefix=results_prefix,
        )
        del df
        gc.collect()
        logger.info(f"Exported aggregated batch results to {output_path}.")
        return output_path

    def aggregate_single_batch(self, batch: list[TaskState], batch_idx: int) -> str:
        """Aggregates a single batch of task states.

        Writes the results to a temporary location.
        """
        task_outputs: list[dict] = []
        for task in batch:
            task_outputs.extend(self.load_task_output(task.id))

        validated_task_outputs: list[dict] = self.validate_task_outputs(task_outputs)
        # NOTE: might also help to include deduplication here?
        results_prefix = f"{self.job_name}-{self.job_id}-{batch_idx}"
        tmp_output_fp = self.export_aggregated_batch_result(
            task_outputs=validated_task_outputs,
            results_prefix=results_prefix,
        )
        logger.info(f"Exported outputs from {len(batch)} tasks to {tmp_output_fp}.")

        return tmp_output_fp

    def aggregate_intermediate_results(self, batch_fpaths: list[str]) -> str:
        """Aggregates intermediate results.

        Returns the final aggregated filepath.
        """
        # list of dicts might not be optimally efficient TBH. Should consider
        # something like Polars soon.
        task_outputs: list[dict] = []
        for fpath in batch_fpaths:
            # NOTE: check if this is the most efficient way to do this.
            # TODO: try other ways to do this that could be more efficient.
            df = pd.read_parquet(fpath)
            task_outputs.extend(df.to_dict(orient="records"))
            del df
            gc.collect()

        validated_task_outputs: list[dict] = self.validate_task_outputs(task_outputs)
        tmp_output_fp = self.export_aggregated_batch_result(validated_task_outputs)
        logger.info(
            f"Exported outputs from {len(batch_fpaths)} files to {tmp_output_fp}."
        )
        return tmp_output_fp

    def recursively_aggregate_intermediate_results(
        self, batch_fpaths: list[str]
    ) -> list[str]:
        """Recursively aggregates intermediate results.

        Returns the new list of batch filepaths.
        """
        new_batch_fpaths: list[list[str]] = create_batches(
            batch_fpaths, aggregator_batch_size
        )
        output_batch_fpaths: list[str] = []
        for batch_fpaths in new_batch_fpaths:
            tmp_output_fp = self.aggregate_intermediate_results(batch_fpaths)
            output_batch_fpaths.append(tmp_output_fp)
        return output_batch_fpaths

    def run_hierarchical_aggregation(self):
        """Runs hierarchical aggregation on the task states.

        This will aggregate the task states in a hierarchical manner,

        It will first chunk the task batches into smaller batches, and then
        aggregate those and write the intermediate results into temporary files.

        If there are multiple temporary files, it will attempt to load the temporary
        files in batches and then aggregate those. This will happen recursively
        until there is only one file left, and then this is the final aggregated
        output.
        """
        # start by aggregating the batched task states.
        batch_fpaths: list[str] = []
        for task_batch in self.batched_task_states:
            tmp_output_fp = self.aggregate_single_batch(task_batch)
            batch_fpaths.append(tmp_output_fp)

        while len(batch_fpaths) > 1:
            # then recursively aggregate the intermediate results.
            batch_fpaths = self.recursively_aggregate_intermediate_results(batch_fpaths)

        # write the final aggregated results.
        self.write_aggregated_results(batch_fpaths[0])

    def write_aggregated_results(self, fpath: str):
        """Writes the aggregated results to the output location."""
        # TODO: check if this is the most efficient way to do this.
        if self.output_service_name:
            df = pd.read_parquet(fpath)
            export_data_to_local_storage(
                service=self.output_service_name,
                df=df,
                export_format=self.output_format,
            )
            del df
            gc.collect()
        else:
            if not os.path.exists(self.output_location):
                os.makedirs(self.output_location)
            output_path = os.path.join(
                self.output_location,
                f"{self.job_name}-{self.job_id}.parquet",
            )
            if self.output_compression:
                df = pd.read_parquet(fpath)
                df.to_parquet(
                    output_path,
                    compression=self.output_compression,
                    index=False,
                    partition_cols=self.output_partition_keys
                    if self.output_partition_keys
                    else None,
                )
                del df
                gc.collect()
                logger.info(
                    f"Compressed and wrote aggregated results to {output_path} with {self.output_compression} compression"
                )
            else:
                shutil.copy2(fpath, output_path)

    def write_manifest(self):
        """Writes the manifest for the aggregated results."""
        aggregation_manifest: dict = self._generate_aggregation_manifest()
        manifests_by_type = {
            "aggregation": [aggregation_manifest],
        }
        manifest_writer = ManifestWriter(manifests_by_type=manifests_by_type)
        manifest_writer.write_manifests()
        logger.info(f"Wrote manifest for aggregated results for job {self.job_id}.")

    def _generate_aggregation_manifest(self) -> dict:
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "output_service_name": self.output_service_name,
            "output_format": self.output_format,
            "output_compression": self.output_compression,
            "output_partition_keys": self.output_partition_keys,
            "completed_at": generate_current_datetime_str(),
            "metadata": json.dumps({}),
        }

    def aggregate(self):
        """Aggregates the results of a job across the various tasks."""
        self.run_hierarchical_aggregation()
        self.write_aggregated_results()
        self.write_manifest()


def kickoff_aggregator(job_id: str):
    aggregator = Aggregator(job_id=job_id)
    aggregator.aggregate()
