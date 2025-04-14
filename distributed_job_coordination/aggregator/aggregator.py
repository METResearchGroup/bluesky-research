"""Aggregates the results of a job across the various tasks."""

import os

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
from distributed_job_coordination.lib.s3_utils import S3Utils
from lib.helper import create_batches
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
        # TODO: need a new method, something like "load_task_results_from_scratch"
        # that has the "results.parquet" file.
        return self.storage_manager.load_batch_from_scratch(
            task_id=task_id,
            filename_prefix=self.job_name,
        )

    def validate_task_outputs(self, task_outputs: list[dict]) -> list[dict]:
        """Validates the task outputs."""
        return task_outputs

    # NOTE: should have an interface with StorageManager.
    # NOTE: using the same interface for both the regular batch and the
    # intermediate aggregation batches.
    def export_aggregated_batch_result(self, task_outputs: list[dict]):
        """Exports the aggregated batch result."""
        return None

    def aggregate_single_batch(self, batch: list[TaskState]) -> str:
        """Aggregates a single batch of task states.

        Writes the results to a temporary location.
        """
        task_outputs: list[dict] = []
        for task in batch:
            task_outputs.extend(self.load_task_output(task.id))

        validated_task_outputs: list[dict] = self.validate_task_outputs(task_outputs)
        # NOTE: might also help to include deduplication here?
        tmp_output_fp = self.export_aggregated_batch_result(validated_task_outputs)
        logger.info(f"Exported outputs from {len(batch)} tasks to {tmp_output_fp}.")

        return tmp_output_fp

    def aggregate_intermediate_results(self, batch_fpaths: list[str]) -> str:
        """Aggregates intermediate results.

        Returns the final aggregated filepath.
        """
        task_outputs: list[dict] = []
        for fpath in batch_fpaths:
            # NOTE: check if this is the most efficient way to do this.
            # TODO: try other ways to do this that could be more efficient.
            df = pd.read_parquet(fpath)
            task_outputs.extend(df.to_dict(orient="records"))

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

    # TODO: can do this in an intermediate scratch location,
    # something like /job_name=<job_name>/job_id=<job_id>/aggregation/
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

    # TODO: can just copy/move final file from aggregation into the
    # final location.
    def write_aggregated_results(self):
        """Writes the aggregated results to the output location."""
        pass

    def write_manifest(self):
        """Writes the manifest for the aggregated results."""
        pass

    def aggregate(self):
        """Aggregates the results of a job across the various tasks."""
        self.run_hierarchical_aggregation()
        self.write_aggregated_results()
        self.write_manifest()


def kickoff_aggregator(job_id: str):
    aggregator = Aggregator(job_id=job_id)
    aggregator.aggregate()
