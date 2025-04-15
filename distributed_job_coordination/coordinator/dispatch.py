"""
Job dispatch functionality for submitting jobs to Slurm.

This module provides utilities for submitting distributed jobs to Slurm,
including job arrays and resource allocation.
"""

import subprocess

from distributed_job_coordination.lib.job_config import ComputeConfig
from distributed_job_coordination.lib.job_state import TaskState
from distributed_job_coordination.worker.generate_slurm_script import (
    generate_slurm_script,
)
from lib.log.logger import get_logger

logger = get_logger(__name__)


class DispatchError(Exception):
    """Exception raised for job dispatch related errors."""

    pass


class Dispatch:
    """
    Dispatch class for submitting and managing jobs.
    """

    def __init__(self):
        pass

    def submit_batch_to_slurm(
        compute_config: ComputeConfig, task_states: list[TaskState]
    ) -> dict:
        """Submits a batch to Slurm.

        Returns a map of batch IDs to Slurm IDs, in order to update the
        respective TaskState objects with the correct worker IDs."""

        job_id = task_states[0].job_id
        job_name = task_states[0].job_name

        bash_script_path = generate_slurm_script(
            compute_config=compute_config, job_id=job_id
        )

        task_ids = [task_state.id for task_state in task_states]

        map_task_id_to_slurm_id = {}

        for task_id in task_ids:
            command = f"sbatch ./{bash_script_path} --job_name {job_name} --job_id {job_id} --task_id {task_id}"
            logger.info(f"Submitting task {task_id} to Slurm with command: {command}")
            slurm_id = subprocess.check_output(command, shell=True).decode().strip()
            map_task_id_to_slurm_id[task_id] = slurm_id

        return map_task_id_to_slurm_id

    def cancel_slurm_job():
        """Cancels a single job, including all of its downstream tasks.

        A 'job' here is a single distributed job across tasks, NOT a slurm 'job'.
        Here, a task == a single Slurm 'job'. We cancel all the Slurm jobs
        (i.e., tasks) related to a single distributed job."""
        pass

    def cancel_slurm_task():
        """Cancels a single task (i.e., a single Slurm job)."""
        pass
