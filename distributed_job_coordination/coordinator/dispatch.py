"""
Job dispatch functionality for submitting jobs to Slurm.

This module provides utilities for submitting distributed jobs to Slurm,
including job arrays and resource allocation.
"""

import logging


logger = logging.getLogger(__name__)


class DispatchError(Exception):
    """Exception raised for job dispatch related errors."""

    pass


class Dispatch:
    """
    Dispatch class for submitting and managing jobs.
    """

    def __init__(self):
        pass

    def submit_batch_to_slurm() -> dict:
        """Submits a batch to Slurm.

        Returns a map of batch IDs to Slurm IDs, in order to update the
        respective TaskState objects with the correct worker IDs."""
        pass

    def cancel_slurm_job():
        """Cancels a single job, including all of its downstream tasks.

        A 'job' here is a single distributed job across tasks, NOT a slurm 'job'.
        Here, a task == a single Slurm 'job'. We cancel all the Slurm jobs
        (i.e., tasks) related to a single distributed job."""
        pass

    def cancel_slurm_task():
        """Cancels a single task (i.e., a single Slurm job)."""
        pass
