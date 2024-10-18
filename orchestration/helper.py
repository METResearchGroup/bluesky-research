"""Helper functions for orchestration."""

import os

import subprocess
import time

from lib.constants import root_directory, repo_name

pipelines_directory = os.path.join(root_directory, repo_name, "pipelines")


def run_slurm_job(script_path: str, timeout_ok: bool = False) -> str:
    try:
        # Submit the job
        result = subprocess.run(
            ["sbatch", script_path], capture_output=True, text=True, check=True
        )
        job_id = result.stdout.split()[-1]
        print(f"Submitted batch job {job_id}")

        # Wait for the job to complete
        while True:
            status = subprocess.run(
                ["squeue", "-j", job_id, "-h"], capture_output=True, text=True
            )
            if status.stdout.strip() == "":
                # Job is no longer in the queue, assume it's completed
                break
            time.sleep(30)  # Check every 30 seconds

        # Check the job's exit status.
        sacct_result = subprocess.run(
            ["sacct", "-j", job_id, "--format=State,ExitCode", "-n"],
            capture_output=True,
            text=True,
            check=True,
        )
        job_statuses = [
            line.strip().split()
            for line in sacct_result.stdout.splitlines()
            if line.strip()
        ]

        # Check if any job step failed, considering TIMEOUT as successful (we have some
        # long-running jobs that are allowed to timeout since Slurm will kill the jobs).
        for status, exit_code in job_statuses:
            if timeout_ok:
                if status not in ["COMPLETED", "TIMEOUT"] or (
                    status != "TIMEOUT" and exit_code.split(":")[0] != "0"
                ):
                    raise Exception(f"SLURM job failed. Job steps: {job_statuses}")
            else:
                if status != "COMPLETED" or exit_code.split(":")[0] != "0":
                    raise Exception(f"SLURM job failed. Job steps: {job_statuses}")

        print(f"SLURM job completed successfully. Job ID: {job_id}")
        return job_id
    except subprocess.CalledProcessError as e:
        error_message = f"Error running SLURM job: {e}"
        print(error_message)
        raise Exception(error_message)
    except Exception as e:
        error_message = f"Error checking SLURM job status: {e}"
        print(error_message)
        raise Exception(error_message)
