"""Compaction pipeline logic.

Logic is to do the following:
- Run compaction service.
- Run data snapshot service.
"""

import os

from prefect import task, flow

from orchestration.helper import pipelines_directory, run_slurm_job


@task(log_prints=True)
def compact_all_services():
    """Compacts all services."""
    bash_script_path = os.path.join(
        pipelines_directory, "compact_all_services", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path, timeout_ok=False)
    if result is None:
        raise Exception("SLURM job failed in compact_all_services task")
    return result


@task(log_prints=True)
def snapshot_data():
    """Snapshots the data."""
    bash_script_path = os.path.join(
        pipelines_directory, "snapshot_data", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path, timeout_ok=False)
    if result is None:
        raise Exception("SLURM job failed in snapshot_data task")
    return result


@flow(name="Compaction pipeline", log_prints=True)
def compaction_pipeline():
    """Compacts all services and then snapshots the data."""
    job_compact_all_services = compact_all_services.submit(wait_for=False)
    snapshot_data.submit(wait_for=[job_compact_all_services])


if __name__ == "__main__":
    compaction_pipeline()

    # Deploy the compaction pipeline
    compaction_pipeline.serve(
        name="Compaction pipeline",
        tags=["slurm", "prod", "compaction"],
        cron="0 7,19 * * *",
    )
