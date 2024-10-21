"""Sync pipeline."""

import os

from prefect import task, flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from orchestration.helper import pipelines_directory, run_slurm_job


@task(log_prints=True)
def sync_firehose():
    """Syncs the firehose data."""
    bash_script_path = os.path.join(
        pipelines_directory, "sync_post_records", "firehose", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path, timeout_ok=True)
    if result is None:
        raise Exception("SLURM job failed in sync_firehose task")
    return result


@task(log_prints=True)
def write_firehose_data():
    """Writes the streamed firehose data to persistent store."""
    bash_script_path = os.path.join(
        pipelines_directory,
        "sync_post_records",
        "firehose",
        "submit_firehose_writes_job.sh",
    )
    result = run_slurm_job(bash_script_path, timeout_ok=True)
    if result is None:
        raise Exception("SLURM job failed in write_firehose_data task")
    return result


@task(log_prints=True)
def sync_most_liked():
    """Syncs the most liked posts."""
    bash_script_path = os.path.join(
        pipelines_directory, "sync_post_records", "most_liked", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path, timeout_ok=False)
    if result is None:
        raise Exception("SLURM job failed in sync_most_liked task")
    return result


@flow(name="Sync pipeline", log_prints=True)
def sync_data_pipeline(run_most_liked=True):
    """Syncs the data pipeline."""
    sync_firehose.submit(wait_for=False)
    write_firehose_data.submit(wait_for=False)

    if run_most_liked:
        sync_most_liked.submit(wait_for=False)


@flow(name="Most Liked Sync", log_prints=True)
def sync_most_liked_pipeline():
    """Syncs only the most liked posts."""
    sync_most_liked.submit()


if __name__ == "__main__":
    # Create deployments
    # NOTE: shouldn't be triggered since "sync_data_pipeline(run_most_liked=True)"
    # will be a long-running process.
    # daily_sync_deployment = Deployment.build_from_flow(
    #     flow=sync_data_pipeline,
    #     name="Daily Sync Pipeline",
    #     schedule=CronSchedule(cron="0 8 * * *"),
    #     tags=["slurm", "prod", "firehose"],
    #     parameters={"run_most_liked": False},
    # )

    most_liked_deployment = Deployment.build_from_flow(
        flow=sync_most_liked_pipeline,
        name="Most Liked Sync Pipeline",
        schedule=CronSchedule(cron="0 */4 * * *"),
        tags=["slurm", "prod", "most_liked"],
    )

    # Apply deployments
    # daily_sync_deployment.apply()
    most_liked_deployment.apply()

    # Run the initial sync_data_pipeline
    sync_data_pipeline(run_most_liked=True)
