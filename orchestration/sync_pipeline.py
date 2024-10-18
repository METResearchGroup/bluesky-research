"""Sync pipeline."""

from datetime import timedelta
import os

from prefect import task, flow
from prefect.schedules import IntervalSchedule


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
    job_sync_firehose = sync_firehose.submit()
    job_write_firehose_data = write_firehose_data.submit(wait_for=[job_sync_firehose])

    if run_most_liked:
        sync_most_liked.submit(wait_for=[job_write_firehose_data])


@flow(name="Most Liked Sync", log_prints=True)
def sync_most_liked_pipeline():
    """Syncs only the most liked posts."""
    sync_most_liked.submit()


if __name__ == "__main__":
    # Schedule for daily pipeline (including firehose)
    daily_schedule = IntervalSchedule(interval=timedelta(days=1))

    # Schedule for most liked posts (every 4 hours)
    most_liked_schedule = IntervalSchedule(interval=timedelta(hours=4))

    # Deploy the main pipeline (runs daily, without most_liked)
    sync_data_pipeline.serve(
        name="Daily Sync Pipeline",
        tags=["slurm", "prod", "firehose"],
        schedule=daily_schedule,
        parameters={"run_most_liked": False},
    )

    # Deploy the most liked pipeline (runs every 4 hours)
    sync_most_liked_pipeline.serve(
        name="Most Liked Sync Pipeline",
        tags=["slurm", "prod", "most_liked"],
        schedule=most_liked_schedule,
    )

    # kick off the data pipeline as soon as the script is run.
    sync_data_pipeline(run_most_liked=True)
