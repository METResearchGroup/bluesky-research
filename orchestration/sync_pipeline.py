"""Sync pipeline."""

import os

from prefect import task, flow

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
    # kick off the data pipeline as soon as the script is run.
    sync_data_pipeline(run_most_liked=True)

    # Deploy the main pipeline (runs daily, without most_liked)
    sync_data_pipeline.serve(
        name="Daily Sync Pipeline",
        tags=["slurm", "prod", "firehose"],
        cron="0 8 * * *",
        parameters={"run_most_liked": False},
    )

    # Deploy the most liked pipeline (runs every 4 hours)
    sync_most_liked_pipeline.serve(
        name="Most Liked Sync Pipeline",
        tags=["slurm", "prod", "most_liked"],
        cron="0 */4 * * *",
    )
