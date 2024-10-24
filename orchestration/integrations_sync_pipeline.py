"""Integrations sync pipeline."""

import os

from prefect import task, flow

from orchestration.helper import pipelines_directory, run_slurm_job

num_hours_kickoff = 2  # kick off pipeline every 2 hours
num_minutes_kickoff = 60 * num_hours_kickoff
num_seconds_kickoff = num_minutes_kickoff * 60

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

@flow(name="Integrations Sync Pipeline", log_prints=True)
def integrations_sync_pipeline():
    """Syncs integrations."""
    sync_most_liked.submit()


if __name__ == "__main__":
    integrations_sync_pipeline()

    integrations_sync_pipeline.serve(
        name="Integrations Sync Pipeline",
        tags=["slurm", "prod"],
        interval=num_seconds_kickoff,
    )
