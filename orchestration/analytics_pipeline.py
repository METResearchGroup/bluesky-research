"""Analytics pipeline logic."""

import os

from prefect import task, flow

from orchestration.helper import pipelines_directory, run_slurm_job


@task(log_prints=True)
def compact_user_session_logs():
    """Compacts the user session logs."""
    bash_script_path = os.path.join(
        pipelines_directory, "compact_user_session_logs", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path, timeout_ok=False)
    if result is None:
        raise Exception("SLURM job failed in compact_user_session_logs task")
    return result



@task(log_prints=True)
def aggregate_study_user_activities():
    """Aggregates all study user activities."""
    bash_script_path = os.path.join(
        pipelines_directory, "aggregate_study_user_activities", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path, timeout_ok=False)
    if result is None:
        raise Exception("SLURM job failed in aggregate_study_user_activities task")
    return result


@flow(name="Analytics pipeline", log_prints=True)
def analytics_pipeline():
    """Aggregates study user activities and then runs analytics."""
    job_compact_user_session_logs = compact_user_session_logs.submit(wait_for=False)
    aggregate_study_user_activities.submit(wait_for=[job_compact_user_session_logs])


if __name__ == "__main__":
    analytics_pipeline()

    # deploy the analytics pipeline
    analytics_pipeline.serve(
        name="Analytics pipeline",
        tags=["slurm", "prod", "analytics"],
        cron="0 8 * * *",
    )
