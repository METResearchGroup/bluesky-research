"""Recommendation pipeline orchestration."""

import os

from prefect import task, flow

from orchestration.helper import pipelines_directory, run_slurm_job


num_hours_kickoff = 4  # kick off pipeline every 4 hours
num_minutes_kickoff = 60 * num_hours_kickoff
num_seconds_kickoff = num_minutes_kickoff * 60

@task(log_prints=True)
def rank_score_feeds():
    """Scores posts, ranks them, and generates feeds."""
    bash_script_path = os.path.join(
        pipelines_directory, "rank_score_feeds", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in rank_score_feeds task")
    return result


@flow(name="Recommendation pipeline", log_prints=True)
def recommendation_pipeline():
    rank_score_feeds.submit()


if __name__ == "__main__":
    recommendation_pipeline()

    recommendation_pipeline.serve(
        name="Recommendation pipeline",
        tags=["slurm", "prod"],
        interval=num_seconds_kickoff,
    )
