"""Test file for experimenting with Prefect."""

import os

from prefect import task, flow

from orchestration.helper import pipelines_directory, run_slurm_job

current_directory = os.getcwd()


num_hours_kickoff = 3  # kick off pipeline every 3 hours
num_minutes_kickoff = 60 * num_hours_kickoff
num_seconds_kickoff = num_minutes_kickoff * 60


@task(log_prints=True)
def preprocess_raw_data():
    """Preprocesses the latest raw data."""
    bash_script_path = os.path.join(
        pipelines_directory, "preprocess_raw_data", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in preprocess_raw_data task")
    return result


@task(log_prints=True)
def calculate_superposters():
    """Calculates superposters."""
    bash_script_path = os.path.join(
        pipelines_directory, "calculate_superposters", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in calculate_superposters task")
    return result


@task(log_prints=True)
def run_ml_inference_perspective_api():
    """Performs ML inference on the latest data using the Perspective API."""
    bash_script_path = os.path.join(
        pipelines_directory, "classify_records", "perspective_api", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in run_ml_inference_perspective_api task")
    return result


@task(log_prints=True)
def run_ml_inference_sociopolitical():
    """Performs ML inference on the latest data using the Perspective API."""
    bash_script_path = os.path.join(
        pipelines_directory, "classify_records", "sociopolitical", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in run_ml_inference_sociopolitical task")
    return result


@task(log_prints=True)
def consolidate_enrichment_integrations():
    """Consolidates enrichment integrations."""
    bash_script_path = os.path.join(
        pipelines_directory, "consolidate_enrichment_integrations", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in consolidate_enrichment_integrations task")
    return result


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


@flow(name="Production data pipeline", log_prints=True)
def production_data_pipeline():
    # kick off preprocessing.
    job_preprocess_raw_data = preprocess_raw_data.submit()

    # run integrations concurrently after preprocessing is finished.
    job_calculate_superposters = calculate_superposters.submit(
        wait_for=[job_preprocess_raw_data]
    )
    job_run_ml_inference_perspective_api = run_ml_inference_perspective_api.submit(
        wait_for=[job_preprocess_raw_data]
    )
    job_run_ml_inference_sociopolitical = run_ml_inference_sociopolitical.submit(
        wait_for=[job_preprocess_raw_data]
    )

    # run enrichment integration consolidation after all integrations are finished.
    job_consolidate_enrichment_integrations = (
        consolidate_enrichment_integrations.submit(
            wait_for=[
                job_calculate_superposters,
                job_run_ml_inference_perspective_api,
                job_run_ml_inference_sociopolitical,
            ]
        )
    )

    # run scoring, ranking, and feed generation after enrichment integrations are finished.
    rank_score_feeds.submit(wait_for=[job_consolidate_enrichment_integrations])


if __name__ == "__main__":
    production_data_pipeline()

    production_data_pipeline.serve(
        name="Production data pipeline",
        tags=["slurm", "prod"],
        interval=num_seconds_kickoff,
    )