"""Test file for experimenting with Prefect."""

import os

from prefect import task, flow
import subprocess

from lib.constants import root_directory

current_directory = os.getcwd()
pipelines_directory = os.path.join(root_directory, "pipelines")


num_hours_kickoff = 3 # kick off pipeline every 3 hours
num_minutes_kickoff = 60 * num_hours_kickoff
num_seconds_kickoff = num_minutes_kickoff * 60


def run_slurm_job(script_path):
    try:
        result = subprocess.run(["sbatch", script_path], capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
        if result.returncode != 0:
            raise Exception(f"Error running SLURM job: {result.stderr}")
        return result.stdout.split()[-1]
    except Exception as e:
        print(f"Error running SLURM job: {e}")
        return None


@task
def preprocess_raw_data():
    """Preprocesses the latest raw data."""
    bash_script_path = os.path.join(
        pipelines_directory, "preprocess_raw_data", "submit_job.sh"
    )
    return run_slurm_job(bash_script_path)


@task
def calculate_superposters():
    """Calculates superposters."""
    bash_script_path = os.path.join(
        pipelines_directory, "calculate_superposters", "submit_job.sh"
    )
    return run_slurm_job(bash_script_path)


@task
def run_ml_inference_perspective_api():
    """Performs ML inference on the latest data using the Perspective API."""
    bash_script_path = os.path.join(
        pipelines_directory, "classify", "perspective_api", "submit_job.sh"
    )
    return run_slurm_job(bash_script_path)


@task
def run_ml_inference_sociopolitical():
    """Performs ML inference on the latest data using the Perspective API."""
    bash_script_path = os.path.join(
        pipelines_directory, "classify", "sociopolitical", "submit_job.sh"
    )
    return run_slurm_job(bash_script_path)


@task
def consolidate_enrichment_integrations():
    """Consolidates enrichment integrations."""
    bash_script_path = os.path.join(
        pipelines_directory, "consolidate_enrichment_integrations", "submit_job.sh"
    )
    return run_slurm_job(bash_script_path)


@task
def rank_score_feeds()
    """Scores posts, ranks them, and generates feeds."""
    bash_script_path = os.path.join(
        pipelines_directory, "rank_score_feeds", "submit_job.sh"
    )
    return run_slurm_job(bash_script_path)


@flow(name="Production data pipeline")
def production_data_pipeline():

    # kick off preprocessing.
    job_preprocess_raw_data = preprocess_raw_data.submit()

    # run integrations concurrently after preprocessing is finished.
    job_calculate_superposters = calculate_superposters.submit(wait_for=[job_preprocess_raw_data])
    job_run_ml_inference_perspective_api = run_ml_inference_perspective_api.submit(wait_for=[job_preprocess_raw_data])
    job_run_ml_inference_sociopolitical = run_ml_inference_sociopolitical.submit(wait_for=[job_preprocess_raw_data])

    # run enrichment integration consolidation after all integrations are finished.
    job_consolidate_enrichment_integrations = consolidate_enrichment_integrations.submit(
        wait_for=[
            job_calculate_superposters,
            job_run_ml_inference_perspective_api,
            job_run_ml_inference_sociopolitical
        ]
    )
    
    # run scoring, ranking, and feed generation after enrichment integrations are finished.
    rank_score_feeds.submit(wait_for=[job_consolidate_enrichment_integrations])


if __name__ == "__main__":
    production_data_pipeline.serve(
        name="Production data pipeline",
        tags=["slurm", "prod"],
        interval=num_seconds_kickoff,
    )
