"""Vector embeddings pipeline orchestration."""

import os

from prefect import flow, task

from orchestration.helper import pipelines_directory, run_slurm_job


num_hours_kickoff = 24
num_minutes_kickoff = 60 * num_hours_kickoff
num_seconds_kickoff = num_minutes_kickoff * 60


@task(log_prints=True)
def generate_vector_embeddings():
    """Generates vector embeddings for posts (GPU SLURM job)."""
    bash_script_path = os.path.join(
        pipelines_directory, "generate_vector_embeddings", "submit_job.sh"
    )
    result = run_slurm_job(bash_script_path)
    if result is None:
        raise Exception("SLURM job failed in generate_vector_embeddings task")
    return result


@flow(name="Vector embeddings pipeline", log_prints=True)
def vector_embeddings_pipeline():
    generate_vector_embeddings.submit()


if __name__ == "__main__":
    vector_embeddings_pipeline()

    vector_embeddings_pipeline.serve(
        name="Vector embeddings pipeline",
        tags=["slurm", "prod", "gpu", "embeddings"],
        interval=num_seconds_kickoff,
    )
