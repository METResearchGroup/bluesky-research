"""Test file for experimenting with Prefect."""

import os

from prefect import task, flow
import subprocess

current_directory = os.getcwd()


def run_slurm_job(script_path):
    try:
        # use sbatch instead of bash when actually running the script on Quest.
        # result = subprocess.run(["sbatch", script_path], capture_output=True, text=True)
        result = subprocess.run(["bash", script_path], capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
        if result.returncode != 0:
            raise Exception(f"Error running SLURM job: {result.stderr}")
        return result.stdout.split()[-1]
    except Exception as e:
        print(f"Error running SLURM job: {e}")
        return None


@task
def job_A():
    return run_slurm_job(os.path.join(current_directory, "test_a.sh"))


@task
def job_B():
    return run_slurm_job(os.path.join(current_directory, "test_b.sh"))


@task
def job_C():
    return run_slurm_job(os.path.join(current_directory, "test_c.sh"))


@task
def job_D():
    return run_slurm_job(os.path.join(current_directory, "test_d.sh"))


@task
def job_E():
    return run_slurm_job(os.path.join(current_directory, "test_e.sh"))


@task
def job_F():
    return run_slurm_job(os.path.join(current_directory, "test_f.sh"))


@flow(name="test_flow")
def test_flow():
    # a is first job
    job_a = job_A()

    # b, c, d need to run concurrently and are kicked off after a
    job_b = job_B.submit(wait_for=[job_a])
    job_c = job_C.submit(wait_for=[job_a])
    job_d = job_D.submit(wait_for=[job_a])

    # job e kicked off after b, c, and d are finished.
    job_e = job_E.submit(wait_for=[job_b, job_c, job_d])

    # job f kicked off after e is finished.
    job_F.submit(wait_for=[job_e])


if __name__ == "__main__":
    test_flow.serve(
        name="test-flow-deployment",
        tags=["test", "slurm"],
        interval=60,  # Run every 60 seconds
    )
