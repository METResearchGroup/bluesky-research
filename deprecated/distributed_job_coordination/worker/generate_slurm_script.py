"""
Module for generating Slurm scripts for task workers.

This module provides functionality to generate Slurm scripts based on compute configuration.
"""

import os
from pathlib import Path

from distributed_job_coordination.lib.job_config import ComputeConfig


def generate_slurm_script(compute_config: ComputeConfig, job_id: str) -> str:
    """
    Generate a Slurm script based on the provided compute configuration.

    Args:
        compute_config: Configuration for compute resources
        job_id: The job ID to use in the script

    Returns:
        The path to the generated script
    """
    # Format job name and output log path
    job_name = compute_config.job_name.format(job_id=job_id)
    output_log = compute_config.output_log_path.format(job_name=job_name)

    # Build GPU configuration if needed
    gpu_config = ""
    if compute_config.gpu_count and compute_config.gpu_type:
        gpu_config = (
            f"#SBATCH --gres=gpu:{compute_config.gpu_type}:{compute_config.gpu_count}"
        )

    # Build mail configuration
    mail_config = f"#SBATCH --mail-type={compute_config.mail_type}"
    if compute_config.mail_user:
        mail_config += f"\n#SBATCH --mail-user={compute_config.mail_user}"

    # Generate the script
    script = f"""#!/bin/bash

#SBATCH -A {compute_config.account}
#SBATCH -p {compute_config.partition}
#SBATCH --nodes={compute_config.nodes}
#SBATCH --ntasks-per-node={compute_config.ntasks_per_node}
#SBATCH -t {compute_config.max_runtime}
#SBATCH --mem={compute_config.memory_gb}G
{mail_config}
#SBATCH --job-name={job_name}
#SBATCH --output={output_log}
{gpu_config}

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for task worker."

python -m distributed_job_coordination.worker.task_worker \\
    --job_name "${{1}}" \\
    --job_id "${{2}}" \\
    --task_id "${{3}}"

exit_code=$?
echo "Task worker exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0
"""

    # Get the directory where this script is located
    current_dir = Path(__file__).parent
    output_path = os.path.join(current_dir, "submit_task_worker.sh")

    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(script)
    # Make the script executable
    os.chmod(output_path, 0o755)

    return output_path
