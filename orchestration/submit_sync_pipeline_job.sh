#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 48:00:00
#SBATCH --mem=10G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=sync_pipeline_job_jya0297_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/orchestration/sync_pipeline/jya0297-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting sync pipeline slurm job."
python "/projects/p32375/bluesky-research/orchestration/sync_pipeline.py"
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed sync pipeline slurm job."
exit 0
