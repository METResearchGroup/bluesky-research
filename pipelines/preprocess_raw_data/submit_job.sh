#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2
#SBATCH -t 0:30:00
#SBATCH --mem=10G
#SBATCH --job-name=preprocess_raw_data_job_jya0297_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/preprocess_raw_data/jya0297-%j.log

# NOTE: submit with `sbatch create_cron_job.sh`

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python handler.py
echo "Completed slurm job."
