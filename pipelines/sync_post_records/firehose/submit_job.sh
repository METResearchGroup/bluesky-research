#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=3
#SBATCH -t 24:00:00
#SBATCH --mem=10G
#SBATCH --job-name=firehose_posts_job_jya0297_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/sync/firehose/jya0297-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python handler.py
echo "Completed slurm job."