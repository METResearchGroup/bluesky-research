#!/bin/bash

#SBATCH -A p32375
#SBATCH -p gengpu
#SBATCH --gres=gpu:a100:1
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -t 0:30:00
#SBATCH --mem=10G
#SBATCH --job-name=generate_vector_embeddings_job_jya0297_%j
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=markptorres1@gmail.com

# NOTE: submit with `sbatch create_cron_job.sh`

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python handler.py
echo "Completed slurm job."