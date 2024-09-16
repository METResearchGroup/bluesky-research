#!/bin/bash

#SBATCH -A p32375
#SBATCH -p gengpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2
#SBATCH -t 0:30:00
#SBATCH --mem=10G
#SBATCH --job-name=compact_dedupe_data_job_jya0297_%j
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/ml_inference_perspective_api/jya0297-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python handler.py
echo "Completed slurm job."