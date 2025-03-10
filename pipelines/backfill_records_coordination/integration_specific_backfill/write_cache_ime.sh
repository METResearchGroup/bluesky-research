#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 0:30:00
#SBATCH --mem=30G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=write_cache_ime_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/backfill_records_coordination/write_cache_ime-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for writing cache
PYTHON_CMD="/projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py \
    --write-cache ml_inference_ime"

echo "Running python command: $PYTHON_CMD"

# Write cache to database
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting cache write job"
python $PYTHON_CMD
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0 