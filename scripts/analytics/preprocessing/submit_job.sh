#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2
#SBATCH -t 0:30:00
#SBATCH --mem=20G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=adhoc_analytics_job_jya0297_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/adhoc-analytics/jya0297-%j.log

# NOTE: submit with `sbatch submit_job.sh`

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python "/projects/p32375/bluesky-research/scripts/analytics/preprocessing/export_consolidated_posts_to_duckdb.py"
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0