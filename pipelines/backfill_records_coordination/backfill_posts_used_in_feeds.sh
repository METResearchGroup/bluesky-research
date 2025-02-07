#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 0:30:00
#SBATCH --mem=20G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=backfill_posts_used_in_feeds_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/backfill_records_coordination/backfill_posts_used_in_feeds-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# insert backfill records into queues, for date range.
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python /projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py \
    --record-type posts_used_in_feeds \
    --add-to-queue \
    --start-date 2024-10-01 \
    --end-date 2024-10-03
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0
