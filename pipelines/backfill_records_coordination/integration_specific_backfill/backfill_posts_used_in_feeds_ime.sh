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

# Parse command line arguments
PARTITION_DATE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        *)
            if [ -z "$PARTITION_DATE" ]; then
                PARTITION_DATE="$1"
                shift
            else
                echo "Error: Unexpected argument: $1"
                exit 1
            fi
            ;;
    esac
done

# Check required arguments
if [ -z "$PARTITION_DATE" ]; then
    echo "Error: partition_date argument is required"
    exit 1
fi

# Build python command with integration flag
PYTHON_CMD="/projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py \
    --record-type posts_used_in_feeds \
    --add-to-queue \
    --start-date $PARTITION_DATE \
    --end-date $PARTITION_DATE \
    --integration i"

echo "Running python command: $PYTHON_CMD"

# insert backfill records into queues for partition date
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for partition date: $PARTITION_DATE"
python $PYTHON_CMD
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0 