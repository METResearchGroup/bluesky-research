#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 1:00:00
#SBATCH --mem=20G
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=backfill_posts_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/backfill_records_coordination/backfill_posts-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Parse command line arguments
PARTITION_DATE=""
START_DATE=""
END_DATE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --partition_date)
            PARTITION_DATE="$2"
            shift 2
            ;;
        --start_date)
            START_DATE="$2"
            shift 2
            ;;
        --end_date)
            END_DATE="$2"
            shift 2
            ;;
        *)
            echo "Error: Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Check arguments
if [ ! -z "$PARTITION_DATE" ]; then
    START_DATE=$PARTITION_DATE
    END_DATE=$PARTITION_DATE
elif [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
    echo "Error: Either --partition_date OR both --start_date and --end_date must be provided"
    exit 1
fi

# Build python command with integration flag
PYTHON_CMD="/projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py \
    --record-type posts \
    --add-to-queue \
    --start-date $START_DATE \
    --end-date $END_DATE \
    --run-integrations \
    --integration v"

PYTHON_CMD_2="/projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py \
    --write-cache ml_inference_valence_classifier --clear-queue"

echo "Running python command: $PYTHON_CMD"
echo "Then, running python command: $PYTHON_CMD_2"

# insert backfill records into queues for partition date
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for dates: $START_DATE to $END_DATE"
python $PYTHON_CMD
echo "Completed backfilling records to queue. Now clearing queue and writing to permanent storage."
python $PYTHON_CMD_2
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0 