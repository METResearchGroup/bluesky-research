#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=32G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_modeling_training_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_training-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for training
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/train/train.py"

# Parse command line arguments
MODE="prod"
SAMPLE_PER_DAY=500
OUTPUT_DIR=""
FORCE_FALLBACK=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --sample-per-day)
            SAMPLE_PER_DAY="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="--output-dir $2"
            shift 2
            ;;
        --force-fallback)
            FORCE_FALLBACK="--force-fallback"
            shift
            ;;
        *)
            echo "Unknown option $1"
            echo "Usage: $0 [--mode local|prod] [--sample-per-day N] [--output-dir DIR] [--force-fallback]"
            exit 1
            ;;
    esac
done

echo "Running topic model training with:"
echo "  Mode: $MODE"
echo "  Sample per day: $SAMPLE_PER_DAY"
echo "  Output dir: ${OUTPUT_DIR:-'default'}"
echo "  Force fallback: ${FORCE_FALLBACK:-'no'}"

# Run the topic modeling training
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for topic model training"
python $PYTHON_CMD --mode $MODE --sample-per-day $SAMPLE_PER_DAY $OUTPUT_DIR $FORCE_FALLBACK
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job for training."
exit 0
