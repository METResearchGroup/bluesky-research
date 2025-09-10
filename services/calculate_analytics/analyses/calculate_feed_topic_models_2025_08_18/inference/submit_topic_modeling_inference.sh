#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 4:00:00
#SBATCH --mem=32G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_modeling_inference_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_inference-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for inference
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/inference/inference.py"

# Parse command line arguments
MODE="prod"
MODEL_PATH=""
METADATA_PATH=""
OUTPUT_DIR=""
FORCE_FALLBACK=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --model-path)
            MODEL_PATH="$2"
            shift 2
            ;;
        --metadata-path)
            METADATA_PATH="$2"
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
            echo "Usage: $0 --model-path PATH --metadata-path PATH [--mode local|prod] [--output-dir DIR] [--force-fallback]"
            echo ""
            echo "Required arguments:"
            echo "  --model-path PATH     Path to trained model directory"
            echo "  --metadata-path PATH  Path to model metadata JSON file"
            echo ""
            echo "Optional arguments:"
            echo "  --mode local|prod     Data loading mode (default: prod)"
            echo "  --output-dir DIR      Output directory for results (default: ./results)"
            echo "  --force-fallback      Force conservative configuration"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$MODEL_PATH" ]; then
    echo "Error: --model-path is required"
    echo "Usage: $0 --model-path PATH --metadata-path PATH [options...]"
    exit 1
fi

if [ -z "$METADATA_PATH" ]; then
    echo "Error: --metadata-path is required"
    echo "Usage: $0 --model-path PATH --metadata-path PATH [options...]"
    exit 1
fi

# Validate that paths exist
if [ ! -d "$MODEL_PATH" ]; then
    echo "Error: Model path does not exist: $MODEL_PATH"
    exit 1
fi

if [ ! -f "$METADATA_PATH" ]; then
    echo "Error: Metadata path does not exist: $METADATA_PATH"
    exit 1
fi

echo "Running topic model inference with:"
echo "  Mode: $MODE"
echo "  Model path: $MODEL_PATH"
echo "  Metadata path: $METADATA_PATH"
echo "  Output dir: ${OUTPUT_DIR:-'default'}"
echo "  Force fallback: ${FORCE_FALLBACK:-'no'}"

# Run the topic modeling inference
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for topic model inference"
python $PYTHON_CMD --mode $MODE --model-path "$MODEL_PATH" --metadata-path "$METADATA_PATH" $OUTPUT_DIR $FORCE_FALLBACK
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job for inference."
exit 0
