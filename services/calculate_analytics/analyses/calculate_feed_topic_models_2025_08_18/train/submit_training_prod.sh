#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=32G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_modeling_training_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_training_prod-%j.log

# Production training script for topic modeling
# This script runs training on SLURM with production data and export functionality

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Hardcoded production training parameters
MODE="prod"
SAMPLE_PER_DAY=500
OUTPUT_DIR=""
FORCE_FALLBACK=""

echo "🚀 Starting Production Topic Model Training"
echo "=========================================="
echo "Mode: $MODE"
echo "Sample per day: $SAMPLE_PER_DAY"
echo "Output dir: ${OUTPUT_DIR:-'./trained_models/prod (default)'}"
echo "Force fallback: ${FORCE_FALLBACK:-'no'}"
echo "SLURM Job ID: $SLURM_JOB_ID"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the training
echo "🤖 Starting production training..."
python train.py --mode "$MODE" --sample-per-day "$SAMPLE_PER_DAY" $OUTPUT_DIR $FORCE_FALLBACK

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production training completed successfully!"
    echo "📁 Check the trained_models/prod/ directory for results"
    echo "📊 Model saved and ready for inference"
else
    echo ""
    echo "❌ Production training failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_training_prod-$SLURM_JOB_ID.log"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
