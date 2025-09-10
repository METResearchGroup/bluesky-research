#!/bin/bash

# Local training script for topic modeling
# This script runs training locally (not on SLURM) with local data

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Hardcoded training parameters
MODE="local"
SAMPLE_PER_DAY=500
OUTPUT_DIR=""
FORCE_FALLBACK=""

echo "🚀 Starting Local Topic Model Training"
echo "======================================"
echo "Mode: $MODE"
echo "Sample per day: $SAMPLE_PER_DAY"
echo "Output dir: ${OUTPUT_DIR:-'./trained_models/local (default)'}"
echo "Force fallback: ${FORCE_FALLBACK:-'no'}"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Run the training
echo "🤖 Starting training..."
python train.py --mode "$MODE" --sample-per-day "$SAMPLE_PER_DAY" $OUTPUT_DIR $FORCE_FALLBACK

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Training completed successfully!"
    echo "📁 Check the trained_models/local/ directory for results"
else
    echo ""
    echo "❌ Training failed with exit code $exit_code"
    exit $exit_code
fi
