#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 4:00:00
#SBATCH --mem=64G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_modeling_inference_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_inference_prod-%j.log

# Production inference script for topic modeling
# This script runs inference on SLURM with production data

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for inference
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/inference/run_inference_prod.py"

echo "🚀 Starting Production Topic Model Inference"
echo "==========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running python command: $PYTHON_CMD"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the inference
echo "🤖 Starting production inference..."
python $PYTHON_CMD

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production inference completed successfully!"
    echo "📁 Check the results/prod/ directory for results"
    echo "📊 Topic assignments and analysis completed"
else
    echo ""
    echo "❌ Production inference failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_inference_prod-$SLURM_JOB_ID.log"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
