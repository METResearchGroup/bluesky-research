#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=32G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_modeling_umap_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_umap_prod-%j.log

# Production UMAP visualization script for topic modeling
# This script runs sliced UMAP visualizations on SLURM with production data

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for UMAP visualization
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/visualization/run_umap_prod.py"

echo "🎨 Starting Production UMAP Visualizations"
echo "=========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running python command: $PYTHON_CMD"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the UMAP visualizations
echo "🎨 Starting production UMAP visualizations..."
python $PYTHON_CMD

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production UMAP visualizations completed successfully!"
    echo "📁 Check the visualization/results/prod/ directory for results"
    echo "🖼️ Multiple sliced UMAP plots created"
else
    echo ""
    echo "❌ Production UMAP visualizations failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_umap_prod-$SLURM_JOB_ID.log"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
