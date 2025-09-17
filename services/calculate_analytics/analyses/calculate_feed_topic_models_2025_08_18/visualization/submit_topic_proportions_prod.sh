#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 1:00:00
#SBATCH --mem=16G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_proportions_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_proportions_prod-%j.log

# Production Topic Proportion Visualization script for topic modeling
# This script runs structured topic proportion visualizations on SLURM with production data

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for topic proportion visualization
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/visualization/run_topic_proportions_prod.py"

echo "📊 Starting Production Topic Proportion Visualizations"
echo "========================================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running python command: $PYTHON_CMD"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Time Limit: $SLURM_TIME_LIMIT"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the topic proportion visualizations
echo "📊 Starting production topic proportion visualizations..."
python $PYTHON_CMD

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production topic proportion visualizations completed successfully!"
    echo "📁 Check the visualization/results/topic_proportions/prod/ directory for results"
    echo "📊 Structured visualizations created with consistent top 10 topics"
    echo "📂 Subfolders: overall/, condition/, election_date/, election_date/by_condition/"
else
    echo ""
    echo "❌ Production topic proportion visualizations failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/topic_proportions_prod-$SLURM_JOB_ID.log"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
