#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 1:00:00
#SBATCH --mem=16G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=toxicity_aggregation_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/toxicity_aggregation_prod-%j.log

# Production Toxicity Aggregation script
# This script aggregates daily author-to-average toxicity/outrage data
# across all study days to create comprehensive author-level metrics

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for toxicity aggregation
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/toxicity_join_date_analysis_2025_09_28/aggregate_author_to_average_toxicity_across_days.py"

echo "🧪 Starting Production Toxicity Aggregation"
echo "============================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running python command: $PYTHON_CMD"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Time Limit: $SLURM_TIME_LIMIT"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the toxicity aggregation
echo "🧪 Starting production toxicity aggregation..."
echo "📊 Features:"
echo "  - Sequential loading of daily author-toxicity data"
echo "  - Weighted average calculation across all study days"
echo "  - Author-level aggregation with total post counts"
echo "  - Comprehensive data validation and quality checks"
echo "  - Timestamped export to results directory"
echo ""

python $PYTHON_CMD

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production toxicity aggregation completed successfully!"
    echo "📁 Check the results directory for outputs:"
    echo "  - Aggregated author-to-average toxicity/outrage scores"
    echo "  - Weighted averages across all study days"
    echo "  - Total post counts per author"
    echo "  - Data sorted by post frequency (descending)"
    echo ""
    echo "📊 Analysis includes:"
    echo "  - Cross-day toxicity and moral outrage aggregation"
    echo "  - Author-level posting frequency analysis"
    echo "  - Weighted average calculations (posts_per_day * avg_per_day)"
    echo "  - Data integrity validation across all partition dates"
    echo "  - Comprehensive export with metadata"
else
    echo ""
    echo "❌ Production toxicity aggregation failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/toxicity_aggregation_prod-$SLURM_JOB_ID.log"
    echo ""
    echo "🔧 Common troubleshooting:"
    echo "  - Ensure daily author-toxicity data is available for all study dates"
    echo "  - Check that get_author_to_average_toxicity_outrage.py has been run successfully"
    echo "  - Verify conda environment has all required packages (pandas, etc.)"
    echo "  - Check data permissions and storage access"
    echo "  - Ensure sufficient memory for loading all daily data"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
