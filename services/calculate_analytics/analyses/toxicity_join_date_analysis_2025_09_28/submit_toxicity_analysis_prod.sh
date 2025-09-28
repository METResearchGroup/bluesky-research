#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=24G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=toxicity_analysis_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/toxicity_analysis_prod-%j.log

# Production Toxicity vs Join Date Analysis script
# This script runs toxicity analysis on SLURM with production data
# Analyzes correlation between user join dates and toxicity/outrage levels

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for toxicity analysis
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/toxicity_join_date_analysis_2025_09_28/get_author_to_average_toxicity_outrage.py"

echo "🧪 Starting Production Toxicity vs Join Date Analysis"
echo "=================================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running python command: $PYTHON_CMD"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Time Limit: $SLURM_TIME_LIMIT"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the toxicity analysis
echo "🧪 Starting production toxicity analysis..."
echo "📊 Features:"
echo "  - Sequential processing of daily Perspective API posts (memory-safe)"
echo "  - Author-to-average toxicity/outrage score calculation"
echo "  - User join date correlation analysis"
echo "  - Data quality validation and monitoring"
echo "  - Comprehensive export to local storage and S3"
echo ""

python $PYTHON_CMD

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production toxicity analysis completed successfully!"
    echo "📁 Check the results directory for outputs:"
    echo "  - Author-to-average toxicity/outrage scores by partition date"
    echo "  - Data quality metrics and validation reports"
    echo "  - Exported data to local storage and S3"
    echo "  - Processing logs and performance metrics"
    echo ""
    echo "📊 Analysis includes:"
    echo "  - Perspective API toxicity and moral outrage scores"
    echo "  - User posting frequency analysis"
    echo "  - Join date correlation calculations"
    echo "  - Data integrity validation"
    echo "  - Sequential processing for memory safety"
else
    echo ""
    echo "❌ Production toxicity analysis failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/toxicity_analysis_prod-$SLURM_JOB_ID.log"
    echo ""
    echo "🔧 Common troubleshooting:"
    echo "  - Ensure Perspective API data is available for study dates"
    echo "  - Check preprocessed posts data availability"
    echo "  - Verify conda environment has all required packages (pandas, etc.)"
    echo "  - Check data permissions and storage access"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
