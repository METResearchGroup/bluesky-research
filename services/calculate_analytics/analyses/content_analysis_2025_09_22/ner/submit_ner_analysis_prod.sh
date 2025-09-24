#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=16G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=ner_analysis_prod_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/ner_analysis_prod-%j.log

# Production NER Analysis script for entity extraction and visualization
# This script runs Named Entity Recognition analysis on SLURM with production data
# Includes multithreaded entity extraction and comprehensive visualizations

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load conda environment
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# Set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command for NER analysis
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/content_analysis_2025_09_22/ner/main.py"

echo "🔍 Starting Production NER Analysis"
echo "=================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running python command: $PYTHON_CMD"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Time Limit: $SLURM_TIME_LIMIT"
echo ""

# Change to script directory
cd "$SCRIPT_DIR"

# Activate conda environment and set PYTHONPATH
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH

# Run the NER analysis
echo "🔍 Starting production NER analysis..."
echo "📊 Features:"
echo "  - Multithreaded entity extraction (auto-scaling)"
echo "  - Political/sociopolitical entity focus (PERSON, ORG, GPE, DATE)"
echo "  - Condition-based analysis (Reverse Chronological, Engagement, Diversified)"
echo "  - Pre/post election period analysis"
echo "  - Comprehensive visualizations and CSV exports"
echo ""

python $PYTHON_CMD

# Check exit code
exit_code=$?
if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 Production NER analysis completed successfully!"
    echo "📁 Check the results directory for outputs:"
    echo "  - Entity frequency analysis by condition"
    echo "  - Pre/post election entity comparisons"
    echo "  - Rank change visualizations (tornado charts)"
    echo "  - CSV exports of top entities"
    echo "  - Comprehensive visualization metadata"
    echo ""
    echo "📊 Analysis includes:"
    echo "  - Top 20 entities per condition"
    echo "  - Election period stratification (cutoff: 2024-11-05)"
    echo "  - Entity normalization and alias mapping"
    echo "  - Professional visualizations with consistent styling"
else
    echo ""
    echo "❌ Production NER analysis failed with exit code $exit_code"
    echo "📋 Check the log file for details: /projects/p32375/bluesky-research/lib/log/study_analytics/ner_analysis_prod-$SLURM_JOB_ID.log"
    echo ""
    echo "🔧 Common troubleshooting:"
    echo "  - Ensure spaCy model is installed: python -m spacy download en_core_web_sm"
    echo "  - Check data availability and permissions"
    echo "  - Verify conda environment has all required packages"
    exit $exit_code
fi

echo "✅ SLURM job completed successfully"
exit 0
