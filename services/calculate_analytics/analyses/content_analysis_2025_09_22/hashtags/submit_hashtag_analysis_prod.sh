#!/bin/bash
#SBATCH --job-name=hashtag_analysis_prod
#SBATCH --output=hashtag_analysis_prod_%j.out
#SBATCH --error=hashtag_analysis_prod_%j.err
#SBATCH --time=2:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=ALL
#SBATCH --mail-user=mark@example.com

# Set up environment
export PYTHONPATH="/Users/mark/Documents/work/bluesky-research:$PYTHONPATH"

# Activate conda environment
source ~/miniconda3/etc/profile.d/conda.sh
conda activate bluesky_research

# Navigate to project directory
cd /Users/mark/Documents/work/bluesky-research

# Log start time
echo "Starting hashtag analysis at $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE MB"

# Run the hashtag analysis
echo "Running hashtag analysis..."
python -m services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.main

# Check exit status
if [ $? -eq 0 ]; then
    echo "Hashtag analysis completed successfully at $(date)"
    echo "Results saved to: services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags/results/"
else
    echo "Hashtag analysis failed with exit code $? at $(date)"
    exit 1
fi

# Log completion
echo "Job completed at $(date)"
echo "Total runtime: $SECONDS seconds"
