#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 1:00:00
#SBATCH --mem=30G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=feed_bias_analysis_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/calculate_analytics/feed_selection_bias_analysis-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/correlation_analysis_2025_08_24/feed_selection_bias_analysis.py"

echo "Running python command: $PYTHON_CMD"

# Run the feed selection bias analysis
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for feed selection bias analysis"
python $PYTHON_CMD
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0
