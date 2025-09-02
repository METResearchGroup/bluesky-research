#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 3:00:00
#SBATCH --mem=25G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=baseline_measures_analysis_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/baseline_measures_analysis-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/get_baseline_measures_across_all_posts_2025_09_02/main.py"

echo "Running python command: $PYTHON_CMD"

# Run the baseline measures analysis
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for baseline measures analysis"
python $PYTHON_CMD
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0
