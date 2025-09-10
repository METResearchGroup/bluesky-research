#!/bin/bash

#SBATCH -A p32375
#SBATCH -p normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 8:00:00
#SBATCH --mem=50G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=topic_modeling_analysis_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/topic_modeling_analysis-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/main.py"

echo "Running python command: $PYTHON_CMD"

# Run the topic modeling analysis
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for topic modeling analysis"
python $PYTHON_CMD
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0
