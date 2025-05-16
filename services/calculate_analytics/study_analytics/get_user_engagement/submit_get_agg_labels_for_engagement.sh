#!/bin/bash

#SBATCH -A p32375
#SBATCH -p short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=25G
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=get_agg_labels_for_engagements_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/study_analytics/get_agg_labels_for_engagements-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# Build python command
PYTHON_CMD="/projects/p32375/bluesky-research/services/calculate_analytics/study_analytics/get_user_engagement/get_agg_labels_for_engagements.py"

echo "Running python command: $PYTHON_CMD"

# Run the get_agg_labels_for_engagements analysis
source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job for get_agg_labels_for_engagements analysis"
python $PYTHON_CMD
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."
exit 0
