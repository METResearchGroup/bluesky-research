#!/bin/bash
# Resources for submitting jobs: https://services.northwestern.edu/TDClient/30/Portal/KB/ArticleDet?ID=1796
# Full list of directives: https://services.northwestern.edu/TDClient/30/Portal/KB/ArticleDet?ID=1795

#SBATCH --account=p32375 # project ID
#SBATCH --partition gengpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:a100:1
#SBATCH --time 0:10:00
#SBATCH --mem=2G
#SBATCH --constraint=pcie
#SBATCH --job-name=test_job_jya0297
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/logfile_test-%j.log

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

PYTHONFILE_NAME="test_script_1.py"

echo "Starting slurm job."

# print all vars
echo "DIR: $DIR"
echo "CONDA_PATH: $CONDA_PATH"
echo "PYTHONPATH: $PYTHONPATH"

source $CONDA_PATH
conda activate bluesky_research
export PYTHONPATH=$PYTHONPATH
cd $DIR && python $PYTHONFILE_NAME

echo "Completed slurm job."
