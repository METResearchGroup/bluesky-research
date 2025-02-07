#!/bin/bash

#SBATCH -A p32375
#SBATCH -p gengpu
#SBATCH --gres=gpu:a100:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH -t 2:00:00
#SBATCH --mem=15G
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=markptorres1@gmail.com
#SBATCH --job-name=backfill_records_coordination_ml_inference_ime_%j
#SBATCH --output=/projects/p32375/bluesky-research/lib/log/backfill_records_coordination/ml_inference_ime-%j.log

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH
echo "Starting slurm job."
python "/projects/p32375/bluesky-research/pipelines/backfill_records_coordination/app.py" --record-type posts --integration ml_inference_ime --run-integrations
exit_code=$?
echo "Python script exited with code $exit_code"
if [ $exit_code -ne 0 ]; then
    echo "Job failed with exit code $exit_code"
    exit $exit_code
fi
echo "Completed slurm job."

# Send log file via email
log_file="/projects/p32375/bluesky-research/lib/log/backfill_records_coordination/ml_inference_ime-${SLURM_JOB_ID}.log"
if [ -f "$log_file" ]; then
    mail -s "Log output for job ${SLURM_JOB_ID}" markptorres1@gmail.com < "$log_file"
fi

exit 0
