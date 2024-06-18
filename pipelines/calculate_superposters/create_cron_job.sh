#!/bin/bash

# This bash script will create a cron job that calculates superposters via main.py everyday at 2am Eastern Time (currently UTC-5)/1am Central.
# It will execute the following
#   - python main.py

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# 4pm Central
CRON_EXPRESSION="0 1 * * *"

# Define the cron job command
PIPELINE_CRON_JOB="$CRON_EXPRESSION source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH && cd $DIR && python main.py >> /projects/p32375/bluesky-research/lib/log/logfile.log"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$PIPELINE_CRON_JOB") | crontab -

echo "Cron jobs created to calculate superposters everyday at 1am Central Time (6am UTC time)."
