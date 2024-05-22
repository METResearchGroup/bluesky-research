#!/bin/bash

# This bash script will create a cron job that runs the sync pipeline via main.py everyday at 5pm Eastern Time (currently UTC-5)/4pm Central.
# It will execute two commands:
#   - python main.py --sync-type firehose
#   - python main.py --sync-type most_liked

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# 4pm Central
CRON_EXPRESSION="0 16 * * *"

# Define the cron job command
CRON_JOB_FIREHOSE="$CRON_EXPRESSION source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH && cd $DIR && python main.py --sync-type firehose >> /projects/p32375/bluesky-research/lib/log/logfile.log"
CRON_JOB_MOST_LIKED="$CRON_EXPRESSION source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH && cd $DIR && python main.py --sync-type most_liked >> /projects/p32375/bluesky-research/lib/log/logfile.log"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$CRON_JOB_FIREHOSE") | crontab -
(crontab -l 2>/dev/null; echo "$CRON_JOB_MOST_LIKED") | crontab -
#(crontab -l 2>/dev/null; echo "$TESTCRON") | crontab -

echo "Cron jobs created to run sync pipelines everyday at 4pm Central Time."
