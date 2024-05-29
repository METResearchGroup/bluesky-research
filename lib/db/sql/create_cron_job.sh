#!/bin/bash

# This bash script will create a cron job to back up the SQL DBs every 8 hours.
# It will execute the following command:
#   - python backup_databases.py

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# load conda env
CONDA_PATH="/hpc/software/mamba/23.1.0/etc/profile.d/conda.sh"

# set pythonpath
PYTHONPATH="/projects/p32375/bluesky-research/:$PYTHONPATH"

# cron expression for running every 8 hours
CRON_EXPRESSION="0 */8 * * *"

# Define the cron job command
CRON_JOB="$CRON_EXPRESSION source $CONDA_PATH && conda activate bluesky_research && export PYTHONPATH=$PYTHONPATH && cd $DIR && python backup_databases.py >> /projects/p32375/bluesky-research/lib/log/logfile.log"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Cron jobs created to automatically run database backups."
