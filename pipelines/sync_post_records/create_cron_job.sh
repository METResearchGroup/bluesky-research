#!/bin/bash

# This bash script will create a cron job that runs the sync pipeline via main.py everyday at 6pm Eastern Time (currently UTC-5).
# It will execute two commands:
#   - python main.py --sync_type firehose
#   - python main.py --sync_type most_liked

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define the cron job command
CRON_JOB="0 23 * * * cd $DIR && python main.py --sync_type firehose && python main.py --sync_type most_liked"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Cron job created to run main.py everyday at 6pm Eastern Time."
