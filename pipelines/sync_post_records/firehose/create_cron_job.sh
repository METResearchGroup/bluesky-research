# This bash script will create a cron job that runs the sbatch submit_job.sh.

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Every day at 8am UTC
CRON_EXPRESSION="0 8 * * *"

# Define the cron job command
SBATCH_CRON_JOB="$CRON_EXPRESSION cd $DIR && sbatch submit_job.sh && sbatch submit_firehose_writes_job.sh"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$SBATCH_CRON_JOB") | crontab -

echo "Cron job created to run sbatch submit_job.sh every day at 8am UTC."
