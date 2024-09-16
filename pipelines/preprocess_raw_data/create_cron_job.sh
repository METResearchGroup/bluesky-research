# This bash script will create a cron job that runs the sbatch submit_job.sh every 6 hours.

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 45 minutes
CRON_EXPRESSION="*/45 * * * *"

# Define the cron job command
SBATCH_CRON_JOB="$CRON_EXPRESSION cd $DIR && sbatch submit_job.sh"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$SBATCH_CRON_JOB") | crontab -

echo "Cron job created to run sbatch submit_job.sh every 45 minutes."
