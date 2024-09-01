# This bash script will create a cron job that runs the sbatch submit_job.sh every 6 hours.

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 6 hours
CRON_EXPRESSION="0 */6 * * *"

# Define the cron job command
SBATCH_CRON_JOB="$CRON_EXPRESSION cd $DIR && sbatch submit_job.sh >> /projects/p32375/bluesky-research/lib/log/sbatch_log.log"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$SBATCH_CRON_JOB") | crontab -

echo "Cron job created to run sbatch submit_job.sh every 6 hours."
