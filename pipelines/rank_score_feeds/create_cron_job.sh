# This bash script will create a cron job that runs the sbatch submit_job.sh every 6 hours.

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Every 2 hours, starting at the 45 minute mark
CRON_EXPRESSION="45 */2 * * *"

# Define the cron job command
SBATCH_CRON_JOB="$CRON_EXPRESSION cd $DIR && sbatch submit_job.sh"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$SBATCH_CRON_JOB") | crontab -

# Define TTL cron job
# Every 6 hours, starting at the 0 minute mark
TTL_CRON_EXPRESSION="0 */6 * * *"

# Define the TTL cron job command
TTL_CRON_JOB="$TTL_CRON_EXPRESSION cd $DIR && sbatch submit_feed_ttl_job.sh"

# Add the TTL cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$TTL_CRON_JOB") | crontab -

echo "Cron job created to run sbatch submit_job.sh every 2 hours, starting at the 45 minute mark."
echo "TTL cron job created to run ttl_old_feeds.py every 6 hours, starting at the 0 minute mark."
