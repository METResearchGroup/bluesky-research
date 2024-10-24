# This bash script will create a cron job that runs the sbatch submit_job.sh once every 4 days at 8am UTC.

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Once every 4 days at 8am UTC
CRON_EXPRESSION="0 8 */4 * *"

# Define the cron job command
SBATCH_CRON_JOB="$CRON_EXPRESSION cd $DIR && sbatch submit_recommendation_pipeline_job.sh"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$SBATCH_CRON_JOB") | crontab -

echo "Cron job created to run sbatch submit_recommendation_pipeline_job.sh once every 4 days at 8am UTC."
