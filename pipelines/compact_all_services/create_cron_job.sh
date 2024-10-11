# This bash script will create a cron job that runs the sbatch submit_job.sh twice per day, starting at 2am Central Time (7am UTC).

# Get the current directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run twice per day at 2am and 2pm Central Time (7am and 7pm UTC)
CRON_EXPRESSION="0 7,19 * * *"

# Define the cron job command
SBATCH_CRON_JOB="$CRON_EXPRESSION cd $DIR && sbatch submit_job.sh"

# Add the cron job to the current user's crontab
(crontab -l 2>/dev/null; echo "$SBATCH_CRON_JOB") | crontab -

echo "Cron job created to run sbatch submit_job.sh twice per day at 2am and 2pm Central Time (7am and 7pm UTC)."
