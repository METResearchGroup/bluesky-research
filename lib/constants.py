"""Constants for general use."""

from datetime import datetime, timezone
import os

# likely that we will want to filter out some authors (e.g., spam accounts)
BLOCKED_AUTHORS = []
# we only want recent posts (Bluesky docs recommend 3 days, see )
NUM_DAYS_POST_RECENCY = 3
timestamp_format = "%Y-%m-%d-%H:%M:%S"
bsky_timestamp_format = "%Y-%m-%dT%H:%M:%S.000Z"
current_datetime = datetime.now(timezone.utc)
current_datetime_str = current_datetime.strftime(timestamp_format)


current_file_directory = os.path.dirname(os.path.abspath(__file__))
# level above git directory.
root_directory = os.path.abspath(os.path.join(current_file_directory, "../.."))
root_data_dirname = "bluesky_research_data"
root_local_data_directory = os.path.join(root_directory, root_data_dirname)


def convert_pipeline_to_bsky_dt_format(pipeline_dt: str) -> str:
    """Converts a pipeline datetime string to a Bluesky datetime string."""
    dt = datetime.strptime(pipeline_dt, timestamp_format)
    dt_formatted: str = dt.strftime(bsky_timestamp_format)
    return dt_formatted
