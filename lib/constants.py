"""Constants for general use."""

from datetime import datetime, timezone
import os

# likely that we will want to filter out some authors (e.g., spam accounts)
BLOCKED_AUTHORS = []
# we only want recent posts (Bluesky docs recommend 3 days, see )
NUM_DAYS_POST_RECENCY = 3
timestamp_format = "%Y-%m-%d-%H:%M:%S"
default_bsky_timestamp_format = "%Y-%m-%dT%H:%M:%S"  # e.g. "2024-01-15T13:45:30"
bsky_timestamp_formats = [
    default_bsky_timestamp_format,
    timestamp_format,  # e.g., "2024-01-15-13:45:30", this is the default that we use in the pipeline; some records may have this already
    "%Y-%m-%dT%H:%M:%S.%fZ",  # e.g. "2024-01-15T13:45:30.000000Z"
    "%Y-%m-%dT%H:%M:%SZ",  # e.g. "2024-01-15T13:45:30Z"
    "%Y-%m-%dT%H:%M:%S.%f%z",  # e.g. "2024-01-15T13:45:30.000000+0000"
    "%Y-%m-%dT%H:%M:%S%z",  # e.g. "2024-10-20T17:52:03+00:00"
    "%Y-%m-%dT%H:%M:%S.%f",  # e.g. "2024-10-20T18:36:54.5935595"
    "%Y-%m-%dT%H:%M:%S.%fZ",  # e.g. "2024-10-20T18:36:54.5935595Z" (microseconds precision)
]

study_start_date = (
    "2024-09-29"  # think it's 09/30 but we offset by 1 for timezone-related edge cases
)
study_end_date = "2024-12-01"

partition_date_format = "%Y-%m-%d"
current_datetime = datetime.now(timezone.utc)
current_datetime_str = current_datetime.strftime(timestamp_format)


current_file_directory = os.path.dirname(os.path.abspath(__file__))
# level above git directory.
project_home_directory = os.path.abspath(os.path.join(current_file_directory, ".."))
root_directory = os.path.abspath(os.path.join(current_file_directory, "../.."))
repo_name = "bluesky-research"
root_data_dirname = "bluesky_research_data"
backup_data_dirname = "backup_bluesky_research_data"
root_local_data_directory = os.path.join(root_directory, root_data_dirname)
root_local_backup_data_directory = os.path.join(root_directory, backup_data_dirname)

default_lookback_days = 2

TEST_USER_HANDLES = [
    "testblueskyaccount.bsky.social",
    "testblueskyuserv2.bsky.social",
    "markptorres.bsky.social",
]

STUDY_CONDITIONS = [
    "representative_diversification",
    "engagement",
    "reverse_chronological",
]

INTEGRATION_RUN_METADATA_TABLE_NAME = "integration_run_metadata"
