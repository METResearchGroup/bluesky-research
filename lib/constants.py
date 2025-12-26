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


def convert_pipeline_to_bsky_dt_format(pipeline_dt: str) -> str:
    """Converts a pipeline datetime string to a Bluesky datetime string."""
    dt = datetime.strptime(pipeline_dt, timestamp_format)
    dt_formatted: str = dt.strftime(default_bsky_timestamp_format)
    return dt_formatted


def normalize_timestamp(timestamp: str) -> str:
    """Normalizes timestamps that use hour 24 instead of 00.

    Args:
        timestamp: A timestamp string that might contain hour 24

    Returns:
        Normalized timestamp with hour 00 of the next day
    """
    if "T24:" in timestamp:
        return timestamp.replace("T24:", "T00:")
    return timestamp


def try_default_ts_truncation(timestamp: str) -> str:
    """Truncates a timestamp to seconds precision.

    Args:
        timestamp: A timestamp string that starts with format "YYYY-MM-DDThh:mm:ss"
            but may have varying precision endings

    Returns:
        The timestamp truncated to seconds precision (YYYY-MM-DDThh:mm:ss)
    """
    return timestamp[:19]  # Truncate after seconds (19 chars: YYYY-MM-DDThh:mm:ss)


def convert_bsky_dt_to_pipeline_dt(bsky_dt: str) -> str:
    """Converts a Bluesky datetime string to a pipeline datetime string.

    There's been various timestamp formats used in Bluesky, so we try to convert
    the timestamp to a pipeline datetime string by trying each format.

    Timestamps are the worst...
    """
    bsky_dt = normalize_timestamp(bsky_dt)
    for bsky_timestamp_format in bsky_timestamp_formats:
        try:
            dt = datetime.strptime(bsky_dt, bsky_timestamp_format)
            dt_formatted: str = dt.strftime(timestamp_format)
            return dt_formatted
        except ValueError:
            continue
    default_ts_truncation = try_default_ts_truncation(bsky_dt)
    try:
        dt = datetime.strptime(default_ts_truncation, default_bsky_timestamp_format)
        dt_formatted: str = dt.strftime(timestamp_format)
        return dt_formatted
    except ValueError:
        raise ValueError(f"Invalid Bluesky datetime string: {bsky_dt}")
