"""Fetches all the feeds from S3.

1. Loads all the feed files from 2024-09-30 to 2024-12-01
2. Parses the feed files into dataframes.
3. Saves the dataframes locally.
"""

from datetime import datetime, timedelta
import os

from lib.aws.s3 import S3
from services.calculate_analytics.study_analytics.constants import (
    raw_data_root_path,
)

s3 = S3()

exclude_partition_date = "2024-10-08"  # server crashed on this date.

root_s3_uri = os.path.join("custom_feeds", "cached_custom_feeds")
export_dir = os.path.join(raw_data_root_path, "feeds")


def get_partition_dates(start_date: str, end_date: str) -> list[str]:
    """Returns a list of dates between start_date and end_date, inclusive.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of dates in YYYY-MM-DD format
    """
    partition_dates = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_timestamp:
        if current_date.strftime("%Y-%m-%d") == exclude_partition_date:
            current_date += timedelta(days=1)
            continue
        partition_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return partition_dates


def main():
    print("Fetching all the feeds...")
    # partition_dates: list[str] = get_partition_dates(start_date="2024-09-30", end_date="2024-12-01")
    partition_dates: list[str] = get_partition_dates(
        start_date="2024-09-28", end_date="2024-12-01"
    )
    for partition_date in partition_dates:
        print(f"Fetching {partition_date}...")
        folder_prefix = os.path.join(root_s3_uri, f"partition_date={partition_date}")
        print(f"Folder prefix: {folder_prefix}")
        keys = s3.get_keys_given_prefix(folder_prefix)
        print(f"Found {len(keys)} keys for {partition_date}...")

        backfilled_keys = [key for key in keys if "backfill" in key]

        # if there is a backfilled version of the keys, it's already compacted,
        # else download all the keys individually.
        if backfilled_keys:
            df = s3.load_keys_to_df(backfilled_keys)
        else:
            df = s3.load_keys_to_df(keys)
        export_filepath = os.path.join(export_dir, f"feeds_{partition_date}.parquet")
        df.to_parquet(export_filepath)
        print(f"Saved to {export_filepath}")
        print(f"Finished fetching {partition_date}.")
        print("-" * 10)
    print("Finished fetching all the feeds.")


if __name__ == "__main__":
    main()
