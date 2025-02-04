"""Fetches all the user session logs from S3."""

from datetime import datetime, timedelta
import os

from lib.aws.s3 import S3
from services.calculate_analytics.study_analytics.constants import (
    raw_data_root_path,
)

s3 = S3()

root_s3_uri = "user_session_logs"

exclude_partition_date = "2024-10-13"  # server crashed on this date.

export_dir = os.path.join(raw_data_root_path, "user_session_logs")


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
    print("Fetching all the user session logs...")
    partition_dates: list[str] = get_partition_dates(
        start_date="2024-09-30", end_date="2024-12-01"
    )
    for partition_date in partition_dates:
        if partition_date == exclude_partition_date:
            continue
        print(f"Fetching {partition_date}...")
        folder_prefix = os.path.join(root_s3_uri, f"partition_date={partition_date}")
        print(f"Folder prefix: {folder_prefix}")
        keys = s3.get_keys_given_prefix(folder_prefix)
        print(f"Found {len(keys)} keys for {partition_date}...")
        compacted_keys = [key for key in keys if "compacted" in key]

        # if there is a compacted version of the keys, it's already compacted,
        # else download all the keys individually.
        if compacted_keys:
            print(f"Found {len(compacted_keys)} compacted keys for {partition_date}...")
            df = s3.load_keys_to_df(compacted_keys)
        else:
            print(
                f"No compacted keys found for {partition_date}. Loading {len(keys)} keys..."
            )
            df = s3.load_keys_to_df(keys)
        export_filepath = os.path.join(
            export_dir, f"user_session_logs_{partition_date}.parquet"
        )
        df.to_parquet(export_filepath)
        print(f"Saved to {export_filepath}")
        print(f"Finished fetching {partition_date}.")
        print("-" * 10)
    print("Finished fetching all the user session logs.")


if __name__ == "__main__":
    main()
