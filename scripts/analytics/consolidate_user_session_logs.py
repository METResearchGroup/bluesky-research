"""Loads all the user session logs that were dumped locally and consolidates
them into a single parquet file.
"""

from datetime import datetime, timedelta
import json
import os

import numpy as np
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
log_filedir = os.path.join(current_dir, "user_session_logs")


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
        partition_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return partition_dates


def serialize_feed(feed):
    if isinstance(feed, np.ndarray):
        return json.dumps(feed.tolist())
    elif isinstance(feed, (list, dict)):
        return json.dumps(feed)
    return json.dumps(feed) if feed is not None else None


def main():
    partition_dates = get_partition_dates(
        start_date="2024-09-29", end_date="2024-12-01"
    )
    dfs: list[pd.DataFrame] = []
    for partition_date in partition_dates:
        filepath = os.path.join(log_filedir, f"{partition_date}.parquet")
        try:
            df = pd.read_parquet(filepath)
        except FileNotFoundError:
            print(f"File not found: {filepath}")
            continue
        dfs.append(df)
    compacted_df = pd.concat(dfs)
    feeds_df = pd.DataFrame({"feed": compacted_df["feed"].apply(serialize_feed)})
    compacted_df["feed"] = feeds_df["feed"]
    compacted_df.to_parquet(
        os.path.join(current_dir, "consolidated_user_session_logs.parquet")
    )


if __name__ == "__main__":
    main()
