"""Loads all the user session logs that were dumped locally and consolidates
them into a single parquet file.
"""

import json
import os

import numpy as np
import pandas as pd

from scripts.analytics.helper import get_partition_dates

current_dir = os.path.dirname(os.path.abspath(__file__))
log_filedir = os.path.join(current_dir, "user_session_logs")


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
