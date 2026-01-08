"""Loads all the feeds that were dumped locally and consolidates them into a
single parquet file.
"""

import json
import os

import numpy as np
import pandas as pd

from lib.datetime_utils import get_partition_dates
from services.calculate_analytics.study_analytics.constants import (
    raw_data_root_path,
    consolidated_data_root_path,
)

raw_filedir = os.path.join(raw_data_root_path, "feeds")
consolidated_filedir = os.path.join(consolidated_data_root_path, "feeds")


def serialize_feed(feed):
    if isinstance(feed, np.ndarray):
        return json.dumps(feed.tolist())
    elif isinstance(feed, (list, dict)):
        return json.dumps(feed)
    return json.dumps(feed) if feed is not None else None


def main():
    dfs: list[pd.DataFrame] = []
    partition_dates: list[str] = get_partition_dates(
        start_date="2024-09-28", end_date="2024-12-01"
    )
    for partition_date in partition_dates:
        print(f"Loading {partition_date}...")
        filepath = os.path.join(raw_filedir, f"feeds_{partition_date}.parquet")
        try:
            df = pd.read_parquet(filepath)
            print(f"Loaded {filepath}")
            if "feed_id" not in df.columns:
                df["feed_id"] = df.apply(
                    lambda row: f"{row['user']}::{row['feed_generation_timestamp']}",
                    axis=1,
                )
            if "partition_date" not in df.columns:
                df["partition_date"] = partition_date
            dfs.append(df)
        except FileNotFoundError:
            print(f"File not found: {filepath}")
    compacted_df = pd.concat(dfs)
    feeds_df = pd.DataFrame({"feed": compacted_df["feed"].apply(serialize_feed)})
    compacted_df["feed"] = feeds_df["feed"]
    compacted_df.to_parquet(
        os.path.join(consolidated_filedir, "feeds", "consolidated_feeds.parquet")
    )


if __name__ == "__main__":
    main()
