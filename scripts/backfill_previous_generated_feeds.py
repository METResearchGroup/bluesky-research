"""Processes previous feeds and adds a 'feed_id' to each feed.

This needs to be done on the "cached" feeds in order to prove that it works,
and then can be done on the "active" feeds once that's verified.

Migration plan:
1. Copy the existing cached feeds in S3 to a new location, to have a backup.
2. Load in all posts as a pandas df SELECT * on the cached feeds.
3. Add the 'feed_id' field to the df.
4. Serialize the df to a JSON-dumped string and save to the 'feed' field.
5. Upsert the df back to the cached feeds table, overwriting the existing data.
    - Group by on partition_date and then write one file per partition_date.
    This is OK since each row will now have its correct feed_id based on the
    timestamp.

It looks like you can't change the name of the table created by the Glue crawler,
so I need to change the S3 paths to "cached_custom_feeds".
"""

from datetime import datetime, timedelta
import os

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.s3 import S3

athena = Athena()
s3 = S3()

start_date = "2024-10-01"
start_timestamp = datetime.strptime(start_date, "%Y-%m-%d")


def get_partition_dates() -> list[str]:
    partition_dates = []
    current_date = start_timestamp
    while current_date < datetime.now():
        partition_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return partition_dates


def load_cached_feeds_as_df(start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    query = f"""
        SELECT * 
        FROM cached_custom_feeds 
        WHERE partition_date >= '{start_date}'
        AND partition_date <= '{end_date}'
    """
    df = athena.query_results_as_df(query)
    if df.empty:
        print(f"No data found between {start_date} and {end_date}!")
        return {}

    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    df = pd.DataFrame(df_dicts)

    # Group by partition_date and create dictionary of dataframes
    grouped_dfs = {}
    for partition_date, group_df in df.groupby("partition_date"):
        grouped_dfs[partition_date] = group_df

    return grouped_dfs


def add_feed_id_col_to_df(df: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    if "feed_id" in df.columns and not df["feed_id"].isna().any():
        print(f"Skipping {partition_date} since it already has feed_id...")
        return df
    print(f"Adding feed_id to {df.shape[0]} rows for {partition_date}...")
    df["feed_id"] = df.apply(
        lambda row: f"{row['user']}::{row['feed_generation_timestamp']}", axis=1
    )
    return df


def upsert_df_to_cached_feeds(df: pd.DataFrame, partition_date: str) -> None:
    filename = f"backfilled_cached_custom_feeds_{partition_date}.jsonl"
    key = os.path.join(
        "custom_feeds",
        "cached_custom_feeds",
        f"partition_date={partition_date}",
        filename,
    )
    df_dicts = df.to_dict(orient="records")
    s3.write_dicts_jsonl_to_s3(data=df_dicts, key=key)


def main():
    start_date = "2024-10-24"
    end_date = "2024-11-10"
    dfs_map: dict[str, pd.DataFrame] = load_cached_feeds_as_df(start_date, end_date)
    if not dfs_map:
        print(f"Skipping {start_date} and {end_date} since they have no data.")
        return
    for partition_date, df in dfs_map.items():
        print(f"Processing {partition_date}...")
        df = add_feed_id_col_to_df(df, partition_date)
        upsert_df_to_cached_feeds(df=df, partition_date=partition_date)
        print(f"Done processing {partition_date}!")
    print("Done processing all dates.")


if __name__ == "__main__":
    main()
