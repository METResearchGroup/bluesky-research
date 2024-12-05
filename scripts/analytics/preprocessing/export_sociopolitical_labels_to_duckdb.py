"""Exports the sociopolitical labels to DuckDB.

Exporting to a different database as we're using it for a different purpose.

Loads in all the labeled sociopolitical data and inserts into DuckDB.
"""

import gc
import os

import duckdb
import pandas as pd

from scripts.analytics.helper import get_partition_dates, load_raw_parquet_data_as_df
from scripts.analytics.duckdb_helper import insert_df_to_duckdb

db_name = "sociopolitical_labels"
# Create new DB connection for sociopolitical labels
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, f"{db_name}.db")
conn = duckdb.connect(db_path)

missing_firehose_dates = ["2024-10-08", "2024-10-09", "2024-10-11"]
missing_most_liked_dates = ["2024-10-09", "2024-10-11"]

base_firehose_path = "/projects/p32375/bluesky_research_data/ml_inference_sociopolitical/firehose/cache"
base_most_liked_path = "/projects/p32375/bluesky_research_data/ml_inference_sociopolitical/most_liked/cache"

def load_firehose_sociopolitical_labels(partition_date: str) -> pd.DataFrame:
    if partition_date not in missing_firehose_dates:
        print(f"Loading firehose sociopolitical labels for partition date={partition_date}")
        path = os.path.join(base_firehose_path, f"partition_date={partition_date}")
        return load_raw_parquet_data_as_df(path)
    print(f"No firehose sociopolitical labels for partition date={partition_date}")
    return None

def load_most_liked_labels(partition_date: str) -> pd.DataFrame:
    if partition_date not in missing_most_liked_dates:
        print(f"Loading most liked sociopolitical labels for partition date={partition_date}")
        path = os.path.join(base_most_liked_path, f"partition_date={partition_date}")
        return load_raw_parquet_data_as_df(path)
    print(f"No most liked sociopolitical labels for partition date={partition_date}")


def main():
    total_count = 0
    partition_dates = get_partition_dates(
        start_date="2024-09-28", end_date="2024-12-01"
    )
    for current_partition_date in partition_dates:
        print(f"Processing partition date={current_partition_date}")
        firehose_df = load_firehose_sociopolitical_labels(current_partition_date)
        most_liked_df = load_most_liked_labels(current_partition_date)
        if firehose_df is None or len(firehose_df) == 0:
            df = most_liked_df
        elif most_liked_df is None or len(most_liked_df) == 0:
            df = firehose_df
        else:
            df = pd.concat([firehose_df, most_liked_df], ignore_index=True)
        if df is None or len(df) == 0:
            print(f"No sociopolitical labels for partition date={current_partition_date}. Skipping...")
            continue
        insert_df_to_duckdb(df, "sociopolitical_labels", conn=conn)
        total_count += len(df)
        del df
        gc.collect()
    print(f"Total count of sociopolitical labels: {total_count}")


if __name__ == "__main__":
    main()
