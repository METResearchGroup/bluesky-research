"""Helper functions for analytics scripts."""

import glob
import os

import pandas as pd

start_partition_date = "2024-09-29"
end_partition_date = "2024-12-01"


def load_raw_parquet_data_as_df(base_path: str) -> pd.DataFrame:
    """Load all raw data from parquet files in the raw data directory."""
    # Get all parquet files in the directory
    parquet_files = glob.glob(os.path.join(base_path, "**/*.parquet"), recursive=True)
    if not parquet_files:
        print(f"No parquet files found in {base_path}")
        return None

    # Read and concatenate all parquet files
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        # Extract date from path, assuming format .../partition_date=YYYY-MM-DD/...
        partition_date = file.split("partition_date=")[-1].split("/")[0]
        if not (start_partition_date <= partition_date <= end_partition_date):
            continue
        # Add partition_date from the file path if not in the dataframe
        if "partition_date" not in df.columns:
            df["partition_date"] = partition_date
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return None
