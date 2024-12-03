"""Create a DuckDB database and export the data to it.

Pieces of data to export:
- Posts that are actually used in the feeds:
    - Path: /projects/p32375/bluesky_research_data/analytics/consolidated_posts
    - All are parquet files, partitioned by "partition_date"
- The user session logs and the feeds that the users saw during the session:
    - Path: /projects/p32375/bluesky_research_data/analytics/mapped_user_session_logs.parquet

These all need to be exported to a DuckDB database for downstream analysis.
"""

import os
import glob

import duckdb
import pandas as pd


def load_posts_data(posts_dir: str) -> pd.DataFrame:
    """Load all post data from parquet files in the given directory."""
    # Get all parquet files in the directory
    parquet_files = glob.glob(os.path.join(posts_dir, "**/*.parquet"), recursive=True)
    if not parquet_files:
        raise ValueError(f"No parquet files found in {posts_dir}")

    # Read and concatenate all parquet files
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        # Add partition_date from the file path if not in the dataframe
        if "partition_date" not in df.columns:
            # Extract date from path, assuming format .../partition_date=YYYY-MM-DD/...
            partition_date = file.split("partition_date=")[-1].split("/")[0]
            df["partition_date"] = partition_date
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def load_session_logs(logs_path: str) -> pd.DataFrame:
    """Load user session logs from parquet file."""
    if not os.path.exists(logs_path):
        raise ValueError(f"Session logs file not found: {logs_path}")
    return pd.read_parquet(logs_path)


def create_duckdb_tables(posts_df: pd.DataFrame, logs_df: pd.DataFrame, db_path: str):
    """Create DuckDB database and load dataframes into tables."""
    # Connect to DuckDB
    conn = duckdb.connect(db_path)

    try:
        # Create and load posts table
        conn.execute("CREATE TABLE IF NOT EXISTS posts AS SELECT * FROM posts_df")

        # Create and load user session logs table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS user_session_logs AS SELECT * FROM logs_df"
        )

        # Create indexes for better query performance
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_date ON posts(partition_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_logs_date ON user_session_logs(partition_date)"
        )
    finally:
        conn.close()


def main():
    posts_dir = os.path.join(
        "/projects",
        "p32375",
        "bluesky_research_data",
        "analytics",
        "consolidated_posts",
    )
    logs_path = os.path.join(
        "/projects",
        "p32375",
        "bluesky_research_data",
        "analytics",
        "mapped_user_session_logs.parquet",
    )
    db_path = os.path.join(
        "/projects", "p32375", "bluesky_research_data", "analytics", "bluesky_data.db"
    )

    print("Loading posts data...")
    posts_df = load_posts_data(posts_dir)
    print(f"Loaded {len(posts_df)} posts")

    print("Loading session logs...")
    logs_df = load_session_logs(logs_path)
    print(f"Loaded {len(logs_df)} session logs")

    print("Creating DuckDB database and tables...")
    create_duckdb_tables(posts_df, logs_df, db_path)
    print(f"Successfully created database at {db_path}")


if __name__ == "__main__":
    main()
