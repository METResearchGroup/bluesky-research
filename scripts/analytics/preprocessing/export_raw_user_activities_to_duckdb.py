"""Exports the raw user activities to DuckDB.

These are currently stored as parquet files in
/projects/p32375/bluesky_research_data/study_user_activity/create
- like
- like_on_user_post
- post
- reply_to_user_post (needs preprocessing, so skipping in this iteration).

For the social network data, these are in
/projects/p32375/bluesky_research_data/scraped_user_social_network/cache
"""

import glob
import os

import duckdb
import pandas as pd

from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel


start_partition_date = "2024-09-29"
end_partition_date = "2024-12-01"

db_path = "/projects/p32375/bluesky_research_data/analytics/bluesky_data.db"
study_user_activity_path = (
    "/projects/p32375/bluesky_research_data/study_user_activity/create"
)
like_path = os.path.join(study_user_activity_path, "like", "cache")
like_on_user_post_path = os.path.join(
    study_user_activity_path, "like_on_user_post", "cache"
)
post_path = os.path.join(study_user_activity_path, "post", "cache")
social_network_data_path = (
    "/projects/p32375/bluesky_research_data/scraped_user_social_network/cache"
)

conn = duckdb.connect(db_path)  # if running on cluster
# conn = duckdb.connect('../bluesky_data.db') # if running locally


def write_to_duckdb(df: pd.DataFrame, table_name: str, drop_table: bool = False):
    try:
        if drop_table:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_bluesky_user_did ON {table_name}(bluesky_user_did)"
        )
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")


def export_study_users(drop_table: bool = False):
    """Fetches study users from DynamoDB and then writes to DuckDB table."""
    table_name = "study_users"
    users: list[UserToBlueskyProfileModel] = get_all_users()
    user_dicts = [user.dict() for user in users]
    df = pd.DataFrame(user_dicts)
    write_to_duckdb(df, table_name, drop_table=drop_table)
    print(f"Exported {len(users)} study users to DuckDB table '{table_name}'")


def load_raw_data(base_path: str) -> pd.DataFrame:
    """Load all raw data from parquet files in the raw data directory."""
    # Get all parquet files in the directory
    parquet_files = glob.glob(os.path.join(base_path, "**/*.parquet"), recursive=True)
    if not parquet_files:
        raise ValueError(f"No parquet files found in {base_path}")

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

    return pd.concat(dfs, ignore_index=True)


def export_likes():
    """Load raw like activity data from parquet files and write to DuckDB table 'raw_likes'.

    Reads parquet files containing like activity data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_likes"
    df = load_raw_data(like_path)
    write_to_duckdb(df, table_name)


def export_like_on_user_post():
    """Load raw like-on-user-post activity data from parquet files and write to DuckDB table 'raw_like_on_user_post'.

    Reads parquet files containing like-on-user-post activity data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_like_on_user_post"
    df = load_raw_data(like_on_user_post_path)
    write_to_duckdb(df, table_name)


def export_user_posts():
    """Load raw user post data from parquet files and write to DuckDB table 'raw_user_posts'.

    Reads parquet files containing user post data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_user_posts"
    df = load_raw_data(post_path)
    write_to_duckdb(df, table_name)


def export_social_network_data():
    """Load raw social network data from parquet files and write to DuckDB table 'raw_social_network_data'.

    Reads parquet files containing social network data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_social_network_data"
    df = load_raw_data(social_network_data_path)
    write_to_duckdb(df, table_name)


def main():
    export_study_users(drop_table=True)
    export_likes()
    export_like_on_user_post()
    export_user_posts()
    export_social_network_data()


if __name__ == "__main__":
    main()
