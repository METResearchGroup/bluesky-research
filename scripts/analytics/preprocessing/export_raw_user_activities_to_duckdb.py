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

import os

import pandas as pd

from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from scripts.analytics.duckdb_helper import write_df_to_duckdb
from scripts.analytics.helper import load_raw_parquet_data_as_df

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


def export_study_users(drop_table: bool = False):
    """Fetches study users from DynamoDB and then writes to DuckDB table."""
    table_name = "study_users"
    users: list[UserToBlueskyProfileModel] = get_all_users()
    user_dicts = [user.dict() for user in users]
    df = pd.DataFrame(user_dicts)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    print(f"Exported {len(users)} study users to DuckDB table '{table_name}'")


def export_likes(drop_table: bool = False):
    """Load raw like activity data from parquet files and write to DuckDB table 'raw_likes'.

    Reads parquet files containing like activity data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_likes"
    df = load_raw_parquet_data_as_df(like_path)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    print(f"Exported {df.shape[0]:,} likes to DuckDB table '{table_name}'")


def export_like_on_user_post(drop_table: bool = False):
    """Load raw like-on-user-post activity data from parquet files and write to DuckDB table 'raw_like_on_user_post'.

    Reads parquet files containing like-on-user-post activity data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_like_on_user_post"
    df = load_raw_parquet_data_as_df(like_on_user_post_path)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    print(
        f"Exported {df.shape[0]:,} likes on user posts to DuckDB table '{table_name}'"
    )


def export_user_posts(drop_table: bool = False):
    """Load raw user post data from parquet files and write to DuckDB table 'raw_user_posts'.

    Reads parquet files containing user post data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_user_posts"
    df = load_raw_parquet_data_as_df(post_path)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    print(f"Exported {df.shape[0]:,} user posts to DuckDB table '{table_name}'")


def export_social_network_data(drop_table: bool = False):
    """Load raw social network data from parquet files and write to DuckDB table 'raw_social_network_data'.

    Reads parquet files containing social network data and exports them to a DuckDB table
    with a partition_date column and index.
    """
    table_name = "raw_social_network_data"
    df = load_raw_parquet_data_as_df(social_network_data_path)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    print(
        f"Exported {df.shape[0]:,} social network data to DuckDB table '{table_name}'"
    )


def main():
    """Exports all raw user activities to DuckDB."""
    export_study_users(drop_table=True)
    export_likes(drop_table=True)
    export_like_on_user_post(drop_table=True)
    export_user_posts(drop_table=True)
    export_social_network_data(drop_table=True)


if __name__ == "__main__":
    main()
