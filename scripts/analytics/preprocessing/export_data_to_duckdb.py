"""Create a DuckDB database and export the data to it.

Pieces of data to export:
- Posts that are actually used in the feeds:
    - Path: /projects/p32375/bluesky_research_data/analytics/consolidated_posts
    - All are parquet files, partitioned by "partition_date"
- Post pool:
    - Path: /projects/p32375/bluesky_research_data/consolidated_enriched_post_records/cache
    - All are parquet files, partitioned by "partition_date"
- The user session logs and the feeds that the users saw during the session:
    - Path: /projects/p32375/bluesky_research_data/analytics/mapped_user_session_logs.parquet

These all need to be exported to a DuckDB database for downstream analysis.
"""

import pandas as pd

from scripts.analytics.duckdb_helper import write_df_to_duckdb, add_index_to_table
from scripts.analytics.helper import load_raw_parquet_data_as_df


logs_path = (
    "/projects/p32375/bluesky_research_data/analytics/mapped_user_session_logs.parquet"
)
posts_used_in_feeds_dir = "/projects/p32375/bluesky_research_data/analytics/consolidated_posts"  # all posts actually used in feeds.
post_pool_dir = "/projects/p32375/bluesky_research_data/consolidated_enriched_post_records/cache"  # all posts available to be used in the feed


def export_post_pool(drop_table: bool = False):
    """Exports the pool of all posts that were available to be used in
    feeds to a DuckDB table."""
    table_name = "raw_post_pool"
    df = load_raw_parquet_data_as_df(post_pool_dir)
    # Deduplicate posts by uri, keeping the most recent version
    df = df.sort_values("consolidation_timestamp", ascending=False).drop_duplicates(
        subset=["uri"], keep="first"
    )
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    add_index_to_table(table_name=table_name, column="partition_date")
    print(f"Exported {df.shape[0]:,} posts to DuckDB table '{table_name}'")


def export_posts_used_in_feeds(drop_table: bool = False):
    """Exports the posts used in the feeds to a DuckDB table."""
    table_name = "raw_posts_used_in_feeds"
    df = load_raw_parquet_data_as_df(posts_used_in_feeds_dir)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    add_index_to_table(table_name=table_name, column="partition_date")
    print(
        f"Exported {df.shape[0]:,} posts used in feeds to DuckDB table '{table_name}'"
    )


def export_user_session_logs(drop_table: bool = False):
    """Exports the user session logs to a DuckDB table."""
    table_name = "raw_user_session_logs"
    df = pd.read_parquet(logs_path)
    write_df_to_duckdb(df=df, table_name=table_name, drop_table=drop_table)
    print(f"Exported {df.shape[0]:,} user session logs to DuckDB table '{table_name}'")


def main():
    export_posts_used_in_feeds(drop_table=True)
    export_user_session_logs(drop_table=True)


if __name__ == "__main__":
    main()
