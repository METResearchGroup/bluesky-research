"""
Get summary statistics from DuckDB.
"""

from scripts.analytics.duckdb_helper import get_table_counts, query_table_as_df

# db_path = "/projects/p32375/bluesky_research_data/analytics/bluesky_data.db"


def get_number_of_posts_per_user():
    """Get number of posts per user from raw_user_posts table."""
    query = """
        WITH deduped_posts AS (
            SELECT DISTINCT ON (uri) * FROM raw_user_posts ORDER BY uri, synctimestamp DESC
        )
        SELECT 
            s.bluesky_handle,
            COUNT(*) as post_count
        FROM deduped_posts p
        INNER JOIN study_users s ON p.author_did = s.bluesky_user_did
        GROUP BY s.bluesky_handle
        ORDER BY post_count DESC
    """

    # First get total count
    count_query = """
        WITH deduped_posts AS (
            SELECT DISTINCT ON (uri) * FROM raw_user_posts ORDER BY uri, synctimestamp DESC
        )
        SELECT COUNT(*) 
        FROM deduped_posts p
        INNER JOIN study_users s ON p.author_did = s.bluesky_user_did
    """
    total_posts = query_table_as_df(count_query).iloc[0, 0]
    print(f"Total posts by study users: {total_posts:,}")

    # Get posts per user
    posts_per_user = query_table_as_df(query)
    print(posts_per_user.head(20))
    return posts_per_user


def get_totals_synced_from_firehose():
    """Get totals per table of data synced from firehose.

    This serves as the raw counts of data that we got from our sync,
    prior to any preprocessing.
    """
    tables = [
        "deduped_raw_user_posts",
        "deduped_raw_likes",
        "deduped_raw_like_on_user_post",
        "raw_social_network_data",
    ]
    for table in tables:
        if table == "raw_social_network_data":
            query = f"""
                SELECT COUNT(*) 
                FROM {table} 
                WHERE follow_handle IS NULL OR follower_handle IS NULL
            """
            total = query_table_as_df(query).iloc[0, 0]
        else:
            total = get_table_counts(table)
        print(f"{table}: {total:,}")


def main():
    print("Getting table counts from DuckDB...")
    # get_number_of_posts_per_user()
    get_totals_synced_from_firehose()


if __name__ == "__main__":
    main()
