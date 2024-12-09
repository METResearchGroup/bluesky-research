"""Transforms raw DuckDB tables for downstream analysis."""

import json

import pandas as pd

from scripts.analytics.duckdb_helper import (
    conn,
    get_pilot_users_to_filter,
    query_table_as_df,
)


def dedupe_study_user_activity_tables():
    """Dedupe all the tables and save as deduped_<table_name>."""
    tables = ["raw_user_posts", "raw_likes", "raw_like_on_user_post"]
    pilot_users_to_filter: pd.DataFrame = get_pilot_users_to_filter()
    pilot_user_dids: list[str] = pilot_users_to_filter["bluesky_user_did"].tolist()
    pilot_user_did_str = ",".join([f"'{did}'" for did in pilot_user_dids])

    for table in tables:
        did_field = "author_did" if table == "raw_user_posts" else "author"
        query = f"""
            SELECT DISTINCT ON (uri) * 
            FROM {table} 
            WHERE {did_field} NOT IN ({pilot_user_did_str})
            ORDER BY uri, synctimestamp DESC
        """
        deduped_df = query_table_as_df(query)
        print(f"Deduped {table} table with {deduped_df.shape[0]:,} rows")
        conn.execute(
            f"CREATE OR REPLACE TABLE deduped_{table} AS SELECT * FROM deduped_df"
        )


def clean_user_session_logs():
    """Clean user session logs.

    In particular, consolidate the different serialization schemes of the feeds,
    some are JSON-strings and some are JSON strings of JSON strings (weird, yes).
    """
    query = "SELECT * FROM user_session_logs"
    df = query_table_as_df(query)
    feeds = df["feed"]

    def clean_feed(feed: str) -> str:
        cleaned_feed = json.loads(feed)
        if isinstance(cleaned_feed, str):
            cleaned_feed = json.loads(cleaned_feed)
        return cleaned_feed

    cleaned_feeds = feeds.apply(clean_feed)
    df["feed"] = cleaned_feeds.tolist()
    # extract the post URIs, since each post is currently a JSON in the feed.
    df["feed_uris"] = [[post["post"] for post in feed] for feed in cleaned_feeds]
    print(f"Creating cleaned_user_session_logs table with {df.shape[0]:,} rows")
    conn.execute(
        "CREATE OR REPLACE TABLE cleaned_user_session_logs AS SELECT * FROM df"
    )


# TODO: update this with the consolidated enriched posts
# (which should be called "posts_used_in_feeds").
def merge_user_session_logs_with_enriched_posts():
    """Merge user session logs with enriched posts."""
    query = """
        WITH post_json AS (
            SELECT 
                uri,
                json_object(
                    'uri', uri,
                    'author_did', author_did,
                    'text', text,
                    'synctimestamp', synctimestamp,
                    'indexedAt', indexedAt,
                    'createdAt', createdAt
                ) as post_data
            FROM deduped_raw_user_posts
        )
        SELECT 
            s.*,
            list(p.post_data) as enriched_feed
        FROM cleaned_user_session_logs s,
             post_json p
        WHERE p.uri = ANY(s.feed_uris)
        GROUP BY s.feed_uris, s.feed, s.session_id, s.user_id, s.timestamp
    """
    df = query_table_as_df(query)
    print(
        f"Creating merged_user_session_logs_with_enriched_posts table with {df.shape[0]:,} rows"
    )
    conn.execute(
        "CREATE OR REPLACE TABLE merged_user_session_logs_with_enriched_posts AS SELECT * FROM df"
    )


def main():
    # dedupe_study_user_activity_tables()
    # clean_user_session_logs()
    merge_user_session_logs_with_enriched_posts()


if __name__ == "__main__":
    main()
