"""Transforms raw DuckDB tables for downstream analysis."""

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


def main():
    dedupe_study_user_activity_tables()


if __name__ == "__main__":
    main()
