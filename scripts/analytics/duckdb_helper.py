import os

import duckdb
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, "bluesky_data.db")
conn = duckdb.connect(db_path)


def get_table_counts(table: str) -> int:
    """Get row counts for each table in the database."""
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return result
    except Exception as e:
        print(f"Error getting count for {table}: {e}")


def query_table_as_df(query: str) -> pd.DataFrame:
    """Run a query on DuckDB and return results as a pandas DataFrame.

    Args:
        query: SQL query string to execute

    Returns:
        pandas DataFrame containing query results
    """
    try:
        print(f"Executing query: {query}")
        return conn.execute(query).df()
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()


def get_available_tables():
    """Get all tables in the database."""
    tables_query = (
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    )
    available_tables = query_table_as_df(tables_query)
    print("\nAvailable tables in database:")
    for table in available_tables["table_name"]:
        print(f"- {table}")
    print()


def get_pilot_users_to_filter() -> pd.DataFrame:
    """Get list of pilot users to filter out from analysis."""
    query = """
        SELECT bluesky_handle, bluesky_user_did
        FROM study_users 
        WHERE is_study_user = false 
        OR bluesky_handle IN (
            'markptorres.bsky.social',
            'testblueskyaccount.bsky.social', 
            'testblueskyuserv2.bsky.social',
            'mindtechnologylab.bsky.social'
        )
    """
    return query_table_as_df(query)
