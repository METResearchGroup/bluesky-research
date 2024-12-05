import os

import duckdb
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))

cluster_db_path = "/projects/p32375/bluesky_research_data/analytics/bluesky_data.db"
local_db_path = os.path.join(current_dir, "bluesky_data.db")

conn = duckdb.connect(cluster_db_path)


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


def write_df_to_duckdb(df: pd.DataFrame, table_name: str, drop_table: bool = False):
    try:
        print(f"Writing {df.shape[0]:,} rows to DuckDB table '{table_name}'")
        if drop_table:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_bluesky_user_did ON {table_name}(bluesky_user_did)"
        )
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")


def add_index_to_table(table_name: str, column: str, index_name: str = None):
    """Add an index to a table on a specific column."""
    if index_name is None:
        index_name = f"idx_{table_name}_{column}"
    conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column})")
