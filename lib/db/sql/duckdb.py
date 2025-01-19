"""Helper functions for DuckDB."""
import pandas as pd
import duckdb

from lib.log.logger import get_logger

logger = get_logger(__file__)

class DuckDB:
    def __init__(self, conn: duckdb.Connection):
        self.conn = conn

    def run_query_as_df(self, query: str) -> pd.DataFrame:
        """Run a query and return the result as a pandas DataFrame."""
        return pd.read_sql_query(query, self.conn)
