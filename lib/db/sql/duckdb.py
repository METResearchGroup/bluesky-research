"""Helper functions for DuckDB."""

from typing import Optional, Any

import duckdb
from duckdb.duckdb import DuckDBPyConnection
import pandas as pd

from lib.log.logger import get_logger
from lib.telemetry.duckdb_metrics import DuckDBMetricsCollector

logger = get_logger(__file__)
metrics_collector = DuckDBMetricsCollector()

DEFAULT_TABLE_NAME = "parquet_data"


class DuckDB:
    def __init__(self, conn: Optional[DuckDBPyConnection] = None):
        if not conn:
            logger.info("No connection provided, creating a new in-memory connection.")
        self.conn = conn or duckdb.connect(":memory:")

    @staticmethod
    def create_parquet_connection(
        filepaths: list[str],
        tables: Optional[list[dict[str, Any]]] = None,
        table_name: Optional[str] = DEFAULT_TABLE_NAME,
    ) -> DuckDBPyConnection:
        """Create a DuckDB connection for querying Parquet files.

        Args:
            filepaths: List of paths to Parquet files to query
            tables: Optional list of tables to read. If None, reads all tables.
        Returns:
            DuckDB connection configured for Parquet querying
        """
        conn = duckdb.connect(":memory:")
        files_str = ",".join([f"'{f}'" for f in filepaths])

        # If specific columns are requested, only read those
        if tables:
            for table in tables:
                name = table["name"]
                columns = table["columns"]
                cols_str = ", ".join(columns)
                conn.execute(
                    f"CREATE VIEW {name} AS SELECT {cols_str} FROM read_parquet([{files_str}])"
                )
        else:
            conn.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet([{files_str}])"
            )

        return conn

    @metrics_collector.collect_query_metrics()
    def _run_query_as_df(
        self,
        query: str,
        mode: str = "default",
        filepaths: Optional[list[str]] = None,
        query_metadata: Optional[dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Run a query and return the result as a pandas DataFrame.

        Args:
            query: SQL query to execute
            mode: Query mode ('default' or 'parquet')
            filepaths: List of Parquet file paths when mode='parquet'
            columns: Optional list of columns to read. If None, reads all columns.
        """
        if mode == "parquet":
            if not filepaths:
                raise ValueError("filepaths must be provided when mode='parquet'")
            parquet_conn = self.create_parquet_connection(
                filepaths=filepaths,
                tables=query_metadata.get("tables", None),
            )
            df: pd.DataFrame = parquet_conn.execute(query).df()
        else:
            df: pd.DataFrame = self.conn.execute(query).df()
        return df

    def run_query_as_df(
        self, query: str, mode: str = "default", filepaths: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """Run a query and return the result as a pandas DataFrame."""
        # the decorator returns a tuple of the df and the metrics.
        df, metrics = self._run_query_as_df(query, mode, filepaths)
        return df


def get_duckdb_instance(conn: Optional[DuckDBPyConnection] = None) -> DuckDB:
    """Get a DuckDB instance."""
    return DuckDB(conn)
