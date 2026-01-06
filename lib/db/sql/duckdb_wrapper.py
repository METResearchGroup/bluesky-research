"""Helper functions for DuckDB."""

from typing import Optional, Any
import re

import duckdb
from duckdb import DuckDBPyConnection
import pandas as pd

from lib.log.logger import get_logger
from lib.telemetry.duckdb_metrics import DuckDBMetricsCollector

logger = get_logger(__file__)
metrics_collector = DuckDBMetricsCollector()

DEFAULT_TABLE_NAME = "parquet_data"

_DUCKDB_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_duckdb_identifier(identifier: str) -> str:
    """Safely quote a DuckDB identifier (view/table/column name)."""
    if not _DUCKDB_IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid DuckDB identifier: {identifier!r}")
    return f'"{identifier}"'


def _escape_sql_string_literal(value: str) -> str:
    """Escape a string for inclusion in a single-quoted SQL literal."""
    return value.replace("'", "''")


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
        files_str = ",".join([f"'{_escape_sql_string_literal(f)}'" for f in filepaths])

        # If specific columns are requested, only read those
        if tables:
            for table in tables:
                name = _quote_duckdb_identifier(table["name"])
                columns = table["columns"]
                cols_str = ", ".join(_quote_duckdb_identifier(c) for c in columns)
                query = (
                    f"CREATE VIEW {name} AS SELECT {cols_str} FROM read_parquet([{files_str}])"
                )  # nosec B608
                conn.execute(query)
        else:
            table_name = _quote_duckdb_identifier(table_name or DEFAULT_TABLE_NAME)
            query = (
                f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet([{files_str}])"
            )  # nosec B608
            conn.execute(query)

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
        if not query_metadata:
            query_metadata = {}
        if mode == "parquet":
            if not filepaths:
                logger.warning(
                    """
                    filepaths must be provided when mode='parquet.
                    There are scenarios where data is missing (e.g., in the "active"
                    path, there might not be any up-to-date records). In these cases,
                    it's assumed that the filepaths are not provided.
                    """
                )
                expected_columns = []
                for table in query_metadata.get("tables", []):
                    expected_columns.extend(table["columns"])
                return pd.DataFrame(columns=expected_columns)
            parquet_conn = self.create_parquet_connection(
                filepaths=filepaths,
                tables=query_metadata.get("tables", None),
            )
            df: pd.DataFrame = parquet_conn.execute(query).df()
        else:
            df: pd.DataFrame = self.conn.execute(query).df()
        return df

    def run_query_as_df(
        self,
        query: str,
        mode: str = "default",
        filepaths: Optional[list[str]] = None,
        query_metadata: Optional[dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Run a query and return the result as a pandas DataFrame."""
        # the decorator returns a tuple of the df and the metrics.
        df, metrics = self._run_query_as_df(
            query=query, mode=mode, filepaths=filepaths, query_metadata=query_metadata
        )
        self._export_query_metrics(metrics)
        return df

    # TODO: Implement this in the future.
    def _export_query_metrics(self, metrics: dict[str, Any]) -> None:
        """Export query metrics to the metrics collector."""
        logger.info(f"Query metrics: {metrics}")


def get_duckdb_instance(conn: Optional[DuckDBPyConnection] = None) -> DuckDB:
    """Get a DuckDB instance."""
    return DuckDB(conn)
