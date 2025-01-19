"""Helper functions for DuckDB."""

from typing import Optional, Dict, Any, Set

import duckdb
from duckdb.duckdb import DuckDBPyConnection
import pandas as pd
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, TokenList
from sqlparse.tokens import Keyword, DML, Wildcard

from lib.log.logger import get_logger
from lib.telemetry.duckdb_metrics import DuckDBMetricsCollector

logger = get_logger(__file__)
metrics_collector = DuckDBMetricsCollector()


class DuckDB:
    def __init__(self, conn: Optional[DuckDBPyConnection] = None):
        if not conn:
            logger.info("No connection provided, creating a new in-memory connection.")
        self.conn = conn or duckdb.connect(":memory:")

    def get_query_metadata(self, query: str) -> Dict[str, Any]:
        """Extract metadata from a SQL query without executing it.

        Args:
            query: SQL query string
        Returns:
            Dictionary containing query metadata (tables and columns referenced)
        """

        def extract_from_token(token: Token, tables: Set[str], columns: Set[str]):
            """Recursively extract table and column names from a token."""
            if isinstance(token, TokenList):
                # First, check if this is a SELECT statement and extract columns
                select_seen = False
                for t in token.tokens:
                    if t.ttype is DML and t.value.upper() == "SELECT":
                        select_seen = True
                        continue
                    if select_seen:
                        if t.ttype is Keyword and t.value.upper() in (
                            "FROM",
                            "WHERE",
                            "GROUP",
                            "ORDER",
                        ):
                            select_seen = False
                        elif not t.is_whitespace:
                            if isinstance(t, IdentifierList):
                                for identifier in t.get_identifiers():
                                    columns.add(identifier.get_real_name())
                            elif isinstance(t, Identifier):
                                columns.add(t.get_real_name())
                            elif t.ttype is Wildcard:
                                columns.add("*")
                            select_seen = False

                # Then look for table names in FROM clauses
                if any(
                    t.ttype is Keyword and t.value.upper() == "FROM"
                    for t in token.tokens
                ):
                    for i, t in enumerate(token.tokens):
                        if t.ttype is Keyword and t.value.upper() == "FROM":
                            next_tokens = [
                                tok
                                for tok in token.tokens[i + 1 :]
                                if not tok.is_whitespace
                            ]
                            if next_tokens:
                                next_token = next_tokens[0]
                                if isinstance(next_token, Identifier):
                                    tables.add(next_token.get_real_name())
                                elif isinstance(next_token, IdentifierList):
                                    for identifier in next_token.get_identifiers():
                                        tables.add(identifier.get_real_name())

                # Recursively process all tokens
                for t in token.tokens:
                    extract_from_token(t, tables, columns)

            elif token.ttype is Wildcard:
                columns.add("*")

        tables = set()
        columns = set()

        # Parse the query
        parsed = sqlparse.parse(query)[0]

        # Extract tables and columns
        extract_from_token(parsed, tables, columns)

        # Remove any None values that might have been added
        tables = {t for t in tables if t}
        columns = {c for c in columns if c}

        # Remove table names from columns set
        columns = columns - tables

        # If we have a wildcard, we can't determine specific columns
        if "*" in columns:
            columns = {"*"}

        return {
            "tables": list(tables),
            "columns": list(columns),
            "estimated_row_count": None,
            "estimated_size_bytes": None,
        }

    @staticmethod
    def create_parquet_connection(
        filepaths: list[str],
        columns: Optional[list[str]] = None,
        table_name: str = "parquet_data",
    ) -> DuckDBPyConnection:
        """Create a DuckDB connection for querying Parquet files.

        Args:
            filepaths: List of paths to Parquet files to query
            columns: Optional list of columns to read. If None, reads all columns.
        Returns:
            DuckDB connection configured for Parquet querying
        """
        conn = duckdb.connect(":memory:")
        files_str = ",".join([f"'{f}'" for f in filepaths])

        # If specific columns are requested, only read those
        if columns:
            cols_str = ", ".join(columns)
            conn.execute(
                f"CREATE VIEW {table_name} AS SELECT {cols_str} FROM read_parquet([{files_str}])"
            )
        else:
            conn.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet([{files_str}])"
            )

        return conn

    @metrics_collector.collect_query_metrics()
    def _run_query_as_df(
        self, query: str, mode: str = "default", filepaths: Optional[list[str]] = None
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
            query_metadata = self.get_query_metadata(query)
            parquet_conn = self.create_parquet_connection(
                filepaths=filepaths,
                columns=query_metadata["columns"],
                table_name=query_metadata["tables"][0],
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
        breakpoint()
        return df


def get_duckdb_instance(conn: Optional[DuckDBPyConnection] = None) -> DuckDB:
    """Get a DuckDB instance."""
    return DuckDB(conn)
