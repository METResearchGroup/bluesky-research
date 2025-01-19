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

        def extract_from_token(
            token: Token,
            tables: Set[str],
            columns: Set[str],
            parent_token: Optional[Token] = None,
        ):
            """Recursively extract table and column names from a token."""
            if isinstance(token, TokenList):
                # Track the current context
                is_where = any(
                    t.ttype is Keyword and t.value.upper() == "WHERE"
                    for t in token.tokens
                )
                is_join = any(
                    t.ttype is Keyword and t.value.upper() == "JOIN"
                    for t in token.tokens
                )
                is_on = any(
                    t.ttype is Keyword and t.value.upper() == "ON" for t in token.tokens
                )
                print(f"is_where: {is_where}, is_join: {is_join}, is_on: {is_on}")

                # Process SELECT columns
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
                                    if "." in identifier.value:
                                        _, col = identifier.value.split(".")
                                        columns.add(col.strip('`"'))
                                    else:
                                        columns.add(identifier.get_real_name())
                            elif isinstance(t, Identifier):
                                if "." in t.value:
                                    _, col = t.value.split(".")
                                    columns.add(col.strip('`"'))
                                else:
                                    columns.add(t.get_real_name())
                            elif t.ttype is Wildcard:
                                columns.add("*")
                            select_seen = False

                # Process FROM/JOIN tables
                from_seen = False
                for t in token.tokens:
                    if t.ttype is Keyword and t.value.upper() in ("FROM", "JOIN"):
                        from_seen = True
                        continue
                    if from_seen and not t.is_whitespace:
                        if isinstance(t, Identifier):
                            # Handle table aliases (e.g., "posts p")
                            real_name = t.get_real_name().split(" ")[0].strip('`"')
                            tables.add(real_name)
                        elif isinstance(t, IdentifierList):
                            for identifier in t.get_identifiers():
                                real_name = (
                                    identifier.get_real_name().split(" ")[0].strip('`"')
                                )
                                tables.add(real_name)
                        from_seen = False

                # Process WHERE/JOIN conditions
                for t in token.tokens:
                    if isinstance(t, Identifier):
                        if "." in t.value:
                            _, col = t.value.split(".")
                            columns.add(col.strip('`"'))
                        elif is_where or is_on:
                            columns.add(t.get_real_name().strip('`"'))

                # Recursively process all tokens
                for t in token.tokens:
                    extract_from_token(t, tables, columns, token)

            elif isinstance(token, Identifier):
                # Handle qualified column names (e.g., "p.author")
                if "." in token.value:
                    _, col = token.value.split(".")
                    columns.add(col.strip('`"'))
                elif parent_token and any(
                    t.ttype is Keyword and t.value.upper() in ("WHERE", "ON")
                    for t in parent_token.tokens
                ):
                    columns.add(token.get_real_name().strip('`"'))
            elif token.ttype is Wildcard:
                columns.add("*")

        tables = set()
        columns = set()

        # Parse the query
        parsed = sqlparse.parse(query)[0]

        # Extract tables and columns
        extract_from_token(parsed, tables, columns)

        # Remove any None values and clean up
        tables = {t.strip('`"') for t in tables if t}
        columns = {c.strip('`"') for c in columns if c}

        # Remove table names and aliases from columns
        columns = {c for c in columns if c not in tables and " " not in c}

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
        return df


def get_duckdb_instance(conn: Optional[DuckDBPyConnection] = None) -> DuckDB:
    """Get a DuckDB instance."""
    return DuckDB(conn)
