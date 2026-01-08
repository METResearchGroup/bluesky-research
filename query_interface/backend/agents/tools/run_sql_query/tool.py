"""Tool to execute SQL queries against Athena."""

import threading
from typing import Optional

import pandas as pd

from lib.aws.athena import Athena
from query_interface.backend.agents.tools.run_sql_query.exceptions import (
    SQLQueryExecutionError,
)

# Thread-safe singleton pattern
_athena_client: Athena | None = None
_athena_client_lock = threading.Lock()


def _get_athena_client() -> Athena:
    """Get thread-safe singleton Athena client instance."""
    global _athena_client
    if _athena_client is None:
        with _athena_client_lock:
            if _athena_client is None:
                _athena_client = Athena()
    return _athena_client


def run_sql_query(sql: str, dtypes_map: Optional[dict] = None) -> pd.DataFrame:
    """Execute a SQL query against Athena and return results as DataFrame.

    Note: This function assumes the SQL query has already been prepared
    (e.g., via prepare_sql_for_execution tool). It does NOT enforce LIMIT 10 - that
    should be done during preparation.

    Args:
        sql: The prepared SQL query to execute (should already have LIMIT 10).
        dtypes_map: Optional dictionary mapping column names to pandas dtypes.

    Returns:
        Pandas DataFrame with query results (max 10 rows).

    Raises:
        SQLQueryExecutionError: If the query execution fails.
    """
    try:
        athena = _get_athena_client()

        df = athena.query_results_as_df(
            query=sql,
            dtypes_map=dtypes_map,
        )
        return df
    except Exception as e:
        raise SQLQueryExecutionError(
            f"Failed to execute SQL query '{sql}': {str(e)}"
        ) from e
