"""Query executor that wraps Athena and enforces LIMIT 10 on all queries."""

import re
from typing import Optional

import pandas as pd

from lib.aws.athena import Athena


def enforce_limit_10(sql: str) -> str:
    """Enforce LIMIT 10 on a SQL query.

    Args:
        sql: The SQL query string.

    Returns:
        SQL query with LIMIT 10 enforced.
    """
    sql = sql.strip()
    
    # Remove trailing semicolon if present
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    
    # Case-insensitive regex to find LIMIT clause
    # Matches: LIMIT 123, LIMIT 123; (with semicolon), etc.
    limit_pattern = r'\bLIMIT\s+\d+\b'
    
    if re.search(limit_pattern, sql, re.IGNORECASE):
        # Replace existing LIMIT with LIMIT 10
        sql = re.sub(limit_pattern, 'LIMIT 10', sql, flags=re.IGNORECASE)
    else:
        # Append LIMIT 10
        sql = f"{sql} LIMIT 10"
    
    return sql


def execute_query(sql: str, dtypes_map: Optional[dict] = None) -> pd.DataFrame:
    """Execute a SQL query against Athena and return results as DataFrame.

    Automatically enforces LIMIT 10 on all queries.

    Args:
        sql: The SQL query to execute.
        dtypes_map: Optional dictionary mapping column names to pandas dtypes.

    Returns:
        Pandas DataFrame with query results (max 10 rows).

    Raises:
        Exception: If the query execution fails.
    """
    # Enforce LIMIT 10
    sql_with_limit = enforce_limit_10(sql)
    
    athena = Athena()
    
    try:
        df = athena.query_results_as_df(
            query=sql_with_limit,
            dtypes_map=dtypes_map,
        )
        return df
    except Exception as e:
        raise Exception(f"Athena query execution failed: {str(e)}")

