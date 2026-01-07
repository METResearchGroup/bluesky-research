"""Tool to prepare SQL queries for execution."""

import re

from query_interface.backend.agents.tools.prepare_sql_for_execution.exceptions import (
    SQLPreparationForExecutionError,
)

# Default limit for SQL queries
DEFAULT_LIMIT = 10


def _clean_sql_formatting(sql: str) -> str:
    """Clean SQL formatting by removing markdown code blocks and trailing semicolons.

    Args:
        sql: The raw SQL query string (may include markdown formatting).

    Returns:
        Cleaned SQL query without markdown code blocks or trailing semicolons.
    """
    sql = sql.strip()

    # Remove markdown code blocks if present
    if sql.startswith("```"):
        lines = sql.split("\n")
        # Remove first line (```sql or ```)
        lines = lines[1:]
        # Remove last line if it's ```
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        sql = "\n".join(lines).strip()

    # Remove trailing semicolon if present
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    return sql


def _enforce_limit(sql: str, limit: int = DEFAULT_LIMIT) -> str:
    """Enforce a LIMIT clause on a SQL query.

    Args:
        sql: The SQL query string.
        limit: The limit value to enforce (default: DEFAULT_LIMIT).

    Returns:
        SQL query with the specified LIMIT enforced.
    """
    # Case-insensitive regex to find LIMIT clause
    # Matches: LIMIT 123, LIMIT 123; (with semicolon), etc.
    limit_pattern = r"\bLIMIT\s+\d+\b"

    if re.search(limit_pattern, sql, re.IGNORECASE):
        # Replace existing LIMIT with the specified limit
        sql = re.sub(limit_pattern, f"LIMIT {limit}", sql, flags=re.IGNORECASE)
    else:
        # Append the specified limit
        sql = f"{sql} LIMIT {limit}"

    return sql


def prepare_sql_for_execution(sql: str) -> str:
    """Prepare a SQL query for execution by cleaning and enforcing constraints.

    This function:
    - Removes markdown code blocks if present
    - Enforces LIMIT 10 on the query
    - Cleans up formatting (trailing semicolons, whitespace)

    Args:
        sql: The raw SQL query string (may include markdown formatting).

    Returns:
        Prepared SQL query ready for execution with LIMIT 10 enforced.

    Raises:
        SQLPreparationForExecutionError: If the SQL query cannot be prepared.
    """
    try:
        sql = _clean_sql_formatting(sql)
        sql = _enforce_limit(sql, DEFAULT_LIMIT)
        return sql
    except Exception as e:
        raise SQLPreparationForExecutionError(
            f"Failed to prepare SQL query for execution '{sql}': {str(e)}"
        ) from e
