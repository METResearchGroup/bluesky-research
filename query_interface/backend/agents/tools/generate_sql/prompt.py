from functools import lru_cache

from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from query_interface.backend.constants import AVAILABLE_ATHENA_TABLES


ANALYSIS_TABLE_PREFIX = "archive_"


@lru_cache(maxsize=1)
def _get_schema_context() -> str:
    """Extract table schemas and build a context string for LLM.

    Returns:
        A formatted string containing all available tables and their columns.
    """
    schema_lines: list[str] = []
    schema_lines.append("Available Athena Tables and Columns:\n")

    for athena_table_name in AVAILABLE_ATHENA_TABLES:
        if "study_user_activity" in athena_table_name:
            schema_context_str = _get_schema_context_for_study_user_activity_tables(
                table_name=athena_table_name
            )
        else:
            schema_context_str = _get_schema_context_regular(
                table_name=athena_table_name
            )

        schema_lines.append(schema_context_str)

    full_schema_context: str = "\n".join(schema_lines)
    return full_schema_context


def _get_schema_context_regular(table_name: str) -> str:
    """Get the schema context for a regular table (not a special case)."""
    normalized_table_name: str = _normalize_athena_table_name(table_name)
    column_to_type_map: dict = MAP_SERVICE_TO_METADATA[normalized_table_name][
        "dtypes_map"
    ]
    column_to_type_str: str = _get_column_to_type_str(column_to_type_map)
    schema_context_str: str = f"""
        Table: {table_name}
        Columns: {column_to_type_str}
    """
    return schema_context_str


def _get_schema_context_for_study_user_activity_tables(table_name: str) -> str:
    """We treat study user activity tables as a special case of backwards compatibility.

    Basically, we didn't know what we wanted to do with them, so we tried
    to just dump the raw data into a table.

    There isn't a separate "study_user_activity" service in the
    service_constants.py, but the dtypes that we use for study_user_activity
    are defined in raw_sync (this is an oversight that should've really been
    handled better).
    """
    activity_type: str = table_name.split("_")[-1]  # post, like, etc.
    dtypes_map: dict = MAP_SERVICE_TO_METADATA["raw_sync"]["dtypes_map"]
    column_to_type_map: dict = dtypes_map[activity_type]
    column_to_type_str: str = _get_column_to_type_str(column_to_type_map)
    schema_context_str: str = f"""
        Table: {table_name}
        Columns: {column_to_type_str}
    """
    return schema_context_str


def _normalize_athena_table_name(table_name: str) -> str:
    """Normalize the Athena table name to get the service-level
    representation of that table name."""
    return table_name.removeprefix(ANALYSIS_TABLE_PREFIX)


def _get_column_to_type_str(column_to_type_map: dict) -> str:
    """Get the column-to-type string for a dtypes map."""
    column_to_type_str_list: list[str] = [
        f"{col} ({dtype})" for col, dtype in column_to_type_map.items()
    ]  # e.g., "uri (string), created_at (string)"
    return ", ".join(column_to_type_str_list)


def _get_cached_schema_context() -> str:
    """Get cached schema context, building it if not already cached."""
    return _get_schema_context()


def build_sql_generation_prompt(query: str, schema_context: str | None = None) -> str:
    """Build the prompt for generating SQL from a natural language query.

    Args:
        query: The natural language query to convert to SQL.
        schema_context: Optional schema context string. If None, will use cached context.

    Returns:
        The formatted prompt string.
    """
    if schema_context is None:
        schema_context = _get_cached_schema_context()

    return f"""You are a SQL expert. Convert the following natural language query into a valid Athena SQL SELECT query.

{schema_context}

Instructions:
- Generate only valid SQL SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
- Use proper Athena SQL syntax
- All queries will automatically be limited to 10 rows, so do NOT include LIMIT in your query
- Use appropriate WHERE clauses for filtering
- Use appropriate JOINs when querying multiple tables
- Use proper date/time functions for timestamp comparisons
- Column names and table names are case-sensitive
- Return only the SQL query, no markdown code blocks, no explanations

Natural language query: {query}

Generate the SQL query (SQL only, no explanations):"""
