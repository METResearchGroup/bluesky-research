from lib.db.service_constants import MAP_SERVICE_TO_METADATA

# Cache the schema context at module load
_SCHEMA_CONTEXT_CACHE = None


# TODO: refactor this (or hardcode) as it's pretty complicated to understand.
def _get_schema_context() -> str:
    """Extract table schemas and build a context string for LLM.

    Returns:
        A formatted string containing all available tables and their columns.
    """
    schema_lines = []
    schema_lines.append("Available Athena Tables and Columns:\n")

    for _, metadata in MAP_SERVICE_TO_METADATA.items():
        glue_table_name = metadata.get("glue_table_name", "")
        dtypes_map = metadata.get("dtypes_map", {})

        # Skip services without a glue table name
        if not glue_table_name:
            continue

        # Handle nested dtypes_map (like raw_sync with post/reply/etc.)
        if dtypes_map and isinstance(dtypes_map, dict):
            # Check if first value is a dict (nested structure)
            first_value = next(iter(dtypes_map.values()), None)
            if isinstance(first_value, dict):
                # Nested structure - create entries for each sub-table
                for sub_table, columns in dtypes_map.items():
                    table_name = (
                        f"{glue_table_name}_{sub_table}"
                        if sub_table != glue_table_name
                        else glue_table_name
                    )
                    # Prepend "archive_" to table name for analysis tables
                    table_name = f"archive_{table_name}"
                    column_list = ", ".join(
                        [f"{col} ({dtype})" for col, dtype in columns.items()]
                    )
                    schema_lines.append(f"Table: {table_name}")
                    schema_lines.append(f"  Columns: {column_list}\n")
            else:
                # Flat structure - single table
                # Prepend "archive_" to table name for analysis tables
                table_name = f"archive_{glue_table_name}"
                column_list = ", ".join(
                    [f"{col} ({dtype})" for col, dtype in dtypes_map.items()]
                )
                schema_lines.append(f"Table: {table_name}")
                schema_lines.append(f"  Columns: {column_list}\n")

    return "\n".join(schema_lines)


def _get_cached_schema_context() -> str:
    """Get cached schema context, building it if not already cached."""
    global _SCHEMA_CONTEXT_CACHE
    if _SCHEMA_CONTEXT_CACHE is None:
        _SCHEMA_CONTEXT_CACHE = _get_schema_context()
    return _SCHEMA_CONTEXT_CACHE


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
