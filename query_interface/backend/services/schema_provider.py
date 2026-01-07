"""Schema provider for extracting table schemas from service constants."""

from lib.db.service_constants import MAP_SERVICE_TO_METADATA


def get_schema_context() -> str:
    """Extract table schemas and build a context string for LLM.

    Returns:
        A formatted string containing all available tables and their columns.
    """
    schema_lines = []
    schema_lines.append("Available Athena Tables and Columns:\n")

    for service_name, metadata in MAP_SERVICE_TO_METADATA.items():
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


# Cache the schema context at module load
_SCHEMA_CONTEXT_CACHE = None


def get_cached_schema_context() -> str:
    """Get cached schema context, building it if not already cached."""
    global _SCHEMA_CONTEXT_CACHE
    if _SCHEMA_CONTEXT_CACHE is None:
        _SCHEMA_CONTEXT_CACHE = get_schema_context()
    return _SCHEMA_CONTEXT_CACHE
