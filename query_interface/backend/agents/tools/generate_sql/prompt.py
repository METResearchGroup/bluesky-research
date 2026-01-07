from query_interface.backend.services.schema_provider import get_cached_schema_context


def build_sql_generation_prompt(query: str, schema_context: str | None = None) -> str:
    """Build the prompt for generating SQL from a natural language query.

    Args:
        query: The natural language query to convert to SQL.
        schema_context: Optional schema context string. If None, will use cached context.

    Returns:
        The formatted prompt string.
    """
    if schema_context is None:
        schema_context = get_cached_schema_context()

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
