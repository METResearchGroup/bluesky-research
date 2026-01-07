"""SQL generator agent to convert natural language queries to SQL."""

from openai import OpenAI

from lib.helper import OPENAI_API_KEY
from query_interface.backend.services.schema_provider import get_cached_schema_context


def generate_sql(query: str, schema_context: str | None = None) -> str:
    """Generate SQL query from natural language query.

    Args:
        query: The natural language query to convert.
        schema_context: Optional schema context string. If None, will use cached context.

    Returns:
        Generated SQL query string.

    Raises:
        ValueError: If OPENAI_API_KEY is not set.
        Exception: If the API call fails.
    """
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is not set")

    if schema_context is None:
        schema_context = get_cached_schema_context()

    prompt = f"""You are a SQL expert. Convert the following natural language query into a valid Athena SQL SELECT query.

{schema_context}

Instructions:
- Generate only valid SQL SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
- Use proper Athena SQL syntax
- All queries will automatically be limited to 10 rows, so do NOT include LIMIT in your query
- Use appropriate WHERE clauses for filtering
- Use appropriate JOINs when querying multiple tables
- Use proper date/time functions for timestamp comparisons
- Column names and table names are case-sensitive

Natural language query: {query}

Generate the SQL query (SQL only, no explanations):"""

    client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3,
        )

        content = completion.choices[0].message.content
        if content is None:
            raise Exception("OpenAI API returned empty response")

        sql = content.strip()

        # Remove markdown code blocks if present
        if sql.startswith("```"):
            lines = sql.split("\n")
            # Remove first line (```sql or ```)
            lines = lines[1:]
            # Remove last line if it's ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            sql = "\n".join(lines).strip()

        return sql

    except Exception as e:
        raise Exception(f"SQL generator API call failed: {str(e)}")
