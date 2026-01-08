def build_answerability_prompt(query: str) -> str:
    """Build the prompt for checking if a query can be answered with SQL.

    Args:
        query: The natural language query to evaluate.

    Returns:
        The formatted prompt string.
    """
    return f"""Can this natural language query be converted to a SQL SELECT query?

Query: {query}

Analyze whether this query can be answered by executing a SQL SELECT query against our database.
Consider:
- Does the query ask about data that could be in our database?
- Does it require real-time external information not available in our data?
- Can it be answered with database queries (SELECT statements only)?

Respond with your analysis of whether this query can be answered with SQL."""
