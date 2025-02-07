def normalize_sql(sql: str) -> str:
    """Normalize SQL string by removing extra whitespace and newlines.

    Args:
        sql (str): SQL query string to normalize

    Returns:
        str: Normalized SQL string
    """
    import re

    # Replace all whitespace (including tabs and newlines) with a single space
    sql = re.sub(r"\s+", " ", sql)

    # Normalize spaces around operators and punctuation
    sql = re.sub(r"\s*,\s*", ",", sql)  # Remove spaces around commas
    sql = re.sub(r"\s*=\s*", "=", sql)  # Remove spaces around equals

    # Ensure single space after commas in column lists
    sql = re.sub(r",([^\s])", r", \1", sql)

    # Ensure single space around SQL keywords
    keywords = [
        "SELECT",
        "FROM",
        "WHERE",
        "JOIN",
        "ON",
        "AND",
        "OR",
        "ORDER BY",
        "GROUP BY",
        "HAVING",
    ]
    for keyword in keywords:
        # Replace keyword with spaces around it, handling word boundaries
        pattern = r"\b" + re.escape(keyword) + r"\b"
        sql = re.sub(pattern, " " + keyword + " ", sql)

    # Clean up any resulting multiple spaces
    sql = re.sub(r"\s+", " ", sql)

    # Remove leading/trailing whitespace
    sql = sql.strip()

    return sql
