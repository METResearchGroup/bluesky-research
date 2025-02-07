def normalize_sql(sql: str) -> str:
    """Normalize SQL string by removing extra whitespace and newlines.

    Args:
        sql (str): SQL query string to normalize

    Returns:
        str: Normalized SQL string
    """
    # Split into lines and strip each line
    lines = [line.strip() for line in sql.split("\n")]
    # Join lines with single space, removing empty lines
    sql = " ".join(line for line in lines if line)

    # Normalize spaces around operators and punctuation
    sql = sql.replace(" ,", ",")
    sql = sql.replace(", ", ",")
    sql = sql.replace(" =", "=")
    sql = sql.replace("= ", "=")

    return sql
