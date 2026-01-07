"""Router agent to determine if a natural language query can be converted to SQL."""

from openai import OpenAI

from lib.helper import OPENAI_API_KEY


def can_convert_to_sql(query: str) -> tuple[bool, str]:
    """Determine if a natural language query can be converted to SQL.

    Args:
        query: The natural language query to evaluate.

    Returns:
        A tuple of (can_convert: bool, reason: str).
        If can_convert is True, reason may be empty or contain confirmation.
        If can_convert is False, reason explains why it cannot be converted.

    Raises:
        ValueError: If OPENAI_API_KEY is not set.
        Exception: If the API call fails.
    """
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is not set")

    prompt = f"""Can this natural language query be converted to a SQL SELECT query?

Query: {query}

Respond with only 'YES' or 'NO' followed by a brief reason on the same line.
Example responses:
- YES - This query can be converted to SQL
- NO - This query asks about external information not in the database
- NO - This query requires real-time data that is not available
"""

    client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.3,
        )

        response = completion.choices[0].message.content.strip()

        # Parse response
        response_upper = response.upper()
        if response_upper.startswith("YES"):
            reason = response[len("YES") :].strip()
            if reason.startswith("-"):
                reason = reason[1:].strip()
            return True, reason if reason else "Query can be converted to SQL"
        elif response_upper.startswith("NO"):
            reason = response[len("NO") :].strip()
            if reason.startswith("-"):
                reason = reason[1:].strip()
            return False, reason if reason else "Query cannot be converted to SQL"
        else:
            # Fallback: try to infer from response
            if "yes" in response_upper or "can" in response_upper:
                return True, response
            else:
                return False, response

    except Exception as e:
        raise Exception(f"Router agent API call failed: {str(e)}")
