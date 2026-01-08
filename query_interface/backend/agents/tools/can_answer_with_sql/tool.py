"""Tool to determine if a natural language query can be answered with SQL."""

from query_interface.backend.config import get_config_value
from query_interface.backend.services.llm_service import get_llm_service
from query_interface.backend.agents.tools.can_answer_with_sql.models import (
    SQLAnswerabilityResult,
)
from query_interface.backend.agents.tools.can_answer_with_sql.prompt import (
    build_answerability_prompt,
)
from query_interface.backend.agents.tools.can_answer_with_sql.exceptions import (
    SQLAnswerabilityError,
)


def can_answer_with_sql(query: str) -> SQLAnswerabilityResult:
    """Determine if a natural language query can be answered with SQL.

    Args:
        query: The natural language query to evaluate.

    Returns:
        SQLAnswerabilityResult: A SQLAnswerabilityResult object containing the analysis.

    Raises:
        SQLAnswerabilityError: If SQL answerability check fails (e.g., LLM service errors, API failures).
    """
    llm_service = get_llm_service()
    prompt = build_answerability_prompt(query)

    try:
        result = llm_service.structured_completion(
            messages=[{"role": "user", "content": prompt}],
            response_model=SQLAnswerabilityResult,
            max_tokens=get_config_value("llm", "can_answer_with_sql", "max_tokens"),
            temperature=get_config_value("llm", "can_answer_with_sql", "temperature"),
        )
        return result

    except Exception as e:
        raise SQLAnswerabilityError(
            f"Failed to check SQL answerability for query '{query}': {str(e)}"
        ) from e
