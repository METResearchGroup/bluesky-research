"""Tool to determine if a natural language query can be answered with SQL."""

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
        A SQLAnswerabilityResult object containing the analysis.

    Raises:
        ValueError: If the LLM service cannot be initialized.
        Exception: If the API call fails.
    """
    llm_service = get_llm_service()
    prompt = build_answerability_prompt(query)

    try:
        response = llm_service.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            response_format=SQLAnswerabilityResult,
            max_tokens=150,
            temperature=0.3,
        )

        content: str = response.choices[0].message.content  # type: ignore
        result: SQLAnswerabilityResult = SQLAnswerabilityResult.model_validate_json(
            content
        )

        return result

    except Exception as e:
        raise SQLAnswerabilityError(
            f"Failed to check SQL answerability for query '{query}': {str(e)}"
        ) from e
