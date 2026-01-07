"""Tool to generate SQL from natural language queries."""

from query_interface.backend.config import get_config_value
from query_interface.backend.services.llm_service import get_llm_service
from query_interface.backend.agents.tools.generate_sql.models import (
    SQLGenerationResult,
)
from query_interface.backend.agents.tools.generate_sql.prompt import (
    build_sql_generation_prompt,
)
from query_interface.backend.agents.tools.generate_sql.exceptions import (
    SQLGenerationError,
)


# TODO: need to have a step to clean up the SQL query (either here
# or somewhere else).
def generate_sql(query: str, schema_context: str | None = None) -> SQLGenerationResult:
    """Generate SQL query from natural language query.

    Args:
        query: The natural language query to convert.
        schema_context: Optional schema context string. If None, will use cached context.

    Returns:
        A SQLGenerationResult object containing the generated SQL query.

    Raises:
        ValueError: If the LLM service cannot be initialized.
        Exception: If the API call fails.
    """
    llm_service = get_llm_service()
    prompt = build_sql_generation_prompt(query, schema_context)

    try:
        response = llm_service.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            response_format=SQLGenerationResult,
            max_tokens=get_config_value("llm", "generate_sql", "max_tokens"),
            temperature=get_config_value("llm", "generate_sql", "temperature"),
        )

        content: str = response.choices[0].message.content  # type: ignore
        result: SQLGenerationResult = SQLGenerationResult.model_validate_json(content)
        return result

    except Exception as e:
        raise SQLGenerationError(
            f"Failed to generate SQL for query '{query}': {str(e)}"
        ) from e
