"""Tool to generate SQL from natural language queries."""

from query_interface.backend.config import get_config_value
from ml_tooling.llm.llm_service import get_llm_service
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
        SQLGenerationResult: A SQLGenerationResult object containing the generated SQL query.

    Raises:
        SQLGenerationError: If SQL generation fails (e.g., LLM service errors, API failures).
    """
    llm_service = get_llm_service()
    prompt = build_sql_generation_prompt(query, schema_context)

    try:
        result = llm_service.structured_completion(
            messages=[{"role": "user", "content": prompt}],
            response_model=SQLGenerationResult,
            model=get_config_value("llm", "default_model"),
            max_tokens=get_config_value("llm", "generate_sql", "max_tokens"),
            temperature=get_config_value("llm", "generate_sql", "temperature"),
        )
        return result

    except Exception as e:
        raise SQLGenerationError(
            f"Failed to generate SQL for query '{query}': {str(e)}"
        ) from e
