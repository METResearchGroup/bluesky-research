"""Tool to enforce business logic constraints on user queries before processing."""

from query_interface.backend.config import get_config_value
from query_interface.backend.agents.tools.enforce_user_query_constraints.exceptions import (
    UserQueryConstraintError,
)

DEFAULT_MAX_LENGTH = 5000


def enforce_user_query_constraints(query: str | None) -> None:
    """Enforce business logic constraints on a user query.

    Args:
        query: The user query to validate.

    Raises:
        UserQueryConstraintError: If query exceeds maximum character limit or other constraint fails.
    """
    if query is None:
        raise UserQueryConstraintError("Query cannot be None")

    if not isinstance(query, str):
        raise UserQueryConstraintError(
            f"Query must be a string, got {type(query).__name__}"
        )

    if not query.strip():
        raise UserQueryConstraintError("Query cannot be an empty string")

    max_length: int = (
        get_config_value("query", "max_length")
        if get_config_value("query", "max_length") is not None
        else DEFAULT_MAX_LENGTH
    )

    if len(query) > max_length:
        raise UserQueryConstraintError(
            f"Query exceeds maximum length of {max_length} characters. Query length: {len(query)}"
        )
