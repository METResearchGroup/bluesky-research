"""Retry logic for LLM completions with validation."""

import logging
from typing import Callable, ParamSpec, TypeVar

import litellm.exceptions as litellm_exceptions  # type: ignore[import-untyped]
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

P = ParamSpec("P")
R = TypeVar("R")
logger = logging.getLogger(__name__)


# Exceptions that should NOT be retried (fail hard immediately)
NON_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    litellm_exceptions.AuthenticationError,  # type: ignore[attr-defined]
    litellm_exceptions.InvalidRequestError,  # type: ignore[attr-defined]
    litellm_exceptions.PermissionDeniedError,  # type: ignore[attr-defined]
)


def _should_retry(exception: BaseException) -> bool:
    """Determine if an exception should be retried.

    Returns False for non-retryable exceptions (they fail hard immediately).
    Returns True for all other exceptions (they will be retried).

    Args:
        exception: The exception that was raised

    Returns:
        True if the exception should be retried, False otherwise
    """
    # Fail hard on authentication/invalid request errors - retrying won't help
    if isinstance(exception, NON_RETRYABLE_EXCEPTIONS):
        return False

    # Retry everything else (HTTP errors, validation errors, missing content, etc.)
    return True


def retry_llm_completion(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for retrying LLM completions with exponential backoff.

    Retries on ANY exception EXCEPT non-retryable ones (authentication errors,
    invalid requests, etc.). This allows the model to "try again" if it returns
    invalid output, encounters transient HTTP errors, or has missing content.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)
        max_delay: Maximum delay cap in seconds (default: 60.0)

    Returns:
        Decorated function with retry logic

    Example:
        @retry_llm_completion(max_retries=3)
        def _complete_and_validate_structured(self, ...):
            response = self._chat_completion(...)
            return self.handle_completion_response(response, response_model)
    """
    return retry(
        stop=stop_after_attempt(max_retries + 1),  # +1 for initial attempt
        wait=wait_exponential_jitter(initial=initial_delay, max=max_delay),
        retry=retry_if_exception(_should_retry),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,  # Re-raise the exception after all retries exhausted
    )
