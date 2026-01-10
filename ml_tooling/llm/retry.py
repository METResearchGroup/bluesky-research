"""Simple error classification and retry logic for LLM HTTP calls."""

import logging
from typing import Callable, ParamSpec, TypeVar

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

P = ParamSpec("P")
R = TypeVar("R")
logger = logging.getLogger(__name__)


class HTTPErrorClassifier:
    """Classifies exceptions to determine if they're HTTP-related and retryable.

    Only checks for HTTP/network errors from litellm. Non-HTTP errors
    (e.g., ValueError, AttributeError) are not retried.
    """

    # HTTP status codes that indicate transient HTTP errors
    RETRYABLE_HTTP_STATUS_CODES: set[int] = {
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    }

    @classmethod
    def _get_exception_type_name(cls, exception: BaseException) -> str:
        """Get the fully qualified exception type name."""
        return f"{type(exception).__module__}.{type(exception).__name__}"

    @classmethod
    def is_retryable_http_error(cls, exception: BaseException) -> bool:
        """Determine if an exception is a retryable HTTP error.

        Args:
            exception: The exception that was raised

        Returns:
            True if the error is a retryable HTTP error, False otherwise
        """
        exception_type_name = cls._get_exception_type_name(exception)

        # Check for non-retryable exception types (auth errors, invalid requests, etc.)
        # These are checked by type name to avoid import issues
        non_retryable_names = {
            "litellm.exceptions.AuthenticationError",
            "litellm.AuthenticationError",
            "litellm.exceptions.InvalidRequestError",
            "litellm.InvalidRequestError",
            "litellm.exceptions.PermissionDeniedError",
            "litellm.PermissionDeniedError",
        }
        if exception_type_name in non_retryable_names:
            return False

        # Check for retryable HTTP exception types by name
        retryable_names = {
            "litellm.exceptions.RateLimitError",
            "litellm.RateLimitError",
            "litellm.exceptions.Timeout",
            "litellm.Timeout",
            "litellm.exceptions.ServiceUnavailableError",
            "litellm.ServiceUnavailableError",
            "litellm.exceptions.APIError",
            "litellm.APIError",
        }

        # Check if it's a known retryable exception type
        if exception_type_name in retryable_names:
            # For APIError, verify it's a retryable status code
            if "APIError" in exception_type_name:
                status_code = getattr(exception, "status_code", None)
                if status_code and status_code not in cls.RETRYABLE_HTTP_STATUS_CODES:
                    return False
            return True

        # Check for HTTP status codes in exception attributes (fallback)
        status_code = getattr(exception, "status_code", None)
        if status_code and status_code in cls.RETRYABLE_HTTP_STATUS_CODES:
            return True

        # Default: don't retry (not an HTTP error we recognize)
        return False


def retry_llm_http_call(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for retrying litellm HTTP calls with exponential backoff.

    Only retries HTTP-related errors (rate limits, timeouts, server errors).
    Non-HTTP errors (ValueError, AttributeError, etc.) are not retried.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)
        max_delay: Maximum delay cap in seconds (default: 60.0)

    Returns:
        Decorated function with retry logic

    Example:
        @retry_llm_http_call(max_retries=3)
        def _llm_completion(self, ...):
            return litellm.completion(...)
    """
    return retry(
        stop=stop_after_attempt(max_retries + 1),  # +1 for initial attempt
        wait=wait_exponential(
            multiplier=initial_delay, min=initial_delay, max=max_delay
        ),
        retry=retry_if_exception(HTTPErrorClassifier.is_retryable_http_error),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,  # Re-raise the exception after all retries exhausted
    )
