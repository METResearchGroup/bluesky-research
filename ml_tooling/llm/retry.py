"""Simple error classification and retry logic for LLM HTTP calls."""

import logging
from typing import Callable, ParamSpec, TypeVar

import litellm
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

    # LiteLLM exception types that indicate HTTP/network errors
    RETRYABLE_HTTP_EXCEPTIONS: tuple[type[Exception], ...] = (
        litellm.RateLimitError,  # 429 Too Many Requests
        litellm.Timeout,  # Network/HTTP timeout
        litellm.ServiceUnavailableError,  # 503 Service Unavailable
        litellm.APIError,  # Generic API errors (filtered by status code)
    )

    # HTTP status codes that indicate transient HTTP errors
    RETRYABLE_HTTP_STATUS_CODES: set[int] = {
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    }

    # Non-retryable exceptions (auth errors, invalid requests, etc.)
    NON_RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
        litellm.AuthenticationError,
        litellm.InvalidRequestError,
        litellm.PermissionDeniedError,
    )

    @classmethod
    def is_retryable_http_error(cls, exception: Exception) -> bool:
        """Determine if an exception is a retryable HTTP error.

        Args:
            exception: The exception that was raised

        Returns:
            True if the error is a retryable HTTP error, False otherwise
        """
        # Check non-retryable exceptions first (most specific)
        if isinstance(exception, cls.NON_RETRYABLE_EXCEPTIONS):
            return False

        # Check for HTTP-related exception types
        if isinstance(exception, cls.RETRYABLE_HTTP_EXCEPTIONS):
            # For APIError, verify it's a retryable status code
            if isinstance(exception, litellm.APIError):
                status_code = getattr(exception, "status_code", None)
                if status_code and status_code not in cls.RETRYABLE_HTTP_STATUS_CODES:
                    return False
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
        def _make_http_call(self, ...):
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
