"""Unit tests for retry logic."""

import pytest
from pydantic import BaseModel, ValidationError

import litellm.exceptions as litellm_exceptions  # type: ignore[import-untyped]

from ml_tooling.llm.retry import NON_RETRYABLE_EXCEPTIONS, _should_retry, retry_llm_completion


class TestShouldRetry:
    """Tests for _should_retry function."""

    @pytest.mark.parametrize(
        "exception_class",
        [
            litellm_exceptions.AuthenticationError,  # type: ignore[attr-defined]
            litellm_exceptions.InvalidRequestError,  # type: ignore[attr-defined]
            litellm_exceptions.PermissionDeniedError,  # type: ignore[attr-defined]
        ],
    )
    def test_should_retry_returns_false_for_non_retryable_exceptions(self, exception_class):
        """Test that non-retryable exceptions return False."""
        exception = exception_class("Test message")
        result = _should_retry(exception)
        assert result is False

    @pytest.mark.parametrize(
        "exception_class",
        [
            litellm_exceptions.RateLimitError,  # type: ignore[attr-defined]
            litellm_exceptions.Timeout,  # type: ignore[attr-defined]
            litellm_exceptions.ServiceUnavailableError,  # type: ignore[attr-defined]
            litellm_exceptions.APIError,  # type: ignore[attr-defined]
            ValueError,
            ValidationError,
            Exception,
            AttributeError,
        ],
    )
    def test_should_retry_returns_true_for_retryable_exceptions(self, exception_class):
        """Test that retryable exceptions return True."""
        if exception_class == ValidationError:
            # ValidationError needs special handling for instantiation
            exception = ValidationError.from_exception_data(
                "TestModel", [{"type": "value_error", "loc": ("field",), "msg": "Test"}]
            )
        else:
            exception = exception_class("Test message")
        result = _should_retry(exception)
        assert result is True


class TestRetryLlmCompletion:
    """Tests for retry_llm_completion decorator."""

    def test_retry_llm_completion_succeeds_on_first_attempt(self):
        """Test that decorated function succeeds on first attempt without retrying."""
        call_count = 0

        @retry_llm_completion(max_retries=3, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.parametrize(
        "exception_class",
        [
            litellm_exceptions.RateLimitError,  # type: ignore[attr-defined]
            litellm_exceptions.Timeout,  # type: ignore[attr-defined]
            litellm_exceptions.ServiceUnavailableError,  # type: ignore[attr-defined]
            litellm_exceptions.APIError,  # type: ignore[attr-defined]
            ValueError,
        ],
    )
    def test_retry_llm_completion_retries_then_succeeds(self, exception_class):
        """Test that decorated function retries on retryable errors and then succeeds."""
        call_count = 0

        @retry_llm_completion(max_retries=2, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:  # Fail twice, succeed on third attempt
                if exception_class == litellm_exceptions.APIError:  # type: ignore[attr-defined]
                    raise exception_class("Test error", status_code=500)
                else:
                    raise exception_class("Test error")
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 3

    def test_retry_llm_completion_retries_on_validation_error(self):
        """Test that ValidationError is retried (new behavior - retries on validation failures)."""
        call_count = 0

        @retry_llm_completion(max_retries=2, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                # Create a ValidationError
                raise ValidationError.from_exception_data(
                    "TestModel",
                    [{"type": "value_error", "loc": ("field",), "msg": "Invalid value"}],
                )
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 2

    @pytest.mark.parametrize(
        "exception_class",
        [
            litellm_exceptions.AuthenticationError,  # type: ignore[attr-defined]
            litellm_exceptions.InvalidRequestError,  # type: ignore[attr-defined]
            litellm_exceptions.PermissionDeniedError,  # type: ignore[attr-defined]
        ],
    )
    def test_retry_llm_completion_does_not_retry_on_non_retryable_errors(self, exception_class):
        """Test that decorated function does not retry on non-retryable errors."""
        call_count = 0

        @retry_llm_completion(max_retries=3, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise exception_class("Non-retryable error")

        with pytest.raises(exception_class, match="Non-retryable error"):
            test_function()
        assert call_count == 1  # Only initial attempt, no retries

    def test_retry_llm_completion_respects_max_retries(self):
        """Test that decorated function respects max_retries and eventually raises."""
        call_count = 0
        exception_instance = litellm_exceptions.RateLimitError("Rate limit exceeded")  # type: ignore[attr-defined]

        @retry_llm_completion(max_retries=1, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise exception_instance

        with pytest.raises(litellm_exceptions.RateLimitError, match="Rate limit exceeded"):  # type: ignore[attr-defined]
            test_function()
        assert call_count == 2  # 1 initial + 1 retry

    def test_retry_llm_completion_retries_on_value_error(self):
        """Test that ValueError is retried (new behavior - retries on missing content, etc.)."""
        call_count = 0

        @retry_llm_completion(max_retries=2, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Response content is None")
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 2
