"""Unit tests for retry logic."""

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, Field, ValidationError

import litellm.exceptions as litellm_exceptions  # type: ignore[import-untyped]

from ml_tooling.llm.retry import NON_RETRYABLE_EXCEPTIONS, _should_retry, retry_llm_completion


class _TestModel(BaseModel):
    """Simple test model for ValidationError testing."""
    value: str = Field(description="A test value")


class TestShouldRetry:
    """Tests for _should_retry function."""

    @pytest.mark.parametrize(
        "exception_class,exception_kwargs",
        [
            (litellm_exceptions.AuthenticationError, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.InvalidRequestError, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.PermissionDeniedError, {"message": "Test", "llm_provider": "test", "model": "test", "response": MagicMock()}),  # type: ignore[attr-defined]
        ],
    )
    def test_should_retry_returns_false_for_non_retryable_exceptions(self, exception_class, exception_kwargs):
        """Test that non-retryable exceptions return False."""
        exception = exception_class(**exception_kwargs)  # type: ignore[misc]
        result = _should_retry(exception)
        assert result is False

    @pytest.mark.parametrize(
        "exception_class,exception_kwargs",
        [
            (litellm_exceptions.RateLimitError, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.Timeout, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.ServiceUnavailableError, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.APIError, {"message": "Test", "llm_provider": "test", "model": "test", "status_code": 500}),  # type: ignore[attr-defined]
            (ValueError, {}),
            (ValidationError, {}),
            (Exception, {}),
            (AttributeError, {}),
        ],
    )
    def test_should_retry_returns_true_for_retryable_exceptions(self, exception_class, exception_kwargs):
        """Test that retryable exceptions return True."""
        if exception_class == ValidationError:
            # ValidationError needs special handling - create by validating invalid data
            try:
                _TestModel.model_validate({})  # This will raise ValidationError
            except ValidationError as e:
                exception = e
        elif exception_kwargs:
            exception = exception_class(**exception_kwargs)  # type: ignore[misc]
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
        "exception_class,exception_kwargs",
        [
            (litellm_exceptions.RateLimitError, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.Timeout, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.ServiceUnavailableError, {"message": "Test", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.APIError, {"message": "Test", "llm_provider": "test", "model": "test", "status_code": 500}),  # type: ignore[attr-defined]
            (ValueError, {}),
        ],
    )
    def test_retry_llm_completion_retries_then_succeeds(self, exception_class, exception_kwargs):
        """Test that decorated function retries on retryable errors and then succeeds."""
        call_count = 0

        @retry_llm_completion(max_retries=2, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:  # Fail twice, succeed on third attempt
                if exception_kwargs:
                    raise exception_class(**exception_kwargs)  # type: ignore[misc]
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
                # Create a ValidationError by trying to validate invalid data
                try:
                    _TestModel.model_validate({})
                except ValidationError as e:
                    raise e
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 2

    @pytest.mark.parametrize(
        "exception_class,exception_kwargs",
        [
            (litellm_exceptions.AuthenticationError, {"message": "Non-retryable error", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.InvalidRequestError, {"message": "Non-retryable error", "llm_provider": "test", "model": "test"}),  # type: ignore[attr-defined]
            (litellm_exceptions.PermissionDeniedError, {"message": "Non-retryable error", "llm_provider": "test", "model": "test", "response": MagicMock()}),  # type: ignore[attr-defined]
        ],
    )
    def test_retry_llm_completion_does_not_retry_on_non_retryable_errors(self, exception_class, exception_kwargs):
        """Test that decorated function does not retry on non-retryable errors."""
        call_count = 0

        @retry_llm_completion(max_retries=3, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise exception_class(**exception_kwargs)  # type: ignore[misc]

        with pytest.raises(exception_class):
            test_function()
        assert call_count == 1  # Only initial attempt, no retries

    def test_retry_llm_completion_respects_max_retries(self):
        """Test that decorated function respects max_retries and eventually raises."""
        call_count = 0
        exception_instance = litellm_exceptions.RateLimitError(  # type: ignore[attr-defined]
            message="Rate limit exceeded", llm_provider="test", model="test"
        )

        @retry_llm_completion(max_retries=1, initial_delay=0.01, max_delay=0.1)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise exception_instance

        with pytest.raises(litellm_exceptions.RateLimitError):  # type: ignore[attr-defined]
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
