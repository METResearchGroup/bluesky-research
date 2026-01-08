"""Unit tests for LLMService."""

import pytest
from unittest.mock import MagicMock, patch
from pydantic import BaseModel, Field

from query_interface.backend.services.llm_service import LLMService


class SamplePydanticModel(BaseModel):
    """Test Pydantic model for testing."""

    value: str = Field(description="A test value")
    number: int = Field(description="A test number")


class TestLLMService:
    """Tests for LLMService."""

    def test_structured_completion_returns_parsed_model(self):
        """Test that structured_completion returns a parsed Pydantic model."""
        # Arrange
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        
        # Mock the chat_completion method to return a response with content
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"value": "test", "number": 42}'))
        ]
        
        service.chat_completion = MagicMock(return_value=mock_response)

        # Act
        result = service.structured_completion(
            messages=messages,
            response_model=SamplePydanticModel,
            max_tokens=100,
            temperature=0.7,
        )

        # Assert
        assert isinstance(result, SamplePydanticModel)
        assert result.value == "test"
        assert result.number == 42
        service.chat_completion.assert_called_once()

    def test_structured_completion_raises_value_error_when_content_is_none(self):
        """Test that structured_completion raises ValueError when content is None."""
        # Arrange
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        
        # Mock the chat_completion method to return a response with None content
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content=None))]
        
        service.chat_completion = MagicMock(return_value=mock_response)

        # Act & Assert
        with pytest.raises(ValueError, match="Response content is None"):
            service.structured_completion(
                messages=messages,
                response_model=SamplePydanticModel,
            )

    def test_structured_completion_raises_validation_error_for_invalid_json(self):
        """Test that structured_completion raises ValidationError for invalid JSON."""
        # Arrange
        from pydantic import ValidationError

        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        
        # Mock the chat_completion method to return invalid JSON
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"invalid": "json"}'))
        ]
        
        service.chat_completion = MagicMock(return_value=mock_response)

        # Act & Assert
        with pytest.raises(ValidationError):
            service.structured_completion(
                messages=messages,
                response_model=SamplePydanticModel,
            )

    def test_structured_completion_passes_kwargs_to_chat_completion(self):
        """Test that structured_completion passes kwargs correctly to chat_completion."""
        # Arrange
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        
        # Mock the chat_completion method
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"value": "test", "number": 42}'))
        ]
        
        service.chat_completion = MagicMock(return_value=mock_response)

        # Act
        service.structured_completion(
            messages=messages,
            response_model=SamplePydanticModel,
            max_tokens=200,
            temperature=0.5,
            top_p=0.9,
        )

        # Assert
        call_kwargs = service.chat_completion.call_args[1]
        assert call_kwargs["max_tokens"] == 200
        assert call_kwargs["temperature"] == 0.5
        assert call_kwargs["top_p"] == 0.9
        assert call_kwargs["response_format"] == SamplePydanticModel

    def test_structured_completion_uses_custom_model_when_provided(self):
        """Test that structured_completion uses custom model when provided."""
        # Arrange
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        
        # Mock the chat_completion method
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"value": "test", "number": 42}'))
        ]
        
        service.chat_completion = MagicMock(return_value=mock_response)

        # Act
        service.structured_completion(
            messages=messages,
            response_model=SamplePydanticModel,
            model="gpt-4",
        )

        # Assert
        call_kwargs = service.chat_completion.call_args[1]
        assert call_kwargs["model"] == "gpt-4"
