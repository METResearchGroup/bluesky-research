"""Unit tests for LLMService."""

import pytest
from unittest.mock import MagicMock, patch
from pydantic import BaseModel, Field

from ml_tooling.llm.llm_service import LLMService


class SamplePydanticModel(BaseModel):
    """Test Pydantic model for testing."""

    value: str = Field(description="A test value")
    number: int = Field(description="A test number")


class TestLLMService:
    """Tests for LLMService."""

    @patch("ml_tooling.llm.llm_service.litellm.completion")
    def test_chat_completion_raises_exception_from_litellm(self, mock_litellm_completion):
        """Test that chat_completion re-raises exceptions from litellm.completion."""
        # Arrange
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        mock_litellm_completion.side_effect = Exception("API error")

        # Act & Assert
        with pytest.raises(Exception, match="API error"):
            service.chat_completion(messages=messages)

    @patch("ml_tooling.llm.llm_service.litellm.completion")
    def test_chat_completion_raises_exception_with_response_format(self, mock_litellm_completion):
        """Test that chat_completion re-raises exceptions when using response_format."""
        # Arrange
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        mock_litellm_completion.side_effect = ValueError("Invalid schema")

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid schema"):
            service.chat_completion(
                messages=messages,
                response_format=SamplePydanticModel,
            )

    @patch("ml_tooling.llm.llm_service.litellm.completion")
    def test_chat_completion_returns_model_response(self, mock_litellm_completion):
        """Test that chat_completion returns ModelResponse successfully."""
        # Arrange
        from litellm import ModelResponse, Choices, Message

        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        mock_response = ModelResponse(
            id="test-id",
            choices=[Choices(message=Message(role="assistant", content="test response"))],
        )
        mock_litellm_completion.return_value = mock_response

        # Act
        result = service.chat_completion(messages=messages)

        # Assert
        assert isinstance(result, ModelResponse)
        assert result.id == "test-id"
        mock_litellm_completion.assert_called_once()

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


class TestFixSchemaForOpenai:
    """Tests for _fix_schema_for_openai method."""

    def test_adds_additional_properties_to_single_object(self):
        """Test that additionalProperties: false is added to a single object type."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert result["additionalProperties"] is False
        assert result["type"] == "object"
        assert "properties" in result

    def test_adds_additional_properties_to_nested_objects(self):
        """Test that additionalProperties: false is added to nested objects."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "profile": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                        },
                    },
                },
            },
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert result["additionalProperties"] is False
        assert result["properties"]["user"]["additionalProperties"] is False
        assert (
            result["properties"]["user"]["properties"]["profile"]["additionalProperties"]
            is False
        )

    def test_adds_additional_properties_to_objects_in_lists(self):
        """Test that additionalProperties: false is added to objects within lists."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                    },
                },
            },
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert result["additionalProperties"] is False
        assert result["properties"]["items"]["items"]["additionalProperties"] is False

    def test_does_not_modify_non_object_types(self):
        """Test that non-object types are not modified."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "string",
            "enum": ["option1", "option2"],
            "description": "A string field",
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert "additionalProperties" not in result
        assert result["type"] == "string"
        assert result["enum"] == ["option1", "option2"]

    def test_handles_empty_schema(self):
        """Test that empty schema is handled correctly."""
        # Arrange
        service = LLMService()
        schema = {}

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert result == {}
        assert "additionalProperties" not in result

    def test_handles_schema_with_no_objects(self):
        """Test that schema with no object types returns unchanged."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "string",
            "properties": {
                "field1": {"type": "string"},
                "field2": {"type": "integer"},
            },
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert "additionalProperties" not in result
        assert result["type"] == "string"

    def test_handles_complex_nested_structure(self):
        """Test that complex nested structures with objects in various locations are handled."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "properties": {
                        "array_of_objects": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "deep_nested": {
                                        "type": "object",
                                        "properties": {"value": {"type": "string"}},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert result["additionalProperties"] is False
        assert result["properties"]["nested"]["additionalProperties"] is False
        assert (
            result["properties"]["nested"]["properties"]["array_of_objects"]["items"][
                "additionalProperties"
            ]
            is False
        )
        assert (
            result["properties"]["nested"]["properties"]["array_of_objects"]["items"][
                "properties"
            ]["deep_nested"]["additionalProperties"]
            is False
        )

    def test_preserves_existing_additional_properties(self):
        """Test that existing additionalProperties values are overwritten to False."""
        # Arrange
        service = LLMService()
        schema = {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "nested": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
            },
        }

        # Act
        result = service._fix_schema_for_openai(schema)

        # Assert
        assert result["additionalProperties"] is False
        assert result["properties"]["nested"]["additionalProperties"] is False

    def test_does_not_modify_original_schema(self):
        """Test that the original schema is not modified (deep copy)."""
        # Arrange
        service = LLMService()
        original_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "nested": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                },
            },
        }
        # Store original state
        original_has_additional_props = "additionalProperties" in original_schema
        original_nested_has_additional_props = (
            "additionalProperties" in original_schema["properties"]["nested"]
        )

        # Act
        result = service._fix_schema_for_openai(original_schema)

        # Assert
        # Result should have additionalProperties
        assert result["additionalProperties"] is False
        assert result["properties"]["nested"]["additionalProperties"] is False
        # Original should remain unchanged
        assert ("additionalProperties" in original_schema) == original_has_additional_props
        assert (
            "additionalProperties" in original_schema["properties"]["nested"]
        ) == original_nested_has_additional_props
        # Verify structure is preserved
        assert result["type"] == original_schema["type"]
        assert result["properties"]["name"] == original_schema["properties"]["name"]
