"""Unit tests for LLMService."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from ml_tooling.llm.llm_service import LLMService


class SamplePydanticModel(BaseModel):
    value: str = Field(description="A test value")
    number: int = Field(description="A test number")


class _DummyProvider:
    """Minimal provider stub to satisfy LLMService internals in unit tests."""

    _initialized = True

    def initialize(self, api_key=None) -> None:  # noqa: ANN001
        return None

    def supports_model(self, model_name: str) -> bool:  # noqa: ARG002
        return True

    def format_structured_output(self, response_model, model_config):  # noqa: ANN001,ARG002
        return {"type": "json_schema", "json_schema": {"schema": {}}}

    def prepare_completion_kwargs(
        self, *, model: str, messages: list[dict], response_format, model_config, **kwargs  # noqa: ANN001,ARG002
    ) -> dict:
        return {"model": model, "messages": messages, **kwargs}


class TestLLMService:
    @patch("ml_tooling.llm.llm_service.litellm.completion")
    def test__chat_completion_reraises_exceptions(self, mock_litellm_completion):
        """_chat_completion should re-raise exceptions from litellm.completion."""
        service = LLMService()
        provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]
        mock_litellm_completion.side_effect = Exception("API error")

        # Avoid depending on provider/model registry logic here.
        with patch.object(service, "_prepare_completion_kwargs", return_value=({}, None)):
            with pytest.raises(Exception, match="API error"):
                service._chat_completion(messages=messages, model="gpt-4o-mini", provider=provider)

    @patch("ml_tooling.llm.llm_service.litellm.completion")
    def test__chat_completion_returns_model_response(self, mock_litellm_completion):
        """_chat_completion should return a ModelResponse from litellm.completion."""
        from litellm import Choices, Message, ModelResponse

        service = LLMService()
        provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]

        mock_response = ModelResponse(
            id="test-id",
            choices=[Choices(message=Message(role="assistant", content="test response"))],
        )
        mock_litellm_completion.return_value = mock_response

        with patch.object(service, "_prepare_completion_kwargs", return_value=({}, None)):
            result = service._chat_completion(messages=messages, model="gpt-4o-mini", provider=provider)

        assert isinstance(result, ModelResponse)
        assert result.id == "test-id"
        mock_litellm_completion.assert_called_once()

    def test_structured_completion_returns_parsed_model(self):
        """structured_completion should parse the response content into the response_model."""
        service = LLMService()
        messages = [{"role": "user", "content": "test prompt"}]
        dummy_provider = _DummyProvider()

        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"value": "test", "number": 42}'))
        ]

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(service, "_chat_completion", return_value=mock_response) as mock_chat:
                result = service.structured_completion(
                    messages=messages,
                    response_model=SamplePydanticModel,
                    model="gpt-4o-mini",
                    max_tokens=100,
                    temperature=0.7,
                )

        assert isinstance(result, SamplePydanticModel)
        assert result.value == "test"
        assert result.number == 42
        mock_chat.assert_called_once()

    def test_structured_completion_raises_value_error_when_content_is_none(self):
        service = LLMService()
        dummy_provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]

        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content=None))]

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(service, "_chat_completion", return_value=mock_response):
                with pytest.raises(ValueError, match="Response content is None"):
                    service.structured_completion(
                        messages=messages,
                        response_model=SamplePydanticModel,
                        model="gpt-4o-mini",
                    )

    def test_structured_completion_raises_validation_error_for_invalid_json(self):
        from pydantic import ValidationError

        service = LLMService()
        dummy_provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]

        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content='{"invalid": "json"}'))]

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(service, "_chat_completion", return_value=mock_response):
                with pytest.raises(ValidationError):
                    service.structured_completion(
                        messages=messages,
                        response_model=SamplePydanticModel,
                        model="gpt-4o-mini",
                    )

    def test_structured_completion_passes_kwargs_to__chat_completion(self):
        service = LLMService()
        dummy_provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]

        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content='{"value": "test", "number": 42}'))
        ]

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(service, "_chat_completion", return_value=mock_response) as mock_chat:
                service.structured_completion(
                    messages=messages,
                    response_model=SamplePydanticModel,
                    model="gpt-4o-mini",
                    max_tokens=200,
                    temperature=0.5,
                    top_p=0.9,
                )

        call_kwargs = mock_chat.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o-mini"
        assert call_kwargs["response_format"] == SamplePydanticModel
        assert call_kwargs["max_tokens"] == 200
        assert call_kwargs["temperature"] == 0.5
        assert call_kwargs["top_p"] == 0.9
