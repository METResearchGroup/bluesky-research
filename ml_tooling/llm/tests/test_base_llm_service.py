"""Unit tests for BaseLLMService."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from pydantic import BaseModel, Field, ValidationError

from ml_tooling.llm.base_llm_service import BaseLLMService
from ml_tooling.llm.providers.base import LLMProviderProtocol


class SamplePydanticModel(BaseModel):
    value: str = Field(description="A test value")
    number: int = Field(description="A test number")


class TestBaseLLMService_handle_completion_response:
    """Tests for BaseLLMService.handle_completion_response."""

    def test_parses_valid_json(self):
        """handle_completion_response should parse valid JSON into the response model."""
        from litellm import Choices, Message, ModelResponse

        service = BaseLLMService()
        expected = SamplePydanticModel(value="v", number=1)
        response = ModelResponse(
            id="test-id",
            choices=[
                Choices(
                    message=Message(role="assistant", content=expected.model_dump_json())
                )
            ],
        )

        result = service.handle_completion_response(response, SamplePydanticModel)

        assert result == expected

    def test_raises_value_error_when_content_is_none(self):
        """handle_completion_response should raise ValueError when response content is None."""
        from litellm import Choices, Message, ModelResponse

        service = BaseLLMService()
        response = ModelResponse(
            id="test-id",
            choices=[Choices(message=Message(role="assistant", content=None))],
        )

        with pytest.raises(ValueError, match="Response content is None"):
            service.handle_completion_response(response, SamplePydanticModel)

    def test_raises_validation_error_for_invalid_json(self):
        """handle_completion_response should raise ValidationError for invalid JSON."""
        from litellm import Choices, Message, ModelResponse

        service = BaseLLMService()
        response = ModelResponse(
            id="test-id",
            choices=[Choices(message=Message(role="assistant", content="{}"))],
        )

        with pytest.raises(ValidationError):
            service.handle_completion_response(response, SamplePydanticModel)


class TestBaseLLMService_handle_batch_completion_responses:
    """Tests for BaseLLMService.handle_batch_completion_responses."""

    def test_parses_all_responses_in_order(self):
        """handle_batch_completion_responses should parse all responses into models in order."""
        from litellm import Choices, Message, ModelResponse

        service = BaseLLMService()
        expected = [
            SamplePydanticModel(value="a", number=1),
            SamplePydanticModel(value="b", number=2),
        ]
        responses = [
            ModelResponse(
                id="id-a",
                choices=[
                    Choices(
                        message=Message(
                            role="assistant", content=expected[0].model_dump_json()
                        )
                    )
                ],
            ),
            ModelResponse(
                id="id-b",
                choices=[
                    Choices(
                        message=Message(
                            role="assistant", content=expected[1].model_dump_json()
                        )
                    )
                ],
            ),
        ]

        result = service.handle_batch_completion_responses(responses, SamplePydanticModel)

        assert result == expected

    def test_raises_value_error_when_any_content_is_none(self):
        """handle_batch_completion_responses should raise ValueError if any response content is None."""
        from litellm import Choices, Message, ModelResponse

        service = BaseLLMService()
        responses = [
            ModelResponse(
                id="id-a",
                choices=[Choices(message=Message(role="assistant", content='{"value":"a","number":1}'))],
            ),
            ModelResponse(
                id="id-b",
                choices=[Choices(message=Message(role="assistant", content=None))],
            ),
        ]

        with pytest.raises(ValueError, match="Response content is None"):
            service.handle_batch_completion_responses(responses, SamplePydanticModel)

    def test_raises_validation_error_when_any_json_is_invalid(self):
        """handle_batch_completion_responses should raise ValidationError if any response JSON is invalid."""
        from litellm import Choices, Message, ModelResponse

        service = BaseLLMService()
        responses = [
            ModelResponse(
                id="id-a",
                choices=[Choices(message=Message(role="assistant", content='{"value":"a","number":1}'))],
            ),
            ModelResponse(
                id="id-b",
                choices=[Choices(message=Message(role="assistant", content="{}"))],
            ),
        ]

        with pytest.raises(ValidationError):
            service.handle_batch_completion_responses(responses, SamplePydanticModel)


class TestBaseLLMService__get_provider_for_model:
    """Tests for BaseLLMService._get_provider_for_model."""

    def test_initializes_provider_when_not_initialized(self):
        """_get_provider_for_model should initialize provider if not already initialized."""
        service = BaseLLMService()
        provider = Mock(spec=LLMProviderProtocol)
        setattr(provider, "_initialized", False)

        with patch("ml_tooling.llm.base_llm_service.LLMProviderRegistry.get_provider", return_value=provider):
            result = service._get_provider_for_model("some-model")

        assert result is provider
        provider.initialize.assert_called_once()

    def test_does_not_initialize_provider_when_already_initialized(self):
        """_get_provider_for_model should not initialize provider if already initialized."""
        service = BaseLLMService()
        provider = Mock(spec=LLMProviderProtocol)
        setattr(provider, "_initialized", True)

        with patch("ml_tooling.llm.base_llm_service.LLMProviderRegistry.get_provider", return_value=provider):
            result = service._get_provider_for_model("some-model")

        assert result is provider
        provider.initialize.assert_not_called()


class TestBaseLLMService__prepare_completion_kwargs:
    """Tests for BaseLLMService._prepare_completion_kwargs."""

    def test_uses_model_config_and_allows_overrides(self):
        """_prepare_completion_kwargs should merge model config kwargs with user overrides."""
        service = BaseLLMService()
        provider = Mock(spec=LLMProviderProtocol)

        model_config_obj = Mock()
        model_config_obj.get_all_llm_inference_kwargs.return_value = {"temperature": 0.1}

        provider.format_structured_output.return_value = {"type": "json_schema"}

        captured = {}

        def prepare_completion_kwargs(**kwargs):  # noqa: ANN001
            captured.update(kwargs)
            return {"ok": True}

        provider.prepare_completion_kwargs.side_effect = prepare_completion_kwargs

        with patch(
            "ml_tooling.llm.base_llm_service.ModelConfigRegistry.get_model_config",
            return_value=model_config_obj,
        ):
            completion_kwargs, response_format_dict = service._prepare_completion_kwargs(
                model="m",
                provider=provider,
                response_format=SamplePydanticModel,
                temperature=0.9,
                max_tokens=123,
            )

        assert completion_kwargs == {"ok": True}
        assert response_format_dict == {"type": "json_schema"}
        assert captured["model"] == "m"
        assert captured["messages"] == []
        assert captured["response_format"] == {"type": "json_schema"}
        assert captured["model_config"] == {"kwargs": {"temperature": 0.1}}
        assert captured["temperature"] == 0.9
        assert captured["max_tokens"] == 123

    def test_falls_back_to_empty_model_config_when_missing(self):
        """_prepare_completion_kwargs should fall back to empty model config if registry lookup fails."""
        service = BaseLLMService()
        provider = Mock(spec=LLMProviderProtocol)
        provider.format_structured_output.return_value = None

        captured = {}

        def prepare_completion_kwargs(**kwargs):  # noqa: ANN001
            captured.update(kwargs)
            return {"ok": True}

        provider.prepare_completion_kwargs.side_effect = prepare_completion_kwargs

        with patch(
            "ml_tooling.llm.base_llm_service.ModelConfigRegistry.get_model_config",
            side_effect=ValueError("nope"),
        ):
            completion_kwargs, response_format_dict = service._prepare_completion_kwargs(
                model="m",
                provider=provider,
                response_format=None,
                temperature=0.5,
            )

        assert completion_kwargs == {"ok": True}
        assert response_format_dict is None
        assert captured["model_config"] == {"kwargs": {}}
        assert captured["temperature"] == 0.5

