"""Unit tests for AsyncLLMService."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel, Field

from ml_tooling.llm.async_llm_service import AsyncLLMService
from ml_tooling.llm.exceptions import LLMAuthError, LLMTransientError
from ml_tooling.llm.providers.base import LLMProviderProtocol


class SamplePydanticModel(BaseModel):
    value: str = Field(description="A test value")
    number: int = Field(description="A test number")


class _DummyProvider(LLMProviderProtocol):
    """Minimal provider stub to satisfy AsyncLLMService internals in unit tests."""

    _initialized = True
    _api_key = "dummy-test-key"

    @property
    def provider_name(self) -> str:
        return "dummy"

    @property
    def supported_models(self) -> list[str]:
        return ["dummy-model"]

    @property
    def api_key(self) -> str:
        return self._api_key

    def initialize(self, api_key=None) -> None:  # noqa: ANN001
        return None

    def supports_model(self, model_name: str) -> bool:  # noqa: ARG002
        return True

    def format_structured_output(self, response_model, model_config):  # noqa: ANN001,ARG002
        return {"type": "json_schema", "json_schema": {"schema": {}}}

    def prepare_completion_kwargs(
        self, model: str, messages: list[dict], response_format, model_config, **kwargs  # noqa: ANN001,ARG002
    ) -> dict:
        return {"model": model, "messages": messages, **kwargs}


class TestAsyncLLMService__chat_completion_async:
    """Tests for AsyncLLMService._chat_completion_async."""

    @pytest.mark.asyncio
    @patch("ml_tooling.llm.async_llm_service.litellm.acompletion", new_callable=AsyncMock)
    async def test_reraises_standardized_exception_on_litellm_error(
        self, mock_litellm_acompletion
    ):
        """_chat_completion_async should standardize and re-raise LiteLLM exceptions."""
        service = AsyncLLMService()
        provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]
        mock_litellm_acompletion.side_effect = Exception("API error")

        with patch.object(service, "_prepare_completion_kwargs", return_value=({}, None)):
            with pytest.raises(Exception, match="API error"):
                await service._chat_completion_async(
                    messages=messages, model="gpt-4o-mini", provider=provider
                )

    @pytest.mark.asyncio
    @patch("ml_tooling.llm.async_llm_service.litellm.acompletion", new_callable=AsyncMock)
    async def test_returns_model_response(self, mock_litellm_acompletion):
        """_chat_completion_async should return a ModelResponse from litellm.acompletion."""
        from litellm import Choices, Message, ModelResponse

        service = AsyncLLMService()
        provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]

        mock_response = ModelResponse(
            id="test-id",
            choices=[Choices(message=Message(role="assistant", content="test response"))],
        )
        mock_litellm_acompletion.return_value = mock_response

        with patch.object(service, "_prepare_completion_kwargs", return_value=({}, None)):
            result = await service._chat_completion_async(
                messages=messages, model="gpt-4o-mini", provider=provider
            )

        assert isinstance(result, ModelResponse)
        assert result.id == "test-id"
        mock_litellm_acompletion.assert_awaited_once()


class TestAsyncLLMService__batch_completion_async:
    """Tests for AsyncLLMService._batch_completion_async."""

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty_list(self):
        """_batch_completion_async should return an empty list for empty input."""
        service = AsyncLLMService()
        provider = _DummyProvider()

        result = await service._batch_completion_async(
            messages_list=[], model="gpt-4o-mini", provider=provider
        )
        assert result == []

    @pytest.mark.asyncio
    @patch("ml_tooling.llm.async_llm_service.litellm.acompletion", new_callable=AsyncMock)
    async def test_preserves_order(self, mock_litellm_acompletion):
        """_batch_completion_async should preserve input ordering in its outputs."""
        from litellm import Choices, Message, ModelResponse

        service = AsyncLLMService()
        provider = _DummyProvider()
        messages_list = [
            [{"role": "user", "content": "p0"}],
            [{"role": "user", "content": "p1"}],
            [{"role": "user", "content": "p2"}],
        ]

        async def side_effect(*args, **kwargs):  # noqa: ANN001
            content = kwargs["messages"][0]["content"]
            # Invert completion timing so results would be scrambled without gather ordering.
            await asyncio.sleep({"p0": 0.03, "p1": 0.02, "p2": 0.01}[content])
            return ModelResponse(
                id=f"id-{content}",
                choices=[
                    Choices(message=Message(role="assistant", content=f"resp-{content}"))
                ],
            )

        mock_litellm_acompletion.side_effect = side_effect

        with patch.object(service, "_prepare_completion_kwargs", return_value=({}, None)):
            results = await service._batch_completion_async(
                messages_list=messages_list, model="gpt-4o-mini", provider=provider
            )

        ids = [r.id for r in results]
        assert ids == ["id-p0", "id-p1", "id-p2"]
        assert mock_litellm_acompletion.await_count == 3

    @pytest.mark.asyncio
    @patch("ml_tooling.llm.async_llm_service.litellm.acompletion", new_callable=AsyncMock)
    async def test_respects_max_concurrent(self, mock_litellm_acompletion):
        """_batch_completion_async should not exceed max_concurrent in-flight requests."""
        from litellm import Choices, Message, ModelResponse

        service = AsyncLLMService()
        provider = _DummyProvider()
        messages_list = [[{"role": "user", "content": f"p{i}"}] for i in range(8)]

        in_flight = 0
        max_seen = 0
        lock = asyncio.Lock()

        async def side_effect(*args, **kwargs):  # noqa: ANN001
            nonlocal in_flight, max_seen
            async with lock:
                in_flight += 1
                max_seen = max(max_seen, in_flight)
            await asyncio.sleep(0.02)
            async with lock:
                in_flight -= 1
            content = kwargs["messages"][0]["content"]
            return ModelResponse(
                id=f"id-{content}",
                choices=[Choices(message=Message(role="assistant", content=content))],
            )

        mock_litellm_acompletion.side_effect = side_effect

        with patch.object(service, "_prepare_completion_kwargs", return_value=({}, None)):
            _ = await service._batch_completion_async(
                messages_list=messages_list,
                model="gpt-4o-mini",
                provider=provider,
                max_concurrent=3,
            )

        assert max_seen <= 3


class TestAsyncLLMService_structured_completion_async:
    """Tests for AsyncLLMService.structured_completion_async."""

    @pytest.mark.asyncio
    async def test_parses_response_model(self):
        """structured_completion_async should return a parsed Pydantic model."""
        service = AsyncLLMService()
        dummy_provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]

        parsed_model = SamplePydanticModel(value="test", number=42)

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(
                service, "_complete_and_validate_structured_async", return_value=parsed_model
            ) as mock_complete:
                result = await service.structured_completion_async(
                    messages=messages,
                    response_model=SamplePydanticModel,
                    model="gpt-4o-mini",
                    max_tokens=100,
                    temperature=0.7,
                )

        assert isinstance(result, SamplePydanticModel)
        assert result.value == "test"
        assert result.number == 42
        mock_complete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passes_kwargs_through(self):
        """structured_completion_async should pass kwargs through to _complete_and_validate_structured_async."""
        service = AsyncLLMService()
        dummy_provider = _DummyProvider()
        messages = [{"role": "user", "content": "test prompt"}]
        parsed_model = SamplePydanticModel(value="test", number=42)

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(
                service, "_complete_and_validate_structured_async", return_value=parsed_model
            ) as mock_complete:
                _ = await service.structured_completion_async(
                    messages=messages,
                    response_model=SamplePydanticModel,
                    model="gpt-4o-mini",
                    max_tokens=200,
                    temperature=0.5,
                    top_p=0.9,
                )

        call_kwargs = mock_complete.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o-mini"
        assert call_kwargs["response_format"] == SamplePydanticModel
        assert call_kwargs["max_tokens"] == 200
        assert call_kwargs["temperature"] == 0.5
        assert call_kwargs["top_p"] == 0.9


class TestAsyncLLMService_structured_batch_completion_async:
    """Tests for AsyncLLMService.structured_batch_completion_async."""

    @pytest.mark.asyncio
    async def test_passes_max_concurrent_through(self):
        """structured_batch_completion_async should pass max_concurrent through to _complete_and_validate_structured_batch_async."""
        service = AsyncLLMService()
        dummy_provider = _DummyProvider()

        parsed_models = [SamplePydanticModel(value="v", number=1)]

        with patch.object(service, "_get_provider_for_model", return_value=dummy_provider):
            with patch.object(
                service,
                "_complete_and_validate_structured_batch_async",
                return_value=parsed_models,
            ) as mock_complete:
                _ = await service.structured_batch_completion_async(
                    prompts=["p"],
                    response_model=SamplePydanticModel,
                    model="gpt-4o-mini",
                    max_concurrent=7,
                )

        assert mock_complete.call_args.kwargs["max_concurrent"] == 7


class TestRetryIntegration_async:
    """Tests for retry behavior on async retry-decorated methods."""

    @pytest.mark.asyncio
    async def test_retries_transient_then_succeeds(self):
        """_complete_and_validate_structured_async should retry transient errors and eventually succeed."""
        from litellm import Choices, Message, ModelResponse

        service = AsyncLLMService()
        provider = _DummyProvider()

        expected = SamplePydanticModel(value="ok", number=1)

        calls = 0

        async def side_effect(*args, **kwargs):  # noqa: ANN001
            nonlocal calls
            calls += 1
            if calls < 2:
                raise LLMTransientError("transient")
            # Return a LiteLLM-style response so parsing is exercised.
            return ModelResponse(
                id="test-id",
                choices=[
                    Choices(
                        message=Message(
                            role="assistant", content=expected.model_dump_json()
                        )
                    )
                ],
            )

        with patch.object(service, "_chat_completion_async", side_effect=side_effect):
            result = await service._complete_and_validate_structured_async(
                messages=[{"role": "user", "content": "x"}],
                model="gpt-4o-mini",
                provider=provider,
                response_format=SamplePydanticModel,
            )

        assert result == expected
        assert calls == 2

    @pytest.mark.asyncio
    async def test_does_not_retry_auth_error(self):
        """_complete_and_validate_structured_async should not retry auth errors."""
        service = AsyncLLMService()
        provider = _DummyProvider()

        calls = 0

        async def side_effect(*args, **kwargs):  # noqa: ANN001
            nonlocal calls
            calls += 1
            raise LLMAuthError("nope")

        with patch.object(service, "_chat_completion_async", side_effect=side_effect):
            with pytest.raises(LLMAuthError, match="nope"):
                await service._complete_and_validate_structured_async(
                    messages=[{"role": "user", "content": "x"}],
                    model="gpt-4o-mini",
                    provider=provider,
                    response_format=SamplePydanticModel,
                )

        assert calls == 1

