"""Async LLM service for interacting with LLM providers via LiteLLM."""

from __future__ import annotations

import asyncio
import threading
from typing import TypeVar

import litellm
from litellm import ModelResponse
from pydantic import BaseModel

from ml_tooling.llm.base_llm_service import BaseLLMService
from ml_tooling.llm.config.model_registry import ModelConfigRegistry
from ml_tooling.llm.exceptions import standardize_litellm_exception
from ml_tooling.llm.providers.base import LLMProviderProtocol
from ml_tooling.llm.retry import retry_llm_completion

T = TypeVar("T", bound=BaseModel)


class AsyncLLMService(BaseLLMService):
    """LLM service for making API requests via LiteLLM (asynchronous)."""

    async def _chat_completion_async(
        self,
        messages: list[dict],
        model: str,
        provider: LLMProviderProtocol,
        response_format: type[BaseModel] | None = None,
        **kwargs,
    ) -> ModelResponse:
        """Create an async chat completion request using the specified provider."""
        completion_kwargs, _ = self._prepare_completion_kwargs(
            model=model,
            provider=provider,
            response_format=response_format,
            **kwargs,
        )
        completion_kwargs["messages"] = messages
        # Avoid global LiteLLM state; use the provider instance's key per request.
        completion_kwargs["api_key"] = provider.api_key

        try:
            result = await litellm.acompletion(**completion_kwargs)  # type: ignore
        except Exception as e:
            raise standardize_litellm_exception(e) from e

        return (
            result
            if isinstance(result, ModelResponse)
            else ModelResponse(**result.__dict__)  # type: ignore
        )

    async def _batch_completion_async(
        self,
        messages_list: list[list[dict]],
        model: str,
        provider: LLMProviderProtocol,
        response_format: type[BaseModel] | None = None,
        max_concurrent: int | None = None,
        **kwargs,
    ) -> list[ModelResponse]:
        """Create async batch completion requests via concurrent `acompletion` calls.

        Notes:
            - `asyncio.gather()` preserves result ordering.
            - `max_concurrent=None` means unbounded concurrency.
            - `max_concurrent<=0` is treated as unbounded concurrency.
        """
        if not messages_list:
            return []

        completion_kwargs, _ = self._prepare_completion_kwargs(
            model=model,
            provider=provider,
            response_format=response_format,
            **kwargs,
        )
        completion_kwargs.pop("messages", None)
        completion_kwargs["api_key"] = provider.api_key

        async def _one(messages: list[dict]) -> ModelResponse:
            request_kwargs = completion_kwargs.copy()
            request_kwargs["messages"] = messages
            try:
                result = await litellm.acompletion(**request_kwargs)  # type: ignore
            except Exception as e:
                raise standardize_litellm_exception(e) from e

            return (
                result
                if isinstance(result, ModelResponse)
                else ModelResponse(**result.__dict__)  # type: ignore
            )

        if max_concurrent is None or max_concurrent <= 0:
            tasks = [_one(messages) for messages in messages_list]
            return list(await asyncio.gather(*tasks))

        semaphore = asyncio.Semaphore(max_concurrent)

        async def _limited(messages: list[dict]) -> ModelResponse:
            async with semaphore:
                return await _one(messages)

        tasks = [_limited(messages) for messages in messages_list]
        return list(await asyncio.gather(*tasks))

    @retry_llm_completion(max_retries=3, initial_delay=1.0, max_delay=60.0)
    async def _complete_and_validate_structured_async(
        self,
        messages: list[dict],
        model: str,
        provider: LLMProviderProtocol,
        response_format: type[T],
        **kwargs,
    ) -> T:
        """Execute async chat completion and validate/parse the response."""
        response = await self._chat_completion_async(
            messages=messages,
            model=model,
            provider=provider,
            response_format=response_format,
            **kwargs,
        )
        return self.handle_completion_response(response, response_format)

    @retry_llm_completion(max_retries=3, initial_delay=1.0, max_delay=60.0)
    async def _complete_and_validate_structured_batch_async(
        self,
        messages_list: list[list[dict]],
        model: str,
        provider: LLMProviderProtocol,
        response_format: type[T],
        max_concurrent: int | None = None,
        **kwargs,
    ) -> list[T]:
        """Execute async batch completion and validate/parse all responses."""
        responses = await self._batch_completion_async(
            messages_list=messages_list,
            model=model,
            provider=provider,
            response_format=response_format,
            max_concurrent=max_concurrent,
            **kwargs,
        )
        return self.handle_batch_completion_responses(responses, response_format)

    async def structured_completion_async(
        self,
        messages: list[dict],
        response_model: type[T],
        model: str | None = None,
        **kwargs,
    ) -> T:
        """Create an async chat completion request and return a Pydantic model."""
        if model is None:
            model = ModelConfigRegistry.get_default_model()
        provider = self._get_provider_for_model(model)
        return await self._complete_and_validate_structured_async(
            messages=messages,
            model=model,
            provider=provider,
            response_format=response_model,
            **kwargs,
        )

    async def structured_batch_completion_async(
        self,
        prompts: list[str],
        response_model: type[T],
        model: str | None = None,
        role: str = "user",
        max_concurrent: int | None = None,
        **kwargs,
    ) -> list[T]:
        """Create async batch completion requests and return Pydantic models."""
        if model is None:
            model = ModelConfigRegistry.get_default_model()
        provider = self._get_provider_for_model(model)
        messages_list = [[{"role": role, "content": prompt}] for prompt in prompts]
        return await self._complete_and_validate_structured_batch_async(
            messages_list=messages_list,
            model=model,
            provider=provider,
            response_format=response_model,
            max_concurrent=max_concurrent,
            **kwargs,
        )


_async_llm_service_instance: AsyncLLMService | None = None
_async_llm_service_lock = threading.Lock()


def get_async_llm_service() -> AsyncLLMService:
    """Dependency provider for async LLM service."""
    global _async_llm_service_instance
    if _async_llm_service_instance is None:
        with _async_llm_service_lock:
            if _async_llm_service_instance is None:
                _async_llm_service_instance = AsyncLLMService()
    return _async_llm_service_instance
