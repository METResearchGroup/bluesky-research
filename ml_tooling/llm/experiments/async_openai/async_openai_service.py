"""Async OpenAI service for structured batch completions.

This is an experiment-oriented alternative to `ml_tooling.llm.llm_service.LLMService`
that calls OpenAI directly using the official async SDK, while preserving:
- `structured_batch_completion` interface shape
- retry semantics (HTTP + validation)
- internal exception taxonomy (`ml_tooling.llm.exceptions`)

Key difference: it is *truly async* and uses semaphore-bounded concurrency.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, TypeVar, cast

from openai import AsyncOpenAI
from openai import (
    APIConnectionError,
    APIError,
    APIResponseValidationError,
    APIStatusError,
    APITimeoutError,
    OpenAIError,
    RateLimitError,
)
from pydantic import BaseModel

from lib.load_env_vars import EnvVarsContainer
from ml_tooling.llm.config.model_registry import ModelConfigRegistry
from ml_tooling.llm.exceptions import (
    LLMAuthError,
    LLMException,
    LLMInvalidRequestError,
    LLMPermissionDeniedError,
    LLMTransientError,
)
from ml_tooling.llm.providers.openai_provider import OpenAIProvider
from ml_tooling.llm.retry import retry_llm_completion

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class AsyncOpenAIService:
    """Async service that performs concurrent structured completions via OpenAI SDK."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        verbose: bool = False,
    ) -> None:
        if api_key is None:
            # Align with provider behavior (loads repo .env + applies RUN_MODE logic).
            api_key = EnvVarsContainer.get_env_var("OPENAI_API_KEY", required=True)

        self._client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._openai_provider = OpenAIProvider()

        if not verbose:
            self._suppress_noisy_logs()

    def _suppress_noisy_logs(self) -> None:
        # Keep parity with LLMService’s “quiet by default” behavior.
        logging.getLogger("httpx").setLevel(logging.WARNING)

    def _resolve_model(self, model: str | None) -> str:
        return model or ModelConfigRegistry.get_default_model()

    def _resolve_default_kwargs(self, model: str, **kwargs: Any) -> dict[str, Any]:
        """Merge registry defaults with user kwargs (user wins)."""
        try:
            model_config = ModelConfigRegistry.get_model_config(model)
            defaults = model_config.get_all_llm_inference_kwargs()
        except (ValueError, FileNotFoundError):
            defaults = {}
        return {**defaults, **kwargs}

    def _openai_response_format_for_model(
        self, response_model: type[BaseModel]
    ) -> dict[str, Any]:
        # Reuse existing schema formatting logic to match OpenAI strict requirements.
        # Note: OpenAIProvider.format_structured_output doesn’t require initialization.
        return self._openai_provider.format_structured_output(response_model, {})

    def _standardize_openai_exception(self, exception: Exception) -> LLMException:
        """Standardize OpenAI SDK exceptions into internal exception taxonomy."""
        message = str(exception) or f"{type(exception).__name__}"
        status_code = getattr(exception, "status_code", None)

        # Class-based handling where available.
        if isinstance(exception, RateLimitError):
            return LLMTransientError(message, original_exception=exception)
        if isinstance(exception, (APITimeoutError, APIConnectionError)):
            return LLMTransientError(message, original_exception=exception)
        if isinstance(exception, APIStatusError):
            # Use status_code mapping below.
            pass
        if isinstance(exception, (APIResponseValidationError,)):
            # Treated like a transient/provider failure; retryable by default.
            return LLMTransientError(message, original_exception=exception)
        if isinstance(exception, APIError):
            # Often includes 5xx; treat as transient unless we can prove otherwise.
            pass
        if isinstance(exception, OpenAIError):
            # Fall through to status_code mapping / heuristics.
            pass

        # Status-code based mapping (works across SDK versions).
        if isinstance(status_code, int):
            if status_code == 401:
                return LLMAuthError(message, original_exception=exception)
            if status_code == 403:
                return LLMPermissionDeniedError(message, original_exception=exception)
            if status_code in {400, 404, 409, 422}:
                return LLMInvalidRequestError(message, original_exception=exception)
            if status_code == 429:
                return LLMTransientError(message, original_exception=exception)
            if 500 <= status_code < 600:
                return LLMTransientError(message, original_exception=exception)

        # Heuristics fallback.
        lowered = message.lower()
        if any(
            needle in lowered
            for needle in (
                "rate limit",
                "timeout",
                "timed out",
                "temporarily",
                "try again",
                "connection",
                "overloaded",
                "unavailable",
            )
        ):
            return LLMTransientError(message, original_exception=exception)

        # Safe default: transient, to allow retry rather than failing hard.
        return LLMTransientError(message, original_exception=exception)

    def _extract_content(self, response: Any) -> str:
        # OpenAI SDK returns a pydantic-ish object; use attribute access.
        content: str | None = response.choices[0].message.content  # type: ignore[attr-defined]
        if content is None:
            raise ValueError(
                "Response content is None. Expected structured output from LLM."
            )
        return content

    @retry_llm_completion(max_retries=3, initial_delay=1.0, max_delay=60.0)
    async def _complete_and_validate_structured(
        self,
        *,
        prompt: str,
        response_model: type[T],
        model: str,
        role: str,
        request_kwargs: dict[str, Any],
    ) -> T:
        response_format = self._openai_response_format_for_model(response_model)
        try:
            # The OpenAI SDK uses strict TypedDict unions for messages/response_format.
            # We intentionally keep our public API flexible (string role, dict schema),
            # so we cast at the boundary.
            messages = cast(Any, [{"role": role, "content": prompt}])
            resp = await self._client.chat.completions.create(
                model=model,
                messages=messages,
                response_format=cast(Any, response_format),
                **request_kwargs,
            )
        except Exception as e:  # noqa: BLE001 - boundary standardization
            raise self._standardize_openai_exception(e) from e

        content = self._extract_content(resp)
        return response_model.model_validate_json(content)

    async def structured_batch_completion(
        self,
        *,
        prompts: list[str],
        response_model: type[T],
        model: str | None = None,
        role: str = "user",
        max_concurrency: int = 50,
        **kwargs: Any,
    ) -> list[T]:
        """Concurrently execute structured completions and return parsed models.

        Strict all-or-nothing semantics:
        - If any prompt fails (after retries), the whole call raises.
        - Pending tasks are cancelled on the first observed failure to reduce wasted spend.
        """
        if not prompts:
            return []
        if max_concurrency <= 0:
            raise ValueError(f"max_concurrency must be > 0, got {max_concurrency}")

        resolved_model = self._resolve_model(model)
        request_kwargs = self._resolve_default_kwargs(resolved_model, **kwargs)

        semaphore = asyncio.Semaphore(max_concurrency)

        async def run_one(idx: int, prompt: str) -> tuple[int, T]:
            async with semaphore:
                result = await self._complete_and_validate_structured(
                    prompt=prompt,
                    response_model=response_model,
                    model=resolved_model,
                    role=role,
                    request_kwargs=request_kwargs,
                )
                return idx, result

        tasks: list[asyncio.Task[tuple[int, T]]] = [
            asyncio.create_task(run_one(i, prompt))
            for i, prompt in enumerate(prompts)
        ]
        pending: set[asyncio.Task[tuple[int, T]]] = set(tasks)
        results: list[T | None] = [None] * len(prompts)

        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_EXCEPTION
                )

                # If any task errored, cancel the rest and raise immediately.
                for task in done:
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is not None:
                        for p in pending:
                            p.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        raise exc

                # No exceptions in this done-set; harvest results.
                for task in done:
                    if task.cancelled():
                        continue
                    idx, value = task.result()
                    results[idx] = value

            # All succeeded; results list should be fully populated.
            missing = [i for i, v in enumerate(results) if v is None]
            if missing:
                # Should never happen; treat as transient-ish failure.
                raise RuntimeError(
                    f"Missing results for indices: {missing}. This indicates an internal concurrency bug."
                )
            return [v for v in results if v is not None]
        finally:
            # Ensure no background tasks linger (especially on cancellation paths).
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
