"""Shared base for sync and async LLM services."""

from __future__ import annotations

from typing import Any, TypeVar

from litellm import ModelResponse
from pydantic import BaseModel

from ml_tooling.llm.config.model_registry import ModelConfigRegistry
from ml_tooling.llm.providers.base import LLMProviderProtocol
from ml_tooling.llm.providers.registry import LLMProviderRegistry

T = TypeVar("T", bound=BaseModel)


class BaseLLMService:
    """Shared base for sync and async LLM services.

    This class contains only synchronous helper methods that are shared by both
    `LLMService` (sync) and `AsyncLLMService` (async) to avoid duplication.
    """

    def __init__(self, verbose: bool = False):
        """Initialize the service.

        Args:
            verbose: If False (default), suppresses LiteLLM info and debug logs.
                    If True, does not suppress LiteLLM logs (uses LiteLLM defaults).

        Note: Providers are initialized lazily when first used to avoid
        requiring API keys for all providers when only one is needed.
        """
        if not verbose:
            self._suppress_litellm_logging()

    def _suppress_litellm_logging(self) -> None:
        """Configure logging to suppress LiteLLM info and debug logs.

        See https://github.com/BerriAI/litellm/issues/6813
        """
        import logging

        logging.getLogger("LiteLLM").setLevel(logging.WARNING)

    def _get_provider_for_model(self, model: str) -> LLMProviderProtocol:
        """Get the provider instance for a given model.

        Args:
            model: Model identifier (e.g., 'gpt-4o-mini', 'groq/llama3-8b-8192')

        Returns:
            Provider instance that supports the given model

        Raises:
            ValueError: If no provider supports the given model
        """
        provider = LLMProviderRegistry.get_provider(model)
        # Lazy initialization: only initialize the provider when it's actually used
        if not getattr(provider, "_initialized", False):
            provider.initialize()
        return provider

    def _prepare_completion_kwargs(
        self,
        model: str,
        provider: LLMProviderProtocol,
        response_format: type[BaseModel] | None = None,
        **kwargs,
    ) -> tuple[dict, dict[str, Any] | None]:
        """Extract shared logic for preparing completion kwargs.

        Used by both single and batch completion methods to avoid duplication.
        Handles model config resolution, response format formatting, and kwargs
        preparation via provider.
        """
        # Get model configuration from registry
        try:
            model_config_obj = ModelConfigRegistry.get_model_config(model)
            # Convert ModelConfig to dict format expected by providers
            model_config_dict = {
                "kwargs": model_config_obj.get_all_llm_inference_kwargs()
            }
        except (ValueError, FileNotFoundError):
            # Model not in config - use empty config dict
            model_config_dict = {"kwargs": {}}

        # Format structured output if needed (delegates to provider)
        response_format_dict = None
        if response_format is not None:
            response_format_dict = provider.format_structured_output(
                response_format, model_config_dict
            )

        # Prepare completion kwargs using provider-specific logic
        # Note: messages is passed as placeholder empty list here, will be set by caller
        completion_kwargs = provider.prepare_completion_kwargs(
            model=model,
            messages=[],  # Placeholder, will be set by caller
            response_format=response_format_dict,
            model_config=model_config_dict,
            **kwargs,  # User kwargs override config kwargs
        )

        return completion_kwargs, response_format_dict

    def handle_completion_response(
        self,
        response: ModelResponse,
        response_model: type[T],
    ) -> T:
        """Handle a single completion response and parse into `response_model`."""
        content: str | None = response.choices[0].message.content  # type: ignore
        if content is None:
            raise ValueError(
                "Response content is None. Expected structured output from LLM."
            )
        return response_model.model_validate_json(content)

    def handle_batch_completion_responses(
        self,
        responses: list[ModelResponse],
        response_model: type[T],
    ) -> list[T]:
        """Handle batch completion responses and parse each into `response_model`."""
        contents = []
        for response in responses:
            content: str | None = response.choices[0].message.content  # type: ignore
            if content is None:
                raise ValueError(
                    "Response content is None. Expected structured output from LLM."
                )
            contents.append(content)

        return [response_model.model_validate_json(content) for content in contents]
