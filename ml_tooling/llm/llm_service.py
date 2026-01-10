"""Service for interacting with LLM providers via LiteLLM."""

import threading
from typing import TypeVar

import litellm
from litellm import ModelResponse
from pydantic import BaseModel

from ml_tooling.llm.config.model_registry import ModelConfigRegistry
from ml_tooling.llm.providers.base import LLMProviderProtocol
from ml_tooling.llm.providers.registry import LLMProviderRegistry

T = TypeVar("T", bound=BaseModel)


class LLMService:
    """LLM service for making API requests via LiteLLM."""

    def __init__(self):
        """Initialize the LLM service.

        Initializes all registered provider instances on startup."""
        for provider_instance in LLMProviderRegistry._instances.values():
            provider_instance.initialize()

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
        return provider

    def _chat_completion(
        self,
        messages: list[dict],
        model: str,
        provider: LLMProviderProtocol,
        response_format: type[BaseModel] | None = None,
        **kwargs,
    ) -> ModelResponse:
        """
        Create a chat completion request using the specified provider.

        This is an internal method that delegates provider-specific logic
        (structured output formatting, kwargs preparation) to the provider.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            model: Model identifier to use
            provider: Provider instance for this model
            response_format: Pydantic model class for structured outputs
            **kwargs: Additional parameters to pass to the API (temperature, max_tokens, etc.)
                These override any default kwargs from the model configuration.

        Returns:
            The chat completion response from litellm

        Raises:
            Exception: Re-raises any exception from litellm.completion
        """
        try:
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
            completion_kwargs = provider.prepare_completion_kwargs(
                model=model,
                messages=messages,
                response_format=response_format_dict,
                model_config=model_config_dict,
                **kwargs,  # User kwargs override config kwargs
            )

            # Make the API call
            result = litellm.completion(**completion_kwargs)  # type: ignore

            # Coercion here to make sure that it is of type ModelResponse
            # LiteLLM can return either ModelResponse or a CustomStreamWrapper;
            # our use case isn't stream-based. This is to satisfy pyright.
            return (
                result
                if isinstance(result, ModelResponse)
                else ModelResponse(**result.__dict__)  # type: ignore
            )
        except Exception:
            raise

    def structured_completion(
        self,
        messages: list[dict],
        response_model: type[T],
        model: str | None = None,
        **kwargs,
    ) -> T:
        """
        Create a chat completion request and return the result as a Pydantic model.

        This is the main public API for structured completions. It orchestrates:
        1. Determining the correct provider for the model
        2. Running chat completion via _chat_completion (which delegates to provider)
        3. Handling the completion response

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            response_model: Pydantic model class to parse the response into
            model: Model to use (default: from config, falls back to gpt-4o-mini-2024-07-18)
            **kwargs: Additional parameters to pass to the API (temperature, max_tokens, etc.)
                These override any default kwargs from the model configuration.

        Returns:
            An instance of the specified Pydantic model parsed from the response

        Raises:
            ValueError: If the model is not supported by any provider, or if the response
                content is missing or invalid
            ValidationError: If the response cannot be parsed into the Pydantic model
        """
        # Step 1: Determine model (use default from config if not provided)
        if model is None:
            model = ModelConfigRegistry.get_default_model()

        # Step 2: Get provider for this model
        provider = self._get_provider_for_model(model)

        # Step 3: Run chat completion (delegates provider-specific logic)
        response: ModelResponse = self._chat_completion(
            messages=messages,
            model=model,
            provider=provider,
            response_format=response_model,
            **kwargs,
        )

        # Step 4: Handle completion response
        transformed_response: T = self.handle_completion_response(
            response, response_model
        )
        return transformed_response

    def handle_completion_response(
        self,
        response: ModelResponse,
        response_model: type[T],
    ) -> T:
        """Handles the completion response."""
        content: str | None = response.choices[0].message.content  # type: ignore
        if content is None:
            raise ValueError(
                "Response content is None. Expected structured output from LLM."
            )
        return response_model.model_validate_json(content)


# Provider function for dependency injection
_llm_service_instance: LLMService | None = None
_llm_service_lock = threading.Lock()


def get_llm_service() -> LLMService:
    """Dependency provider for LLM service."""
    global _llm_service_instance
    if _llm_service_instance is None:
        with _llm_service_lock:
            if _llm_service_instance is None:
                _llm_service_instance = LLMService()
    return _llm_service_instance
