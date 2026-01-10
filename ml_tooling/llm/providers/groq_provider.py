"""Groq provider implementation."""

from typing import Any

import litellm
from pydantic import BaseModel

from lib.load_env_vars import EnvVarsContainer
from ml_tooling.llm.providers.base import LLMProviderProtocol


class GroqProvider(LLMProviderProtocol):
    """Groq provider implementation.

    Handles Groq-specific logic:
    - API key management
    - Structured output format (json_object mode)
    - Model identifier normalization
    """

    def __init__(self):
        self._initialized = False

    @property
    def provider_name(self) -> str:
        return "groq"

    @property
    def supported_models(self) -> list[str]:
        return [
            "groq/llama3-8b-8192",
            "groq/llama3-70b-8192",
        ]

    def initialize(self, api_key: str | None = None) -> None:
        if api_key is None:
            api_key = EnvVarsContainer.get_env_var("GROQ_API_KEY", required=True)
        if not self._initialized:
            litellm.api_key = api_key
            self._initialized = True

    def supports_model(self, model_name: str) -> bool:
        return model_name in self.supported_models

    def format_structured_output(
        self,
        response_model: type[BaseModel],
        model_config: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Format Groq's structured output format."""
        raise NotImplementedError(
            "We'll revisit this later when actively working with Groq models."
        )

    def prepare_completion_kwargs(
        self,
        model: str,
        messages: list[dict],
        response_format: dict[str, Any] | None,
        model_config: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """Prepare Groq-specific completion kwargs."""
        raise NotImplementedError(
            "We'll revisit this later when actively working with Groq models."
        )
