"""HuggingFace provider implementation."""

import os
from typing import Any

from litellm import ModelResponse
from pydantic import BaseModel

from lib.load_env_vars import EnvVarsContainer
from ml_tooling.llm.providers.base import LLMProviderProtocol


class HuggingFaceProvider(LLMProviderProtocol):
    """HuggingFace provider implementation.

    Handles HuggingFace-specific logic:
    - API key management (uses HF_TOKEN -> HUGGINGFACE_API_KEY)
    - Custom API base URLs
    - Model identifier normalization
    """

    def __init__(self):
        self._initialized = False

    @property
    def provider_name(self) -> str:
        return "huggingface"

    @property
    def supported_models(self) -> list[str]:
        return [
            "huggingface/unsloth/llama-3-8b",
            "huggingface/mistralai/Mixtral-8x22B-v0.1",
        ]

    def initialize(self, api_key: str | None = None) -> None:
        if api_key is None:
            api_key = EnvVarsContainer.get_env_var("HF_TOKEN", required=True)
        if not self._initialized:
            # HuggingFace uses HUGGINGFACE_API_KEY environment variable
            os.environ["HUGGINGFACE_API_KEY"] = api_key  # type: ignore
            self._initialized = True

    def supports_model(self, model_name: str) -> bool:
        return model_name in self.supported_models

    def format_structured_output(
        self,
        response_model: type[BaseModel],
        model_config: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Format HuggingFace's structured output format."""
        raise NotImplementedError(
            "We'll revisit this later when actively working with HuggingFace models."
        )

    def prepare_completion_kwargs(
        self,
        model: str,
        messages: list[dict],
        response_format: dict[str, Any] | None,
        model_config: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """Prepare HuggingFace-specific completion kwargs."""
        raise NotImplementedError(
            "We'll revisit this later when actively working with HuggingFace models."
        )

    def handle_completion_response(
        self,
        response: ModelResponse,
        response_model: type[BaseModel] | None = None,
    ) -> BaseModel | None:
        """Handle the completion response from HuggingFace."""
        raise NotImplementedError(
            "We'll revisit this later when actively working with HuggingFace models."
        )
