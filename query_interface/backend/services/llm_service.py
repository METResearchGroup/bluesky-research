"""Service for interacting with LLM providers via LiteLLM."""

import copy
import threading

import litellm
from litellm import ModelResponse
from pydantic import BaseModel

from lib.helper import OPENAI_API_KEY

DEFAULT_MODEL = "gpt-4o-mini-2024-07-18"


class LLMService:
    """LLM service for making API requests via LiteLLM."""

    def __init__(self):
        self.openai_api_key = OPENAI_API_KEY
        # Set the API key for litellm to use
        litellm.api_key = self.openai_api_key

    def chat_completion(
        self,
        messages: list[dict],
        model: str = DEFAULT_MODEL,
        response_format: type[BaseModel] | None = None,
        **kwargs,
    ) -> ModelResponse:
        """
        Create a chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            model: Model to use (default: gpt-4o-mini-2024-07-18)
            response_format: Pydantic model class for structured outputs
            **kwargs: Additional parameters to pass to the API (temperature, max_tokens, etc.)

        Returns:
            The chat completion response from litellm
        """
        # If response_format is a Pydantic model, we need to fix the schema
        # to add additionalProperties: false for OpenAI compatibility
        if response_format is not None:
            schema = response_format.model_json_schema()
            # NOTE: later on, we'll see if there's a better way to do this.
            # Right now, looks like OpenAI is annoyingly strict with their schema
            # and I haven't found a better way to do this.
            fixed_schema = self._fix_schema_for_openai(schema)

            # Convert to OpenAI's structured output format
            response_format_dict = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_format.__name__.lower(),
                    "strict": True,
                    "schema": fixed_schema,
                },
            }
            return litellm.completion(
                model=model,
                messages=messages,
                response_format=response_format_dict,
                **kwargs,
            )

        return litellm.completion(
            model=model, messages=messages, response_format=response_format, **kwargs
        )

    def _fix_schema_for_openai(self, schema: dict) -> dict:
        """
        Recursively add additionalProperties: false to all object definitions.

        OpenAI's structured outputs require all object types to have
        additionalProperties: false explicitly set.
        """
        schema_copy = copy.deepcopy(schema)

        def patch(obj: dict) -> None:
            if isinstance(obj, dict):
                if obj.get("type") == "object":
                    obj["additionalProperties"] = False
                # Recursively patch nested objects
                for value in obj.values():
                    if isinstance(value, dict):
                        patch(value)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                patch(item)

        patch(schema_copy)
        return schema_copy


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
