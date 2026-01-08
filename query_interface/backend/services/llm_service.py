"""Service for interacting with LLM providers via LiteLLM."""

import copy
import threading
from typing import TypeVar

import litellm
from litellm import ModelResponse
from pydantic import BaseModel

from lib.helper import OPENAI_API_KEY
from query_interface.backend.config import get_config_value

T = TypeVar("T", bound=BaseModel)


class LLMService:
    """LLM service for making API requests via LiteLLM."""

    def __init__(self):
        self.openai_api_key = OPENAI_API_KEY
        # Set the API key for litellm to use
        litellm.api_key = self.openai_api_key

    def chat_completion(
        self,
        messages: list[dict],
        model: str | None = None,
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
        if model is None:
            model = get_config_value("llm", "default_model")

        # Prepare the parameters for litellm.completion
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
            result = litellm.completion(
                model=model,  # type: ignore
                messages=messages,
                response_format=response_format_dict,
                **kwargs,
            )
        else:
            result = litellm.completion(
                model=model,  # type: ignore
                messages=messages,
                response_format=response_format,
                **kwargs,
            )

        # coercion here to make sure that it is of type ModelResponse
        # LiteLLM can return either ModelResponse or a CustomStreamWrapper;
        # our use case isn't stream-based. This is to satisfy pyright.
        return (
            result
            if isinstance(result, ModelResponse)
            else ModelResponse(**result.__dict__)  # type: ignore
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

    def structured_completion(
        self,
        messages: list[dict],
        response_model: type[T],
        model: str | None = None,
        **kwargs,
    ) -> T:
        """
        Create a chat completion request and return the result as a Pydantic model.

        This is a convenience method that combines chat_completion with response
        parsing. It handles extracting the content from the response and parsing
        it into the specified Pydantic model.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            response_model: Pydantic model class to parse the response into
            model: Model to use (default: gpt-4o-mini-2024-07-18)
            **kwargs: Additional parameters to pass to the API (temperature, max_tokens, etc.)

        Returns:
            An instance of the specified Pydantic model parsed from the response

        Raises:
            ValueError: If the response content is missing or invalid
            ValidationError: If the response cannot be parsed into the Pydantic model
        """
        response: ModelResponse = self.chat_completion(
            messages=messages,
            model=model,
            response_format=response_model,
            **kwargs,
        )

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
