"""Basic interface for doing LLM-based inference."""

import os

from langchain_community.chat_models import ChatLiteLLM
from litellm import acompletion, completion
from litellm.utils import ModelResponse
import tiktoken

from lib.helper import (
    GOOGLE_AI_STUDIO_KEY,
    GROQ_API_KEY,
    HF_TOKEN,
    OPENAI_API_KEY,
    track_performance,
)

# https://litellm.vercel.app/docs/providers/gemini
os.environ["GEMINI_API_KEY"] = GOOGLE_AI_STUDIO_KEY
os.environ["HUGGINGFACE_API_KEY"] = HF_TOKEN
os.environ["GROQ_API_KEY"] = GROQ_API_KEY
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

DEFAULT_GEMINI_SAFETY_SETTINGS = [
    {
        "category": "HARM_CATEGORY_HARASSMENT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_HATE_SPEECH",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_NONE",
    },
]

GEMINI_1_0_PRO_MODEL_NAME = "gemini-1.0-pro-latest"
GEMINI_1_5_PRO_MODEL_NAME = "gemini-1.5-pro-latest"

BACKEND_OPTIONS = {
    "Gemini": {
        # "model": "gemini/gemini-pro",
        # "model": f"gemini/{GEMINI_1_0_PRO_MODEL_NAME}",
        "model": f"gemini/{GEMINI_1_5_PRO_MODEL_NAME}",
        "kwargs": {
            "temperature": 0.0,
            "safety_settings": DEFAULT_GEMINI_SAFETY_SETTINGS,
        },
    },
    "Gemini-1.0": {
        # "model": "gemini/gemini-pro",
        "model": f"gemini/{GEMINI_1_0_PRO_MODEL_NAME}",
        "kwargs": {
            "temperature": 0.0,
            "safety_settings": DEFAULT_GEMINI_SAFETY_SETTINGS,
        },
    },
    "Gemini-1.5": {
        # "model": "gemini/gemini-pro",
        "model": f"gemini/{GEMINI_1_5_PRO_MODEL_NAME}",
        "kwargs": {
            "temperature": 0.0,
            "safety_settings": DEFAULT_GEMINI_SAFETY_SETTINGS,
        },
    },
    "Llama3-8B (via HuggingFace) (NOT SUPPORTED YET)": {
        "model": "huggingface/unsloth/llama-3-8b",
        "kwargs": {
            "api_base": "https://api-inference.huggingface.co/models/unsloth/llama-3-8b"  # noqa
        },
    },
    "Mixtral 8x22B (via HuggingFace)": {
        "model": "huggingface/mistralai/Mixtral-8x22B-v0.1",
        "kwargs": {
            "api_base": "https://api-inference.huggingface.co/models/mistralai/Mixtral-8x22B-v0.1"  # noqa
        },
    },
    "Llama3-8b (via Groq)": {  # 8k context limit
        "model": "groq/llama3-8b-8192",
        "kwargs": {
            "temperature": 0.0,
            # https://console.groq.com/docs/text-chat#json-mode
            "response_format": {"type": "json_object"},
        },
    },
    "Llama3-70b (via Groq)": {
        "model": "groq/llama3-70b-8192",
        "kwargs": {
            "temperature": 0.0,
            # https://console.groq.com/docs/text-chat#json-mode
            "response_format": {"type": "json_object"},
        },
    },
    "GPT-4o mini": {
        "model": "gpt-4o-mini",
        "kwargs": {
            "temperature": 0.0,
            # https://docs.litellm.ai/docs/completion/json_mode
            "response_format": {"type": "json_object"},
        },
    },
}


def get_langchain_litellm_chat_model(model_name: str = "Gemini") -> ChatLiteLLM:  # noqa
    """Returns a Langchain LiteLLM chat model."""
    model_dict = BACKEND_OPTIONS[model_name]
    return ChatLiteLLM(model=model_dict["model"], model_kwargs=model_dict["kwargs"])


# https://litellm.vercel.app/docs/completion/input
# https://litellm.vercel.app/docs/completion/reliable_completions


@track_performance
def run_query(
    prompt: str, role: str = "user", model_name: str = "Gemini", num_retries: int = 2
) -> str:
    """Runs a query to an LLM model and returns the response."""
    model_dict = BACKEND_OPTIONS[model_name]
    model_params = model_dict.copy()
    kwargs = {
        "model": model_params.pop("model"),
        "messages": [{"role": role, "content": prompt}],
        "num_retries": num_retries,
        **model_params["kwargs"],
    }
    response: ModelResponse = completion(**kwargs)
    content: str = response.get("choices", [{}])[0].get("message", {}).get("content")
    return content


async def async_run_query(
    prompt: str, role: str = "user", model_name: str = "Gemini", num_retries: int = 2
):
    """Runs a query to an LLM model and returns the response."""
    model_dict = BACKEND_OPTIONS[model_name]
    model_params = model_dict.copy()
    kwargs = {
        "model": model_params.pop("model"),
        "messages": [{"role": role, "content": prompt}],
        "num_retries": num_retries,
        **model_params["kwargs"],
    }
    response: ModelResponse = await acompletion(**kwargs)
    content: str = response.get("choices", [{}])[0].get("message", {}).get("content")
    return content


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string.

    Uses OpenAI tokenizer, but should be similar across models.
    """
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    num_tokens = len(encoding.encode(string))
    return num_tokens


if __name__ == "__main__":
    response = completion(
        model="gemini/gemini-pro",
        messages=[{"role": "user", "content": "write code for saying hi from LiteLLM"}],
        safety_settings=[
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_NONE",
            },
        ],
    )
    print(response)
