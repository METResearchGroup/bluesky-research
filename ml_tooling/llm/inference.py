"""Basic interface for doing LLM-based inference."""
import os

from litellm import completion
from litellm.utils import ModelResponse

from lib.helper import GOOGLE_AI_STUDIO_KEY, GROQ_API_KEY, HF_TOKEN

# https://litellm.vercel.app/docs/providers/gemini
os.environ['GEMINI_API_KEY'] = GOOGLE_AI_STUDIO_KEY
os.environ["HUGGINGFACE_API_KEY"] = HF_TOKEN
os.environ["GROQ_API_KEY"] = GROQ_API_KEY

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

BACKEND_OPTIONS = {
    "Gemini": {
        "model": "gemini/gemini-pro",
        "kwargs": {
            "temperature": 0.0,
            "safety_settings": DEFAULT_GEMINI_SAFETY_SETTINGS
        }
    },
    "Llama3-8B (via HuggingFace) (NOT SUPPORTED YET)": {
        "model": "huggingface/unsloth/llama-3-8b",
        "kwargs": {
            "api_base": "https://api-inference.huggingface.co/models/unsloth/llama-3-8b"
        }
    },
    "Mixtral 8x22B (via HuggingFace)": {
        "model": "huggingface/mistralai/Mixtral-8x22B-v0.1",
        "kwargs": {
            "api_base": "https://api-inference.huggingface.co/models/mistralai/Mixtral-8x22B-v0.1"
        }
    },
    "Llama3-8b (via Groq)": {
        "model": "groq/llama3-8b-8192",
        "kwargs": {
            "temperature": 0.0,
            "response_format": {"type": "json_object"},  # https://console.groq.com/docs/text-chat#json-mode
        }
    },
    "Llama3-70b (via Groq)": {
        "model": "groq/llama3-70b-8192",
        "kwargs": {
            "temperature": 0.0,
            "response_format": {"type": "json_object"},  # https://console.groq.com/docs/text-chat#json-mode
        }
    },
}


# https://litellm.vercel.app/docs/completion/input
def run_query(
    prompt: str, role: str = "user", model_name: str = "Gemini"
) -> str:
    """Runs a query to an LLM model and returns the response."""
    model_dict = BACKEND_OPTIONS[model_name]
    model_params = model_dict.copy()
    kwargs = {
        "model": model_params.pop("model"),
        "messages": [{"role": role, "content": prompt}],
        **model_params["kwargs"]
    }
    response: ModelResponse = completion(**kwargs)
    content: str = (
        response.get('choices', [{}])[0].get('message', {}).get('content')
    )
    return content


if __name__ == "__main__":
    response = completion(
        model="gemini/gemini-pro",
        messages=[
            {
                "role": "user",
                "content": "write code for saying hi from LiteLLM"
            }
        ],
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
        ]
    )
    print(response)
