"""Basic interface for doing LLM-based inference."""
import os

from litellm import completion
from litellm.utils import ModelResponse

from lib.helper import GOOGLE_AI_STUDIO_KEY

# https://litellm.vercel.app/docs/providers/gemini
os.environ['GEMINI_API_KEY'] = GOOGLE_AI_STUDIO_KEY


# https://litellm.vercel.app/docs/completion/input
def run_query(
    prompt: str, role: str = "user", model: str = "gemini/gemini-pro"
) -> str:
    """Runs a query to an LLM model and returns the response."""
    response: ModelResponse = completion(
        model=model,
        messages=[{"role": role, "content": prompt}],
        temperature=0.0,  # we want determinism where possible
        # we want to include harmful content since we're using this for
        # toxicity classification
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
