"""Basic interface for doing LLM-based inference."""

import os
import time

from litellm import acompletion, batch_completion, completion
from litellm.utils import ModelResponse
import tiktoken

from services.ml_inference.models import LLMSociopoliticalLabelsModel

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
            # https://docs.litellm.ai/docs/completion/json_mode#pass-in-json_schema
            # "response_format": {
            #     "type": "json_schema",
            #     # if this doesn't work for a list of samples,
            #     # can try the LLMSociopoliticalLabelsModel
            #     "json_schema": LLMSociopoliticalLabelModel.model_json_schema(),
            #     "strict": True,
            # },
            "response_format": LLMSociopoliticalLabelsModel,
        },
    },
}

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


@track_performance
def run_batch_queries(
    prompts: list[str],
    model_name: str,
    role: str = "user",
    num_retries: int = 2,
) -> list[str]:
    model_dict = BACKEND_OPTIONS[model_name]
    model_params = model_dict.copy()
    kwargs = {
        "model": model_params.pop("model"),
        "messages": [[{"role": role, "content": prompt}] for prompt in prompts],
        "num_retries": num_retries,
        **model_params["kwargs"],
    }
    responses: list[ModelResponse] = batch_completion(**kwargs)
    contents = []
    for response in responses:
        content: str = (
            response.get("choices", [{}])[0].get("message", {}).get("content")
        )
        contents.append(content)
    return contents


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
    # 50 sample posts.
    sample_posts = [
        "The government should provide free healthcare for all citizens.",
        "We need to take stronger action against climate change to protect our planet.",
        "Raising the minimum wage is essential for ensuring a living wage for all workers.",
        "Gun control laws need to be stricter to prevent mass shootings.",
        "Social justice movements are crucial for achieving equality in our society.",
        "Education should be funded by the government to ensure access for everyone.",
        "We must protect the rights of marginalized communities in our legislation.",
        "The wealth gap in America is a serious issue that needs to be addressed.",
        "Universal basic income could help alleviate poverty in our country.",
        "Voting rights should be expanded to ensure everyone can participate in democracy.",
        "Lowering taxes will help stimulate the economy and create jobs.",
        "We need to secure our borders and enforce immigration laws more strictly.",
        "The Second Amendment is vital for protecting our personal freedoms.",
        "Deregulation is necessary to allow businesses to thrive and innovate.",
        "Government spending should be reduced to lower the national debt.",
        "Traditional family values are important for a stable society.",
        "We should prioritize American jobs and industries over foreign interests.",
        "School choice allows parents to decide the best education for their children.",
        "Energy independence is crucial for national security and economic growth.",
        "The free market is the best way to drive innovation and prosperity.",
        "Patriotism and national pride should be encouraged in our schools.",
        "Affordable housing is a fundamental right that should be guaranteed.",
        "Mental health services need to be more accessible to everyone.",
        "The education system should focus on critical thinking and creativity.",
        "Public transportation should be improved to reduce traffic congestion.",
        "We must take action to protect endangered species and their habitats.",
        "Corporate tax rates should be adjusted to ensure fair contributions.",
        "Civic education is essential for fostering informed citizens.",
        "The digital divide must be addressed to ensure equal access to technology.",
        "Renewable energy sources should be prioritized for a sustainable future.",
        "Criminal justice reform is necessary to ensure fairness and equity.",
        "We need to invest in infrastructure to support economic growth.",
        "Access to clean water is a basic human right that must be protected.",
        "The arts and humanities should receive more funding in education.",
        "Childcare should be affordable and accessible for all families.",
        "We must combat misinformation to protect democracy.",
        "International cooperation is vital for addressing global challenges.",
        "The gig economy should provide fair wages and benefits for workers.",
        "Animal rights should be protected through stronger legislation.",
        "We need to promote diversity and inclusion in all sectors.",
        "Public health initiatives should focus on prevention and education.",
        "The minimum wage should be a living wage for all workers.",
        "We must ensure that all voices are heard in the political process.",
        "Climate change policies should be based on scientific evidence.",
        "We need to support small businesses to foster local economies.",
        "Voting should be made easier through accessible registration and polling.",
        "The government should invest in research and development for innovation.",
        "We must protect the rights of workers to organize and bargain collectively.",
        "Access to quality education should not depend on socioeconomic status.",
        "We need to address systemic racism in all areas of society.",
        "Public spaces should be designed to be inclusive and accessible.",
        "We must prioritize mental health awareness and destigmatization.",
        "The role of technology in society should be critically examined.",
        "We need to ensure that healthcare is a right, not a privilege.",
        "Environmental justice must be a priority in policy-making.",
        "We should encourage civic engagement among young people.",
        "The impact of social media on society needs to be studied and addressed.",
    ]

    def generate_prompt(posts: list[str]) -> str:
        enumerated_texts = ""
        for i, post in enumerate(posts):
            enumerated_texts += f"{i+1}. {post}\n"
        prompt = f"""
You are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "left", "right", or 'unclear'. \
If the text is not sociopolitical, return "unclear". Base your response on US politics.\

Think through your response step by step.

Do NOT include any explanation. Only return the JSON output.

TEXT:
```
{enumerated_texts}
```
"""
        return prompt

    # create batches of 10 posts.
    prompts: list[str] = []
    batch_size = 10
    for i in range(0, len(sample_posts), batch_size):
        batch_posts = sample_posts[i : i + batch_size]
        prompt = generate_prompt(batch_posts)
        prompts.append(prompt)

    model_name = "GPT-4o mini"

    print(
        f"Running {model_name} with {len(prompts)} prompts, split into 5 batches of 10."
    )  # noqa

    ## For-loop run ##
    time_before = time.time()
    for prompt in prompts:
        run_query(prompt, role="user", model_name=model_name)
    time_after = time.time()
    print(f"Time taken with for-loop: {round(time_after - time_before)} seconds")

    ## Batch run ##
    time_before_batch = time.time()
    run_batch_queries(prompts, role="user", model_name=model_name)
    time_after_batch = time.time()
    print(
        f"Time taken for batch: {round(time_after_batch - time_before_batch, 2)} seconds"
    )
