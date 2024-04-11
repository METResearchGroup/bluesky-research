"""Model for classifying political ideology.

For now, we'll use the Gemma7B instruct model.
- https://huggingface.co/blog/gemma
- https://huggingface.co/google/gemma-7b

To run only on posts that are assumed to be civic.
"""
from typing import Optional

from transformers import AutoTokenizer, pipeline
import torch

from lib.helper import track_function_runtime

model = "google/gemma-7b-it"

tokenizer = AutoTokenizer.from_pretrained(model)
pipeline = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    model_kwargs={"torch_dtype": torch.bfloat16},
    # temperature=0.0, # want deterministic results
    max_new_tokens=100
    #device="cuda",
)

base_prompt = """
Pretend that you are a political ideology classifier. Please classify the \
texts denoted in <text> as either left-leaning, moderate, right-leaning, \
or unclear. Assume that you are given only text that has already been \
classified to be political in nature. Only provide the label \
(“left-leaning”, “moderate”, “right-leaning”, “unclear”) in your \
response, no justification is necessary.

<text>
{text}
"""

DEFAULT_BATCH_SIZE = 1


@track_function_runtime
def classify_post_texts(
    post_texts: list[str],
    batch_size: Optional[int]=DEFAULT_BATCH_SIZE
) -> list[str]:
    """Classify a batch of posts."""
    results = []
    for i in range(0, len(post_texts), batch_size):
        batch_prompts = [
            base_prompt.format(text=post_text) for post_text in post_texts[i:i+batch_size]
        ]
        batch_results = pipeline(batch_prompts)
        results.extend(batch_results)
    return results


def classify_posts(posts: list[dict]) -> list[dict]:
    """Classify a batch of posts."""
    post_texts = [post["text"] for post in posts]
    labels = classify_post_texts(post_texts)
    return [
        {"uri": post["uri"], "label_political_ideology": label}
        for post, label in zip(posts, labels)
    ]


if __name__ == "__main__":
    post_texts = [
        "I think that the government should provide more social services.",
        "“Is it technically fascism?” “Is it technically rape?” “Is it technically genocide?” Remarkable the smug ease with which so many people have replaced moral inquiries with semantic ones.",
        "I've reached out to the offices of all 22 Democrats who voted to censure Rep. Rashida Tlaib because of her comments on Israel/Palestine to see if they planned to do the same for her Republican colleague from Michigan.",
        "This has been the fundamental problem the political press has had with Trump. They've been conditioned to believe it's only a scandal if it's a secret that's been exposed, so all the crimes that Trump does brazenly out in the open — bragging about them on camera even — somehow don't count."
    ]
    classifications: list[dict] = classify_post_texts(post_texts)
    for classification in classifications:
        print('-' * 10)
        print(classification)
