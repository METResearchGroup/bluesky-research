"""Model for classifying political ideology.

For now, we'll use the Gemma7B instruct model.
- https://huggingface.co/blog/gemma
- https://huggingface.co/google/gemma-7b

To run only on posts that are assumed to be civic.
"""
from transformers import AutoTokenizer, pipeline
import torch

from lib.helper import track_function_runtime

model = "google/gemma-7b-it"

tokenizer = AutoTokenizer.from_pretrained(model)
pipeline = pipeline(
    "text-generation",
    model=model,
    model_kwargs={"torch_dtype": torch.bfloat16},
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



@track_function_runtime
def classify(post: dict) -> dict:
    prob = 0.0
    label = 0
    return {"prob": prob, "label": label}


@track_function_runtime
def classify_post_texts(post_texts: list[str]) -> list[dict]:
    """
    messages = [generate_message(post_text) for post_text in post_texts]
    prompt = pipeline.tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    outputs = pipeline(
        prompt,
        max_new_tokens=256,
        do_sample=True,
        temperature=0.7,
        top_k=50,
        top_p=0.95
    )
    res = [
        {"text": text, "label": output["generated_text"]}
        for (text, output) in zip(post_texts, outputs)
    ]
    return res
    """
    batch_prompts = [
        base_prompt.format(text=post_text) for post_text in post_texts
    ]
    results = pipeline(batch_prompts)
    breakpoint()
    return results

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
