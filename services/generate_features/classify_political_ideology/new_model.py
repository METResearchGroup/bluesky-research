"""Use GPT3.5 in order to pass inference in batches."""
from transformers import AutoTokenizer, AutoModelForCausalLM

from lib.helper import track_function_runtime


base_prompt = """
Pretend that you are a political ideology classifier. Please classify the \
texts denoted in <text> as either left-leaning, moderate, right-leaning, \
or unclear. Assume that you are given only text that has already been \
classified to be political in nature. Only provide the label \
(“left-leaning”, “moderate”, “right-leaning”, “unclear”) in your \
response, no justification is necessary. Each text will be enumerated.

<text>
{text}
"""

MAX_TOKEN_LENGTH = 100


tokenizer = AutoTokenizer.from_pretrained("google/gemma-7b")
model = AutoModelForCausalLM.from_pretrained("google/gemma-7b")

input_text = "what you know about the universe"
input_ids = tokenizer(input_text, return_tensors="pt")

outputs = model.generate(**input_ids, max_length=512)
print(tokenizer.decode(outputs[0]))


def generate_text_batches(texts: list[str]) -> list[list[str]]:
    """Generate text batches for model.
    
    Each entry in a batch will be enumerated.
    """
    return [
        ["1. <text>\n2. <text>\n3. <text>\n4. <text>"],
        ["1. <text>\n2. <text>\n3. <text>\n4. <text>"],
        ["1. <text>\n2. <text>\n3. <text>\n4. <text>"],
    ]


def generate_query(batch: list[str]) -> str:
    """Generate a query for the batch."""
    return base_prompt.format(text=batch)


def generate_queries(texts: list[str]) -> list[str]:
    """Generate queries for the model."""
    batches = generate_text_batches(texts)
    return [generate_query(batch) for batch in batches]


def run_query(query: str) -> list[str]:
    """Runs query and returns the result."""
    pass


@track_function_runtime
def get_classifications(posts: list[dict]) -> list[dict]:
    texts: list[str] = [post["text"] for post in posts]
    queries: list[str] = generate_queries(texts)
    results = []
    for query in queries:
        labels = run_query(query)
        results.append(labels)
    post_to_label_map = [
        {"uri": post["uri"], "label": label}
        for post, label in zip(posts, results)
    ]
    return post_to_label_map


if __name__ == "__main__":
    posts = [
        {"uri": "post1", "text": "I think that the government should provide more social services."},
        {"uri": "post2", "text": "“Is it technically fascism?” “Is it technically rape?” “Is it technically genocide?” Remarkable the smug ease with which so many people have replaced moral inquiries with semantic ones."},
        {"uri": "post3", "text": "I've reached out to the offices of all 22 Democrats who voted to censure Rep. Rashida Tlaib because of her comments on Israel/Palestine to see if they planned to do the same for her Republican colleague from Michigan."},
        {"uri": "post4", "text": "This has been the fundamental problem the political press has had with Trump. They've been conditioned to believe it's only a scandal if it's a secret that's been exposed, so all the crimes that Trump does brazenly out in the open — bragging about them on camera even — somehow don't count."},
        {"uri": "post5", "text": "I think that the government should provide more social services."},
    ]
    classifications: list[dict] = get_classifications(posts)
