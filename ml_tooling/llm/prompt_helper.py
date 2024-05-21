"""Helper tools for generating prompts.

Resource for all things prompting: https://github.com/dair-ai/Prompt-Engineering-Guide?tab=readme-ov-file
"""  # noqa
from pprint import pformat
from typing import Literal
import yaml

from ml_tooling.llm.task_prompts import task_name_to_task_prompt_map
from services.add_context.precompute_context.helper import get_or_create_post_and_context_json  # noqa
from transform.bluesky_helper import convert_post_link_to_post


base_prompt = """
{task_prompt}

Here is the post text that needs to be classified:
```
<text>
{post_text}
```

{context}
"""


def generate_context_from_post(
    post: dict,
    format: str = Literal["json", "yaml"],
    pformat_output: bool = True
) -> str:
    """Generates the context from the post."""
    json_context: dict = get_or_create_post_and_context_json(post=post)
    if format == "json":
        res: dict = json_context
        if pformat_output:
            res: str = pformat(res, width=200)
    elif format == "yaml":
        # yes having multiple typings is weird
        res: str = yaml.dump(json_context, sort_keys=False)
    else:
        raise ValueError("Unsupported format. Use 'json' or 'yaml'.")
    full_context = f"""
The following contains the post and its context:
```
{res}
```
"""
    return full_context


def generate_complete_prompt(
    post: dict,
    task_prompt: str,
    is_text_only: bool = False,
    format: str = Literal["json", "yaml"],
) -> str:
    """Given a task prompt and the details of the context, generate
    the resulting complete prompt.
    """
    post_text = post["text"]
    if is_text_only:
        full_context = ""
    else:
        full_context = generate_context_from_post(post=post, format=format)
    return base_prompt.format(
        task_prompt=task_prompt, post_text=post_text, context=full_context
    )


def generate_complete_prompt_for_given_post(
    post: dict,
    task_name: str,
    include_context: bool = True,
    format: str = Literal["json", "yaml"],
) -> str:
    """Generates a complete prompt for a given post."""
    is_text_only = not include_context
    return generate_complete_prompt(
        post=post,
        task_prompt=task_name_to_task_prompt_map[task_name],
        is_text_only=is_text_only,
        format=format
    )


def generate_complete_prompt_for_post_link(
    link: str,
    task_name: str,
    only_json_format: bool = False,
    include_context: bool = True
) -> str:
    """Generates a complete prompt for a given post link."""
    post: dict = convert_post_link_to_post(link)
    return generate_complete_prompt_for_given_post(
        post=post,
        task_name=task_name,
        only_json_format=only_json_format,
        include_context=include_context
    )


def generate_batched_post_prompt(posts: list[dict], task_name: str) -> str:
    """Create a prompt that classifies a batch of posts."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    task_prompt += """
You will receive a batch of posts to classify. The batch of posts is in a JSON
with the following fields: "posts": a list of posts, and "expected_number_of_posts":
the number of posts in the batch.

Each post will be its own JSON \
object including the text to classify (in the "text" field) and the context \
in which the post was made (in the "context" field). Return a list of JSONs \
for each post, in the format specified before. The length of the list of \
JSONs must match the value in the "expected_number_of_posts" field.

Return a JSON with the following format:
{
    "results": <list of JSONs, one for each post>,
    "count": <number of posts classified. Must match `expected_number_of_posts`>
}
"""  # noqa
    post_contexts = []
    for post in posts:
        post_contexts.append(get_or_create_post_and_context_json(post))
    batched_context = {
        "posts": post_contexts,
        "expected_number_of_posts": len(posts)
    }
    batched_context_str = pformat(batched_context, width=200)
    full_prompt = f"""
{task_prompt}

{batched_context_str}
    """
    print(f"Length of batched_context_str: {len(batched_context_str)}")
    print(f"Length of full prompt: {len(full_prompt)}")
    return full_prompt
