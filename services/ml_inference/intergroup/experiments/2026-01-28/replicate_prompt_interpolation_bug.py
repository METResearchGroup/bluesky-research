"""Print full prompts for intergroup Prompt v1 vs v2.

This is intentionally simple: for each test post, print the fully rendered prompt for:
- Prompt version 1 (`INTERGROUP_PROMPT_1`, placeholder `{input}`)
- Prompt version 2 (`INTERGROUP_PROMPT_2`, placeholder `{prompt_input}`)
"""

from __future__ import annotations

from ml_tooling.llm.prompt_utils import generate_batch_prompts
from prompts import (
    INTERGROUP_PROMPT_1, INTERGROUP_PROMPT_2,
)
from services.ml_inference.models import PostToLabelModel


TEST_POSTS: list[str] = [
    "Students from two rival schools teamed up for a community project.",
    "My friend forgot her umbrella at work.",
    "Protesters and police clashed downtown yesterday.",
    "Employees in marketing held a meeting without inviting the IT staff.",
    "Why do coffee drinkers and tea lovers argue so much online?",
    "A new dog park opened in my neighborhood.",
    "Three roommates debated about chores last night.",
]


def _make_post_to_label(text_value) -> PostToLabelModel:
    """Create a PostToLabelModel with minimal required fields."""
    return PostToLabelModel(
        uri="at://did:plc:example/app.bsky.feed.post/3example",
        text=text_value,
        preprocessing_timestamp="2026-01-28-00:00:00",
        batch_id=1,
        batch_metadata="{}",
    )


def main() -> None:
    posts = [_make_post_to_label(t) for t in TEST_POSTS]

    prompts_v1 = generate_batch_prompts(
        batch=posts,
        prompt_template=INTERGROUP_PROMPT_1,
        template_variable_to_model_field_mapping={"input": "text"},
    )
    prompts_v2 = generate_batch_prompts(
        batch=posts,
        prompt_template=INTERGROUP_PROMPT_2,
        template_variable_to_model_field_mapping={"prompt_input": "text"},
    )

    print("===== PROMPT VERSION 1 =====")
    for i, prompt in enumerate(prompts_v1, start=1):
        print(f"\n----- POST {i}/{len(prompts_v1)} -----")
        print(prompt)

    print("\n\n===== PROMPT VERSION 2 =====")
    for i, prompt in enumerate(prompts_v2, start=1):
        print(f"\n----- POST {i}/{len(prompts_v2)} -----")
        print(prompt)


if __name__ == "__main__":
    main()

