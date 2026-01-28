"""Reproduce prompt interpolation showing `<built-in function input>`.

The intergroup prompt templates use Python `.format(...)` placeholders (e.g. `{input}`)
and `ml_tooling.llm.prompt_utils.generate_batch_prompts()` fills those placeholders
from `PostToLabelModel.text`.

If `text` is accidentally set to the builtin `input` function (e.g. `text=input` instead
of `text=input()`), Python stringifies it as `<built-in function input>`, and the prompt
will contain:

    Post: <built-in function input>
"""

from __future__ import annotations

from dataclasses import dataclass

from ml_tooling.llm.prompt_utils import generate_batch_prompts
from prompts import (
    INTERGROUP_PROMPT_1, INTERGROUP_PROMPT_2,
)
from services.ml_inference.models import PostToLabelModel


TEST_POSTS = [
    "Customers are upset because the management changed the return policy.",
    "She was frustrated after missing her bus.",
    "People in City A say City B always cheats during football tournaments.",
    "Members of my hiking club disagreed on where to set up camp.",
    "Why do older employees ignore what the younger staff suggest?",
    "A new bakery opened across from the old one.",
    "Several men argued loudly outside the bar.",
]


def _extract_post_line(prompt: str) -> str:
    """Return the first line starting with 'Post:' for quick inspection."""
    for line in prompt.splitlines():
        if line.startswith("Post:"):
            return line
    return "<NO Post: LINE FOUND>"


def _make_post_to_label(text_value) -> PostToLabelModel:
    """Create a PostToLabelModel with minimal required fields."""
    return PostToLabelModel(
        uri="at://did:plc:example/app.bsky.feed.post/3example",
        text=text_value,
        preprocessing_timestamp="2026-01-28-00:00:00",
        batch_id=1,
        batch_metadata="{}",
    )


@dataclass(frozen=True)
class _Row:
    """Non-Pydantic repro row (mirrors the prompt_utils access pattern)."""

    text: object


def main() -> None:
    # Control: expected behavior with actual text content.
    good_post = _make_post_to_label(TEST_POSTS[0])

    # Bug: accidental `text=input` (builtin function object) instead of a string.
    bad_post = _make_post_to_label(input)

    # Also show it reproduces even without Pydantic coercion.
    bad_row = _Row(text=input)

    prompts_1_good = generate_batch_prompts(
        batch=[good_post],
        prompt_template=INTERGROUP_PROMPT_1,
        template_variable_to_model_field_mapping={"input": "text"},
    )
    prompts_1_bad = generate_batch_prompts(
        batch=[bad_post],
        prompt_template=INTERGROUP_PROMPT_1,
        template_variable_to_model_field_mapping={"input": "text"},
    )
    prompts_1_bad_row = generate_batch_prompts(
        batch=[bad_row],
        prompt_template=INTERGROUP_PROMPT_1,
        template_variable_to_model_field_mapping={"input": "text"},
    )

    prompts_2_good = generate_batch_prompts(
        batch=[good_post],
        prompt_template=INTERGROUP_PROMPT_2,
        template_variable_to_model_field_mapping={"prompt_input": "text"},
    )
    prompts_2_bad = generate_batch_prompts(
        batch=[bad_post],
        prompt_template=INTERGROUP_PROMPT_2,
        template_variable_to_model_field_mapping={"prompt_input": "text"},
    )

    print("=== Prompt 1 (template var: {input}) ===")
    print("good:", _extract_post_line(prompts_1_good[0]))
    print("bad (PostToLabelModel text=input):", _extract_post_line(prompts_1_bad[0]))
    print("bad (plain dataclass text=input):", _extract_post_line(prompts_1_bad_row[0]))
    print()

    print("=== Prompt 2 (template var: {prompt_input}) ===")
    print("good:", _extract_post_line(prompts_2_good[0]))
    print("bad (PostToLabelModel text=input):", _extract_post_line(prompts_2_bad[0]))


if __name__ == "__main__":
    main()

