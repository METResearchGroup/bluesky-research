"""Experimental script to verify prompt interpolation works correctly.

This script:
1. Creates a list of test posts
2. Generates prompts for each post using the IntergroupClassifier
3. Prints each prompt
4. Verifies that each post's text appears in its corresponding prompt
"""

from __future__ import annotations

from services.ml_inference.intergroup.classifier import IntergroupClassifier
from services.ml_inference.models import PostToLabelModel


# List of test posts covering various scenarios
TEST_POSTS: list[str] = [
    "Students from two rival schools teamed up for a community project.",
    "My friend forgot her umbrella at work.",
    "Protesters and police clashed downtown yesterday.",
    "Employees in marketing held a meeting without inviting the IT staff.",
    "Why do coffee drinkers and tea lovers argue so much online?",
    "A new dog park opened in my neighborhood.",
    "Three roommates debated about chores last night.",
    "The debate between Democrats and Republicans continues.",
    "I went to the store to buy groceries.",
    "Fans of Team A and Team B are arguing about the game.",
]


def _make_post_to_label(text: str, index: int) -> PostToLabelModel:
    """Create a PostToLabelModel with minimal required fields."""
    return PostToLabelModel(
        uri=f"at://did:plc:example/app.bsky.feed.post/{index}",
        text=text,
        preprocessing_timestamp="2026-01-28-00:00:00",
        batch_id=1,
        batch_metadata="{}",
    )


def main() -> None:
    """Main function to run the experiment."""
    # Create PostToLabelModel instances
    posts = [
        _make_post_to_label(text, index=i) for i, text in enumerate(TEST_POSTS, start=1)
    ]

    # Create classifier and generate prompts
    classifier = IntergroupClassifier()
    prompts = classifier._generate_batch_prompts(batch=posts)

    # Print results
    print("=" * 80)
    print("PROMPT INTERPOLATION VERIFICATION EXPERIMENT")
    print("=" * 80)
    print(f"\nTotal posts: {len(posts)}")
    print(f"Total prompts generated: {len(prompts)}\n")

    # Verify each prompt contains its corresponding post text
    all_passed = True
    for i, (post, prompt) in enumerate(zip(posts, prompts), start=1):
        print("-" * 80)
        print(f"POST {i}/{len(posts)}")
        print("-" * 80)
        print(f"Post text: {post.text!r}")
        print(f"\nGenerated prompt (last 150 chars):")
        print(repr(prompt[-150:]))
        print(f"\nFull prompt:")
        print(prompt)

        # Check if post text is in prompt
        post_in_prompt = post.text in prompt
        print(f"\n✓ Post text in prompt: {post_in_prompt}")
        
        if not post_in_prompt:
            all_passed = False
            print(f"  ✗ ERROR: Post text not found in prompt!")
        
        # Additional checks
        has_post_label = "Post:" in prompt
        has_answer_label = "Answer:" in prompt
        has_system_prompt = "You are a helpful assistant" in prompt
        has_examples = "## Examples" in prompt
        
        print(f"✓ Has 'Post:' label: {has_post_label}")
        print(f"✓ Has 'Answer:' label: {has_answer_label}")
        print(f"✓ Has system prompt: {has_system_prompt}")
        print(f"✓ Has examples section: {has_examples}")
        
        if not all([has_post_label, has_answer_label, has_system_prompt, has_examples]):
            all_passed = False
        
        print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    if all_passed:
        print("✓ All checks passed! Prompt interpolation is working correctly.")
    else:
        print("✗ Some checks failed. Please review the output above.")
    print("=" * 80)


if __name__ == "__main__":
    main()
