"""Classify if a post has any NSFW content."""
from services.filter_raw_data.classify_nsfw_content.helper import (
    classify_if_posts_have_no_nsfw_content
)


def filter_posts_have_no_nsfw_content(posts: list[dict]) -> list[dict]:
    return classify_if_posts_have_no_nsfw_content(posts=posts)


if __name__ == "__main__":
    filter_posts_have_no_nsfw_content()
