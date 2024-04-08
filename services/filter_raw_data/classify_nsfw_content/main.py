"""Classify if a post has any NSFW content."""
from services.filter_raw_data.classify_nsfw_content.helper import (
    classify_nsfw_of_posts
)


def filter_posts_with_nsfw_content(posts: list[dict]) -> list[dict]:
    return classify_nsfw_of_posts(posts=posts)


if __name__ == "__main__":
    filter_posts_with_nsfw_content()
