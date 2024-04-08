"""Classify if a post has spam."""
from services.filter_raw_data.classify_spam.helper import (
    classify_if_posts_have_spam
)


def filter_posts_with_spam(posts: list[dict]) -> list[dict]:
    return classify_if_posts_have_spam(posts=posts)


if __name__ == "__main__":
    filter_posts_with_spam()
