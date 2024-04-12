"""Classify if a post has spam."""
from services.preprocess_raw_data.classify_spam.helper import (
    classify_if_posts_have_no_spam
)


def filter_posts_have_no_spam(posts: list[dict]) -> list[dict]:
    return classify_if_posts_have_no_spam(posts=posts)


if __name__ == "__main__":
    filter_posts_have_no_spam()
