"""Classify if a post has hate speech."""
from services.preprocess_raw_data.classify_hate_speech.helper import (
    classify_if_posts_have_no_hate_speech
)


def filter_posts_have_no_hate_speech(posts: list[dict]) -> list[dict]:
    return classify_if_posts_have_no_hate_speech(posts=posts)


if __name__ == "__main__":
    filter_posts_have_no_hate_speech()