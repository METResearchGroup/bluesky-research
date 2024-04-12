"""Service for classifying the language of posts."""
from services.preprocess_raw_data.classify_language.helper import (
    classify_language_of_posts
)


def filter_text_is_english(posts: list[dict]) -> list[dict]:
    return classify_language_of_posts(posts=posts)


if __name__ == "__main__":
    filter_text_is_english()
