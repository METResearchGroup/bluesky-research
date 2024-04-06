"""Service for classifying the language of posts."""
from services.filter_raw_data.classify_language.helper import (
    classify_language_of_posts
)


def main(posts: list[dict]) -> None:
    return classify_language_of_posts(posts=posts)


if __name__ == "__main__":
    main()
