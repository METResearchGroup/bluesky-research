"""Classify political ideology of posts."""
from services.generate_features.classify_political_ideology.helper import (
    classify_political_ideology_of_posts
)


def main(posts: list[dict]) -> list[dict]:
    return classify_political_ideology_of_posts(posts=posts)


if __name__ == "__main__":
    main()
