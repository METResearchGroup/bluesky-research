"""Classify posts using the Perspective API from Google Jigsaw"""
from services.generate_features.classify_perspective_api.helper import (
    classify_posts_using_perspective_api
)


def main(posts: list[dict]) -> list[dict]:
    return classify_posts_using_perspective_api(posts=posts)


if __name__ == "__main__":
    main()
