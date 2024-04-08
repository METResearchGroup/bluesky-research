"""Classify if a post has any NSFW content."""
from services.filter_raw_data.classify_nsfw_content.helper import (
    classify_nsfw_of_posts
)


def main(posts: list[dict]) -> None:
    return classify_nsfw_of_posts(posts=posts)


if __name__ == "__main__":
    main()
