"""Helper functionalities for classifying records."""
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

MIN_TEXT_LENGTH = 8


def validate_posts(
    posts_to_classify: list[FilteredPreprocessedPostModel]
) -> tuple[list[FilteredPreprocessedPostModel], list[FilteredPreprocessedPostModel]]:  # noqa
    """Filter which posts should be sent to the Perspective API or not.

    For now, this'll just be a minimum character count. Mostly used as a simple
    filter for posts that don't have any words or might have 1-2 words.
    """
    valid_posts: list[FilteredPreprocessedPostModel] = []
    invalid_posts: list[FilteredPreprocessedPostModel] = []

    for post in posts_to_classify:
        if len(post.text) >= MIN_TEXT_LENGTH:
            valid_posts.append(post)
        else:
            invalid_posts.append(post)
    return (valid_posts, invalid_posts)
