"""Helper tooling for Perspective API classification.

Doesn't include ML-specific tooling, which lives in the `ml_tooling` directory.
"""
from typing import Literal

from services.ml_inference.perspective_api.load_data import load_cached_post_uris  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

cached_post_uris: dict = load_cached_post_uris()


def filter_posts_already_in_cache(
    valid_posts: list[FilteredPreprocessedPostModel],
    invalid_posts: list[FilteredPreprocessedPostModel],
    source_feed: Literal["firehose", "most_liked"]
) -> dict[str, list[FilteredPreprocessedPostModel]]:
    """Filters out posts that already are in the existing classified posts
    cache. This can happen if, for example, a classification job is
    interrupted and then restarted.
    """
    feed_uri_cache = cached_post_uris[source_feed]
    valid_posts = [
        post for post in valid_posts if post.uri not in feed_uri_cache["valid"]
    ]
    invalid_posts = [
        post for post in invalid_posts if post.uri not in feed_uri_cache["invalid"]  # noqa
    ]
    return {"valid_posts": valid_posts, "invalid_posts": invalid_posts}
