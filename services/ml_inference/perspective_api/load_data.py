"""Loads data for Perspective API classification."""
from typing import Literal

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


def load_previously_classified_post_uris() -> set[str]:
    return set()


# TODO: load from DynamoDB.
def load_previous_session_metadata() -> dict:
    """Loads the metadata from the previous classification session."""
    pass


def load_cached_post_uris() -> dict:
    """Returns the URIs of posts that have already been classified by the
    Perspective API. This is useful for filtering out posts that have already
    been classified but haven't been exported to an external store yet, so that
    we don't classify them again.

    Returns a tuple of two sets, one for the valid posts and one for the
    invalid posts.
    """
    return {
        "firehose": {
            "valid": set(),
            "invalid": set()
        },
        "most_liked": {
            "valid": set(),
            "invalid": set()
        }
    }


def load_posts_to_classify(
    source: Literal["local", "s3"],
    source_feed: Literal["firehose", "most_liked"]
) -> list[FilteredPreprocessedPostModel]:
    """Loads posts for Perspective API classification. Loads posts which
    were added after the most recent batch of classified posts.

    Loads the timestamp of the latest preprocessed batch that was classified,
    so that any posts preprocessed after that batch can be classified.
    """
    # TODO: load posts from either s3 or local
    # TODO: load both firehose and most liked posts.
    posts = []
    sorted_posts = sorted(posts, key=lambda x: x.synctimestamp, reverse=False)
    print(f"Number of posts loaded for classification using Perspective API: {len(sorted_posts)}")  # noqa
    return sorted_posts
