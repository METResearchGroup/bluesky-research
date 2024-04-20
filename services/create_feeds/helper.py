"""Helper functionalities for creating feeds."""

from services.create_feeds.create_ranked_feeds import (
    create_ranked_feeds_per_user, create_only_reverse_chronological_feeds
)
from services.create_feeds.database import batch_write_created_feeds
from services.create_feeds.postprocess_feeds import (
    convert_feeds_to_bluesky_format, postprocess_feeds
)


def validate_postprocessed_feeds(user_to_postprocessed_feed_dict: dict):
    """Validates postprocessed feeds using Great Expectations."""
    pass


def write_feeds_to_database(user_bluesky_feeds: list[dict]):
    """Writes latest feed recommendation to database."""
    batch_write_created_feeds(user_bluesky_feeds)


def write_feeds_to_cache(user_bluesky_feeds: list[dict]):
    """Writes latest feed recommendations to cache."""
    pass


def write_latest_feeds(user_bluesky_feeds: list[dict]):
    """Writes latest feeds, to both database and cache."""
    write_feeds_to_database(user_bluesky_feeds)
    write_feeds_to_cache(user_bluesky_feeds)


def create_latest_feeds(reverse_chronological_only: bool = False):
    print("Creating latest feeds...")
    if reverse_chronological_only:
        print("Creating reverse-chronological feeds only.")
        user_to_feed_dict: dict = create_only_reverse_chronological_feeds()
    else:
        user_to_feed_dict: dict = create_ranked_feeds_per_user()
    user_to_postprocessed_feed_dict: dict = postprocess_feeds(
        user_to_feed_dict)
    validate_postprocessed_feeds(user_to_postprocessed_feed_dict)
    user_bluesky_feeds: list[dict] = convert_feeds_to_bluesky_format(
        user_to_postprocessed_feed_dict
    )
    print(f"Writing {len(user_bluesky_feeds)} feeds to database and cache.")
    write_latest_feeds(user_bluesky_feeds)


if __name__ == "__main__":
    create_latest_feeds(reverse_chronological_only=True)
