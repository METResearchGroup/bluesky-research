"""Helper functionalities for creating feeds."""

from lib.db.sql.participant_data_database import get_user_to_bluesky_profiles
from services.create_feeds.create_ranked_feeds import create_feeds_per_condition
from lib.db.sql.created_feeds_database import batch_insert_created_feeds
from services.create_feeds.models import CreatedFeedModel, UserFeedModel
from services.create_feeds.postprocess_feeds import (
    convert_feeds_to_bluesky_format,
    postprocess_feeds,
)
from services.participant_data.models import UserToBlueskyProfileModel


def write_feeds_to_database(feeds: list[CreatedFeedModel]):
    """Writes latest feed recommendation to database."""
    batch_insert_created_feeds(feeds=feeds)


def write_feeds_to_cache(feeds: list[CreatedFeedModel]):
    """Writes latest feed recommendations to cache."""
    pass


def write_latest_feeds(feeds: list[CreatedFeedModel]):
    """Writes latest feeds, to both database and cache."""
    write_feeds_to_database(feeds)
    write_feeds_to_cache(feeds)


def create_latest_feeds():
    """Creates the latest feeds."""
    users: list[UserToBlueskyProfileModel] = get_user_to_bluesky_profiles()
    condition_to_user_map: dict[str, list[UserToBlueskyProfileModel]] = {}

    condition_to_user_map["reverse_chronological"] = []
    condition_to_user_map["engagement"] = []
    condition_to_user_map["representative_diversification"] = []

    for user in users:
        condition_to_user_map[user.condition].append(user)

    feeds: list[UserFeedModel] = create_feeds_per_condition(
        condition_to_user_map=condition_to_user_map
    )
    postprocessed_feeds: list[UserFeedModel] = postprocess_feeds(feeds)
    user_bsky_feeds: list[CreatedFeedModel] = convert_feeds_to_bluesky_format(
        postprocessed_feeds
    )
    write_latest_feeds(user_bsky_feeds)


if __name__ == "__main__":
    create_latest_feeds()
