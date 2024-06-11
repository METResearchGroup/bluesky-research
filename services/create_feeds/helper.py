"""Helper functionalities for creating feeds."""
from services.create_feeds.create_ranked_feeds import (
    create_feeds_per_condition
)
from services.create_feeds.database import batch_write_created_feeds
from services.create_feeds.postprocess_feeds import (
    convert_feeds_to_bluesky_format, postprocess_feeds
)
from services.participant_data.helper import (
    get_user_to_bluesky_profiles_as_list_dicts
)


def write_feeds_to_database(feeds):
    """Writes latest feed recommendation to database."""
    batch_write_created_feeds(feeds)


def write_feeds_to_cache(feeds):
    """Writes latest feed recommendations to cache."""
    pass


def write_latest_feeds(feeds):
    """Writes latest feeds, to both database and cache."""
    write_feeds_to_database(feeds)
    write_feeds_to_cache(feeds)


def create_latest_feeds():
    users: list[dict] = get_user_to_bluesky_profiles_as_list_dicts()
    condition_to_user_map: dict[str, list] = {}

    condition_to_user_map["reverse_chronological"] = []
    condition_to_user_map["engagement"] = []
    condition_to_user_map["representative_diversification"] = []

    for user in users:
        condition = user["condition"]
        condition_to_user_map[condition].append(user)

    feeds = create_feeds_per_condition(condition_to_user_map)
    postprocessed_feeds = postprocess_feeds(feeds)
    user_bsky_feeds = convert_feeds_to_bluesky_format(postprocessed_feeds)
    write_latest_feeds(user_bsky_feeds)


if __name__ == "__main__":
    create_latest_feeds()
