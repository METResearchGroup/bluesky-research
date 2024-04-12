from services.create_feeds.postprocess_feeds import postprocess_feeds
from services.create_feeds.rank_candidates import rank_latest_candidates


def validate_postprocessed_feeds(user_to_postprocessed_feed_dict: dict):
    """Validates postprocessed feeds using Great Expectations."""
    pass


def write_feeds_to_database(user_to_postprocessed_feed_dict: dict):
    """Writes latest feed recommendation to database."""
    pass


def write_feeds_to_cache(user_to_postprocessed_feed_dict: dict):
    """Writes latest feed recommendations to cache."""
    pass


def write_latest_feeds(postprocessed_feeds: dict):
    """Writes latest feeds, to both database and cache."""
    write_feeds_to_database(postprocessed_feeds)
    write_feeds_to_cache(postprocessed_feeds)


def create_latest_feeds():
    user_to_feed_dict: dict = rank_latest_candidates()
    user_to_postprocessed_feed_dict: dict = postprocess_feeds(user_to_feed_dict)
    validate_postprocessed_feeds(user_to_postprocessed_feed_dict)
    write_latest_feeds(user_to_postprocessed_feed_dict)

