"""Non-ML feature generation."""
from datetime import timedelta

from lib.constants import NUM_DAYS_POST_RECENCY, current_datetime
from lib.utils import parse_datetime_string


def is_in_network(post: dict) -> bool:
    """Determines if a post is within a network."""
    return True


# TODO: better as a feature, move to non-ML feature generation
def is_within_similar_networks(post: dict) -> bool:
    """Determines if a post is within a similar network or community.

    Inspired by https://blog.twitter.com/engineering/en_us/topics/open-source/2023/twitter-recommendation-algorithm
    """  # noqa
    return True


def post_is_recent(post_dict) -> bool:
    """Determines if a post is recent."""
    date_object = parse_datetime_string(post_dict["created_at"])
    time_difference = current_datetime - date_object
    time_threshold = timedelta(days=NUM_DAYS_POST_RECENCY)
    if time_difference > time_threshold:
        return False
    return True


feature_funcs = [is_in_network, is_within_similar_networks, post_is_recent]


def generate_non_ml_features(post: dict) -> dict:
    """Generates non-ML features for a post."""
    res = {}
    res["uid"] = post["uid"]
    res["text"] = post["text"]
    for func in feature_funcs:
        res[func.__name__] = func(post)
    return res
