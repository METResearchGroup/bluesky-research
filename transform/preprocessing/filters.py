"""Feed filtering functions."""
from datetime import timedelta

from lib.constants import (
    BLOCKED_AUTHORS, NUM_DAYS_POST_RECENCY, current_datetime
)
from lib.utils import parse_datetime_string



# TODO: better as a feature, move to non-ML feature generation
def is_in_network(post: dict) -> bool:
    """Determines if a post is within a network."""
    return True


# TODO: better as a feature, move to non-ML feature generation
def is_within_similar_networks(post: dict) -> bool:
    """Determines if a post is within a similar network or community.
    
    Inspired by https://blog.twitter.com/engineering/en_us/topics/open-source/2023/twitter-recommendation-algorithm
    """ # noqa
    return True


# TODO: uncomment recency filter
# TODO: we should have this filter for posts that we're considering serving,
# not necessarily posts that we are storing. We can use older posts for
# model training.
def post_is_recent(post_dict) -> bool:
    """Determines if a post is recent."""
    date_object = parse_datetime_string(post_dict["created_at"])
    time_difference = current_datetime - date_object
    time_threshold = timedelta(days=NUM_DAYS_POST_RECENCY)
    #if time_difference > time_threshold:
    #    return False
    return True
