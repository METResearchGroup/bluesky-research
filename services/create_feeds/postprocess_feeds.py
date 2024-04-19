"""Runs any postprocessing steps for feeds.

This is the final step before writing the feeds to the database and cache, so
we want this format to conform with the format expected by the Bluesky API.
"""
from lib.constants import current_datetime

from services.create_feeds.models import CreatedFeedModel


def postprocess_feed(user: str, feed: list[dict]) -> list[dict]:
    """Postprocesses a feed for a given user.
    
    Currently stubbed as a placeholder, but we will likely want this in the
    future.
    """
    return feed


def postprocess_feeds(user_to_feed_dict: dict) -> dict:
    """Postprocesses a feed for a given user."""
    return {
        user: postprocess_feed(user, feed)
        for (user, feed) in user_to_feed_dict.items()
    }


def convert_feeds_to_bluesky_format(user_to_feed_dict: dict) -> list[dict]:
    """Converts feeds to Bluesky format.

    Relevant Bluesky resources:
    - https://github.com/bluesky-social/feed-generator
    - https://github.com/MarshalX/bluesky-feed-generator
    - https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/app.py#L67
    - https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/algos/whats_alf.py#L28
    - https://docs.bsky.app/docs/starter-templates/custom-feeds
    - https://bsky.social/about/blog/7-27-2023-custom-feeds

    A feed is, simply put, a list of post URIs:
    - https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/algos/whats_alf.py#L28

    We take as input a dictionary whose keys are user IDs and whose values are
    the post dictionary objects. We only want the URIs.

    We expect to return to the API a list of SkeletonFeedPost items. The lexicon
    is defined in:
    - https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/defs.json#L160

    Essentially, we need to return to Bluesky a list of JSONs in the following
    format: {"post": <post at-uri>}}

    We will hydrate this on the API side, but for now we just need to store the
    feed URIs for each user for this recommendation of feed generation.
    """
    user_feeds = []
    for user, feed in user_to_feed_dict.items():
        user_feed = {
            "bluesky_user_did": user,
            "feed_uris": ','.join([post["uri"] for post in feed]),
            "timestamp": current_datetime.strftime("%Y-%m-%d-%H:%M:%S")
        }
        CreatedFeedModel(**user_feed)
        user_feeds.append(user_feed)
    return user_feeds
