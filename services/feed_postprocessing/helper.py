"""Helper file that creates the skeleton feed post and feed generator,
as per https://github.com/bluesky-social/feed-generator#overview.
"""
import os

from lib.aws.s3 import FEED_KEY_ROOT, S3, USERS_KEY_ROOT
from services.manage_users.helper import load_all_user_dids

s3_client = S3()


def load_latest_recommendation_for_user(user_did: str) -> dict:
    """Loads the latest recommendation for a user from S3."""
    prefix = os.path.join(USERS_KEY_ROOT, user_did)
    keys: list[str] = s3_client.get_keys_given_prefix(prefix=prefix)
    latest_recommendation_key: str = max(keys)
    latest_timestamp: str = latest_recommendation_key.split("/")[-2]
    recommendation: list[dict] = s3_client.read_jsonl_from_s3(
        latest_recommendation_key
    )
    return {
        "latest_timestamp": latest_timestamp,
        "recommendation": recommendation
    }


def load_latest_recommendations() -> dict:
    """Loads the latest recommendations from S3.

    Returns a dictionary of user DID : recommendations, where the list
    of recommendations is the latest set of recommendations for that user.

    In the format: {
        "user_did": {
            "latest_timestamp": "2021-08-01T00:00:00Z",
            "recommendation": [{post1}, {post2}, {post3}]
        }
    }
    """
    user_dids: list[str] = load_all_user_dids()
    return {
        user_did: load_latest_recommendation_for_user(user_did)
        for user_did in user_dids
    }


def postprocess_recommendation(
    user_did: str, recommendations: list[dict]
) -> list[dict]:
    """Postprocesses the recommendations for a user.

    This can include operations such as:
    - Removing duplicate posts
    - Shuffling/re-ordering posts
    - Removing posts that have been seen by the user
    - Changing the composition of the feed (e.g., adding more posts from
    in-network, etc.)
    """
    return recommendations


def postprocess_feed_recommendations(
    latest_recommendation_by_user: dict
) -> dict:
    """Do postprocessing of feed recommendations.

    Some example operations include:
    - Removing duplicate posts
    - Shuffling/re-ordering posts
    - Removing posts that have been seen by the user
    - Changing the composition of the feed (e.g., adding more posts from
    in-network, etc.)
    """
    return {
        user_did: {
            "latest_timestamp": recommendations["latest_timestamp"],
            "recommendation": postprocess_recommendation(
                user_did, recommendations["recommendation"]
            )
        }
        for user_did, recommendations in latest_recommendation_by_user.items()
    }


def write_postprocessed_feeds_to_s3(
    postprocessed_feed_recommendations: dict
) -> None:
    """Takes the postprocessed feeds and writes them to S3.

    We just need to write the URIs to s3. The `manage_bluesky_feeds` will
    handle hydrating these URIs into the `SkeletonFeedPost` format requested
    by Bluesky.

    Saves feed in a JSON format, with the following structure:
    {
        "uris": ["uri1", "uri2", "uri3"]
    }
    """
    for user_did, recommendations in postprocessed_feed_recommendations.items():
        key = s3_client.create_partitioned_key(
            key_root=FEED_KEY_ROOT,
            userid=user_did,
            timestamp=recommendations["latest_timestamp"],
            filename="feed.json"
        )
        # for the recommendations, all we need are the uris
        uri_list = [post["uri"] for post in recommendations["recommendation"]]
        json_dict = {"uris": uri_list}
        s3_client.write_dict_json_to_s3(json_dict, key)


def do_feed_postprocessing() -> None:
    """Main function that processes the feed."""
    latest_recommendation_by_user: dict = load_latest_recommendations()
    postprocessed_feed_recommendations: dict = (
        postprocess_feed_recommendations(latest_recommendation_by_user)
    )
    write_postprocessed_feeds_to_s3(postprocessed_feed_recommendations)
