"""Helper tooling for managing Bluesky feeds."""

from atproto_client.models.app.bsky.feed.defs import SkeletonFeedPost


def create_skeleton_feed_post(uri: str) -> SkeletonFeedPost:
    return SkeletonFeedPost(uri=uri)


def create_feed_generator(list_uris: list[str]) -> list[SkeletonFeedPost]:
    return [create_skeleton_feed_post(uri) for uri in list_uris]


def get_latest_feed_for_user(user_did: str) -> list[SkeletonFeedPost]:
    """Gets the latest feed for a user."""
    pass
