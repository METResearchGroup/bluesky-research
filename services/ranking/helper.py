"""Helper file that creates the skeleton feed post and feed generator,
as per https://github.com/bluesky-social/feed-generator#overview.
"""
from atproto_client.models.app.bsky.feed.defs import SkeletonFeedPost


def create_skeleton_feed_post(uri: str) -> SkeletonFeedPost:
    return SkeletonFeedPost(uri=uri)


def create_feed_generator(list_uris: list[str]) -> list[SkeletonFeedPost]:
    return [create_skeleton_feed_post(uri) for uri in list_uris]
