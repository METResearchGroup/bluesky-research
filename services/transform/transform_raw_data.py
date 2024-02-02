"""Transforms raw data.

Based on https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.defs.json
"""
from typing import TypedDict

from atproto_client.models.app.bsky.actor.defs import ProfileViewBasic
from atproto_client.models.app.bsky.feed.defs import FeedViewPost, PostView
from atproto_client.models.dot_dict import DotDict


def get_post_author_info(post_author: ProfileViewBasic) -> dict:
    """Get author information from a post."""
    assert isinstance(post_author, ProfileViewBasic)
    return {
        "author_did": post_author["did"],
        "author_handle": post_author["handle"],
        "author_display_name": post_author["display_name"],
    }


def get_post_record_info(post_record: DotDict) -> dict:
    assert isinstance(post_record, DotDict)
    return {
        "created_at": post_record["created_at"],
        "text": post_record["text"],
        "langs": post_record["langs"], # needs to be ["en"]
    }


def flatten_post(post: PostView) -> dict:
    """Flattens the post, grabs engagement information, and other identifiers
    for a given post."""
    assert isinstance(post, PostView)
    post_author_info = get_post_author_info(post.author)
    post_record_info = get_post_record_info(post.record)
    other_info = {
        "cid": post.cid,
        "indexed_at": post.indexed_at,
        "like_count": post.like_count,
        "reply_count": post.reply_count,
        "repost_count": post.repost_count,
    }
    return {
        **post_author_info,
        **post_record_info,
        **other_info,   
    }


class FlattenedFeedViewPost(TypedDict):
    author_did: str
    author_handle: str
    author_display_name: str
    created_at: str
    text: str
    langs: list[str]
    cid: str
    indexed_at: str
    like_count: int
    reply_count: int
    repost_count: int


def flatten_feed_view_post(post: FeedViewPost) -> dict:
    # TODO: figure out what "reason" means in the FeedViewPost?
    assert isinstance(post, FeedViewPost)
    post: FlattenedFeedViewPost = flatten_post(post.post)
    return post
