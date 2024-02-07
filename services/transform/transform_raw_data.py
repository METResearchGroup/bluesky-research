"""Transforms raw data.

Based on https://github.com/MarshalX/atproto/blob/main/lexicons/app.bsky.feed.defs.json
"""
from typing import TypedDict, Union

from atproto_client.models.app.bsky.actor.defs import ProfileViewBasic
from atproto_client.models.app.bsky.feed.post import Main
from atproto_client.models.app.bsky.feed.defs import FeedViewPost, PostView
from atproto_client.models.dot_dict import DotDict


def hydrate_feed_view_post(feed_post: dict) -> FeedViewPost:
    """Hydrate a FeedViewPost from a dictionary."""
    return FeedViewPost(**feed_post)


def get_post_author_info(post_author: ProfileViewBasic) -> dict:
    """Get author information from a post."""
    assert isinstance(post_author, ProfileViewBasic)
    return {
        "author_did": post_author["did"],
        "author_handle": post_author["handle"],
        "author_display_name": post_author["display_name"],
    }


def get_post_record_info(post_record: Union[DotDict, Main]) -> dict:
    assert isinstance(post_record, DotDict) or isinstance(post_record, Main)
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


class FlattenedFirehosePost(TypedDict):
    uri: str
    created_at: str
    text: str
    langs: str
    entities: str
    facets: str
    labels: str
    reply: str
    reply_parent: str
    reply_root: str
    tags: str
    py_type: str
    cid: str
    author: str


def flatten_firehose_post(post: dict) -> FlattenedFirehosePost:
    """Flattens a post from the firehose.
    
    For some reason, the post format from the firehose is different from the
    post format when the post is part of a feed?

    Here is an example of this format:

    {
        'record': Main(
            created_at='2024-02-07T05:10:02.159Z',
            text='こんなポストするとBANされそうで怖いです',
            embed=Main(
                record=Main(
                    cid='bafyreidy6bxkwxbjvw6mqfxivp7rjywk3gpnzvbg2vaks2qhljzs6manyq',
                    uri='at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/3kksirfddwa2z',
                    py_type='com.atproto.repo.strongRef'
                ),
                py_type='app.bsky.embed.record'
            ),
            entities=None,
            facets=None,
            labels=None,
            langs=['ja'],
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:sjeosezgc7mpqn6sfc7neabg/app.bsky.feed.post/3kksiuknorv2u',
        'cid': 'bafyreidmb5wsupl6iz5wo2xjgusjpsrduug6qkpytjjckupdttot6jrbna',
        'author': 'did:plc:sjeosezgc7mpqn6sfc7neabg'
    }
    """
    # manage if post is part of a thread
    reply_parent = None
    reply_root = None
    if "reply" in post["record"] and "parent" in post["record"]["reply"]:
        reply_parent = post["record"]["reply"]["parent"]["uri"]
    if "reply" in post["record"] and "root" in post["record"]["reply"]:
        reply_root = post["record"]["reply"]["root"]["uri"]

    flattened_firehose_dict = {
        "uri": post["uri"],
        "created_at": post["record"]["created_at"],
        "text": post["record"]["text"],
        "langs": (
            ','.join(post["record"]["langs"])
            if post["record"]["langs"] is not None else None
        ),
        "entities": (
            ','.join(post["record"]["entities"])
            if post["record"]["entities"] is not None else None
        ),
        "facets": (
            ','.join(post["record"]["facets"])
            if post["record"]["facets"] is not None else None
        ),
        "labels": (
            ','.join(post["record"]["labels"])
            if post["record"]["labels"] is not None else None
        ),
        "reply": post["record"]["reply"],
        "reply_parent": reply_parent,
        "reply_root": reply_root,
        "tags": (
            ','.join(post["record"]["tags"])
            if post["record"]["tags"] is not None else None
        ),
        "py_type": post["record"]["py_type"],
        "cid": post["cid"],
        "author": post["author"],
    }
    return flattened_firehose_dict


class FlattenedFeedViewPost(TypedDict):
    author_did: str
    author_handle: str
    author_display_name: str
    created_at: str
    text: str
    langs: str
    cid: str
    indexed_at: str
    like_count: int
    reply_count: int
    repost_count: int


def flatten_feed_view_post(post: FeedViewPost) -> dict:
    # TODO: figure out what "reason" means in the FeedViewPost?
    # TODO: still need to figure out what the ID of a post is?
    assert isinstance(post, FeedViewPost)
    post: FlattenedFeedViewPost = flatten_post(post.post)
    return post
