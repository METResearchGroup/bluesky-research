"""Filters used during streaming."""
from datetime import timedelta
from typing import Optional

from atproto_client.models.app.bsky.feed.post import Main as Record
from atproto_client.models.com.atproto.label.defs import SelfLabels

from lib.constants import (
    BLOCKED_AUTHORS, NUM_DAYS_POST_RECENCY, INAPPROPRIATE_TERMS,
    current_datetime
)
from lib.utils import parse_datetime_string
from services.transform.transform_raw_data import flatten_firehose_post


def has_inappropriate_content(record: Record) -> bool:
    """Determines if a post has inappropriate content.

    Example post that should be filtered out:
    {
        'record': Main(
            created_at='2024-02-07T10:14:49.074Z',
            text='Im usually more active on twitter.. so please consider following me there nwn\n\nHere is my bussiness https://varknakfrery.carrd.co/ :3c https://varknakfrery.carrd.co/\n\n #vore #big #bigbutt #bigbelly #thicc',
            embed=Main(
                images=[
                    Image(
                        alt='Mreow meow Prrr',
                        image=BlobRef(
                            mime_type='image/png',
                            size=583044,
                            ref='bafkreidp4lzhdbd5o7lwubh3vy33vpb66s6reifca3z3c6feebxjprqnai',
                            py_type='blob'
                        ),
                        aspect_ratio=None,
                        py_type='app.bsky.embed.images#image'
                    )
                ],
                py_type='app.bsky.embed.images'
            ),
            entities=None,
            facets=None,
            labels=SelfLabels(
                values=[
                    SelfLabel(
                        val='porn',
                        py_type='com.atproto.label.defs#selfLabel'
                    )
                ],
                py_type='com.atproto.label.defs#selfLabels'
            ),
            langs=None,
            reply=None,
            tags=None,
            py_type='app.bsky.feed.post'
        ),
        'uri': 'at://did:plc:yrslt6ypx6pa2sw5dddi2uum/app.bsky.feed.post/3kkszvglywu2c',
        'cid': 'bafyreihs3gw5cldg6p77vq3sauzl65nmbrqyai6et4dmwxry4wh66235ci',
        'author': 'did:plc:yrslt6ypx6pa2sw5dddi2uum'
    }
    """ # noqa
    if hasattr(record, "labels") and record.labels is not None:
        labels: SelfLabels = record.labels
        for label in labels:
            if label.val in INAPPROPRIATE_TERMS:
                return True
    text = record.text.lower()
    for term in INAPPROPRIATE_TERMS:
        if term in text:
            return True
    return False


def is_within_similar_networks(post: dict) -> bool:
    """Determines if a post is within a similar network.
    
    Inspired by https://blog.twitter.com/engineering/en_us/topics/open-source/2023/twitter-recommendation-algorithm
    """ # noqa
    return True


def filter_created_post(post: dict) -> Optional[dict]:
    """Filters any post that we could write to our DB. Also flattens our
    dictionary."""
    record: Record = post['record']
    # get only posts that are English-language
    if record.langs is not None:
        langs: list[str] = record.langs
        if "en" not in langs:
            return None

    # filter out posts from blocked authors
    if post["author"] in BLOCKED_AUTHORS:
        return None

    # do basic content moderation
    if has_inappropriate_content(record):
        return None

    # filter out posts that don't pass our recency filter:
    date_object = parse_datetime_string(record.created_at)
    time_difference = current_datetime - date_object
    time_threshold = timedelta(days=NUM_DAYS_POST_RECENCY)
    if time_difference > time_threshold:
        return None
    
    # filter out posts based on whether they are within-network or
    # are in similar networks
    if not is_within_similar_networks(post):
        return None

    return post


def filter_incoming_posts(operations_by_type: dict) -> list[dict]:
    """Performs filtering on incoming posts and determines which posts have
    to be created or deleted.

    Returns a dictionary of the format:
    {
        "posts_to_create": list[dict],
        "posts_to_delete": list[dict]
    }
    """
    posts_to_create: list[dict] = []
    posts_to_delete: list[str] = [
        p['uri'] for p in operations_by_type['posts']['deleted']
    ]

    for created_post in operations_by_type['posts']['created']:
        filtered_post = filter_created_post(created_post)
        if filtered_post is not None:
            flattened_post = flatten_firehose_post(filtered_post)
            enriched_post = enrich_post(flattened_post)
            post_text = enriched_post["text"]
            print(f"Writing post  with text: {post_text}")
            posts_to_create.append(enriched_post)

    return {
        "posts_to_create": posts_to_create,
        "posts_to_delete": posts_to_delete
    }


def enrich_post(post: dict) -> dict:
    """Enriches post with additional metadata."""
    return post
