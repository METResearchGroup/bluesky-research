"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
from datetime import datetime, timedelta, timezone
from dateutil import parser
from typing import Optional

import peewee

from atproto_client.models.app.bsky.feed.post import Main as Record
from atproto_client.models.com.atproto.label.defs import SelfLabels

from services.sync.stream.database import db, Post
from services.transform.transform_raw_data import flatten_firehose_post


BLOCKED_AUTHORS = [] # likely that we will want to filter out some authors (e.g., spam accounts)
NUM_DAYS_POST_RECENCY = 3 # we only want recent posts (Bluesky docs recommend 3 days, see )
current_datetime = current_datetime = datetime.now(timezone.utc)
INAPPROPRIATE_TERMS = ["porn", "furry"]


def manage_post_creation(posts_to_create: list[dict]) -> None:
    """Manage post insertion into DB."""
    with db.atomic():
        for post_dict in posts_to_create:
            try:
                Post.create(**post_dict)
            except peewee.IntegrityError:
                print(f"Post with URI {post_dict['uri']} already exists in DB.")
                continue


def manage_post_deletes(posts_to_delete: list[str]) -> None:
    """Manage post deletion from DB."""
    Post.delete().where(Post.uri.in_(posts_to_delete))


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
    """
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


def parse_datetime_string(datetime_str: str) -> datetime:
    """Parses the different types of datetime strings in Bluesky (for some 
    reason, there are different datetime string formats used)."""
    return parser.parse(datetime_str).replace(tzinfo=timezone.utc)


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

    return post


def enrich_post(post: dict) -> dict:
    """Enriches post with additional metadata."""
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


def operations_callback(operations_by_type: dict) -> None:
    """Callback for managing posts during stream. We perform the first pass
    of filtering here.

    This function takes as input a dictionary of the format
    {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    which tells us what operations should be added to our database.

    We also manage the logic for saving the posts to our DB as well as deleting
    any posts from our DB that no longer exist.

    Example object:
    {
        'posts': {
            'created': [
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
            ],
            'deleted': []
        },
        'reposts': {'created': [],'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []}
    }
    """
    post_updates = filter_incoming_posts(operations_by_type)
    posts_to_create = post_updates["posts_to_create"]
    posts_to_delete = post_updates["posts_to_delete"]

    if posts_to_create:
        print(f"Creating {len(posts_to_create)} posts...")
        manage_post_creation(posts_to_create)
    if posts_to_delete:
        print(f"Deleting {len(posts_to_delete)} posts...")
        manage_post_deletes(posts_to_delete)
