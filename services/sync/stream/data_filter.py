"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
from datetime import datetime, timedelta, timezone
from dateutil import parser

import peewee

from services.sync.stream.database import db, Post


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
