"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
import os
import threading
import time

import peewee

from lib.aws.s3 import ROOT_BUCKET, S3
from services.sync.stream.constants import (
    POST_BATCH_SIZE, S3_FIREHOSE_KEY_ROOT
)
from services.sync.stream.database import db, Post
from services.sync.stream.filters import filter_incoming_posts


s3_client = S3()
thread_lock = threading.Lock()


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


def write_batch_posts_to_s3(
    posts: list[dict], batch_size: int = POST_BATCH_SIZE
) -> None:
    """Writes batch of posts to s3."""
    with thread_lock:
        print(f"Writing batch of {len(posts)} posts to S3 in chunks of {batch_size}...") # noqa
        while posts:
            batch = posts[:batch_size]
            timestamp = str(int(time.time()))
            key = os.path.join(
                S3_FIREHOSE_KEY_ROOT, f"posts_{timestamp}.jsonl"
            )
            if not isinstance(batch, list):
                raise ValueError("Data must be a list of dictionaries.")
            s3_client.write_dicts_jsonl_to_s3(
               data=batch, bucket=ROOT_BUCKET, key=key
            )
            posts = posts[batch_size:]
        print(f"Finished writing {len(posts)} posts to S3.")


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
    """ # noqa
    post_updates = filter_incoming_posts(operations_by_type)
    posts_to_create = post_updates["posts_to_create"]
    posts_to_delete = post_updates["posts_to_delete"]

    if posts_to_create:
        manage_post_creation(posts_to_create)
    if posts_to_delete:
        manage_post_deletes(posts_to_delete)
