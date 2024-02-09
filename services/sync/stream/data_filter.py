"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
import json
import os
import peewee

from services.sync.stream.constants import tmp_data_dir
from services.sync.stream.database import db, Post
from services.sync.stream.filters import filter_incoming_posts



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


def write_posts_to_local_storage(posts_to_create: list[dict]) -> bool:
    """Our data stream callback operates on individual posts. We want
    to write the posts to local storage, and then write them to S3 in batches.

    This function dumps the post to local storage as a dictionary. In case we
    have a change of functionality, this is capable of handling multiple posts
    within the same callback.
    """
    has_written_post_with_data: bool = False
    for post in posts_to_create:
        hashed_filename = f"{hash(post['uri'])}.json"
        file_path = os.path.join(tmp_data_dir, hashed_filename)
        with open(file_path, "w") as f:
            json_str = json.dumps(post)
            if json_str:
                f.write(json_str)
                has_written_post_with_data = True
    return has_written_post_with_data


def operations_callback(operations_by_type: dict) -> bool:
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

    # add to DB (TODO: will have to swap this out for a bulk insert)
    # or add only to S3 and then add to DB later. TBD.
    if posts_to_create:
        manage_post_creation(posts_to_create)
    if posts_to_delete:
        manage_post_deletes(posts_to_delete)

    has_written_data = False

    # write to storage
    if posts_to_create:
        has_written_data = write_posts_to_local_storage(posts_to_create)

    return has_written_data
