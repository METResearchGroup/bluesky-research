"""Filters data from firehose stream.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/data_filter.py
"""  # noqa
import json
import os
import peewee

import pandas as pd
from pydantic import ValidationError as PydanticValidationError

from lib.db.bluesky_models.transformations import TransformedRecordWithAuthorModel  # noqa
from lib.db.sql.sync_database import db, TransformedRecordWithAuthor
from services.participant_data.helper import get_user_to_bluesky_profiles_as_df
from services.sync.stream.constants import tmp_data_dir
from transform.transform_raw_data import flatten_firehose_post


study_participants_bsky_profiles_df: pd.DataFrame = (
    get_user_to_bluesky_profiles_as_df()
)
existing_users_bsky_dids_set = set(
    study_participants_bsky_profiles_df["bluesky_user_did"]
)


def filter_by_user_in_study(bsky_did: str) -> bool:
    return bsky_did in existing_users_bsky_dids_set


def manage_likes(likes: dict[str, list]) -> dict:
    """Manages the likes and follows.

    We'll build this in later, but this is a placeholder function for when we
    do manage the likes. We only want to do this when it involves a user in our
    study (e.g., if a user likes a post)

    Each like is a dictionary of the format:
    {
        "created": [], "deleted": []
    }
    """
    return {}


def manage_follows(follows: dict[str, list]) -> dict:
    """Manages the follows.

    We'll build this in later, but this is a placeholder function for when we
    do manage the follows. We only want to do this when it involves a user in
    our study (e.g., if a user follows an account).

    Each follow is a dictionary of the format:
    {
        "created": [], "deleted": []
    }
    """
    return {}


def manage_posts(posts: dict[str, list]) -> dict:
    """Manages which posts to create or delete.

    We want to track any new posts in our database.
    """
    posts_to_create: list[dict] = []
    posts_to_delete: list[str] = [
        p['uri'] for p in posts['deleted']
    ]

    for new_post in posts["created"]:
        if new_post is not None:
            flattened_post: dict = flatten_firehose_post(new_post).dict()
            posts_to_create.append(flattened_post)
    return {
        "posts_to_create": posts_to_create,
        "posts_to_delete": posts_to_delete
    }


def filter_incoming_posts(operations_by_type: dict) -> dict:
    """Performs filtering on incoming posts and determines which posts have
    to be created or deleted.

    Returns a dictionary of the format:
    {
        "posts_to_create": list[dict],
        "posts_to_delete": list[dict]
    }

    We want to store all new posts, and then process likes and follows only if
    they are from users in our study or from users in their network.
    """
    posts: dict[str, list] = operations_by_type["posts"]
    likes: dict[str, list] = operations_by_type["likes"]
    follows: dict[str, list] = operations_by_type["follows"]

    manage_likes(likes=likes)
    manage_follows(follows=follows)
    post_updates: dict = manage_posts(posts=posts)
    return post_updates


def manage_post_creation(posts_to_create: list[dict]) -> None:
    """Manage post insertion into DB."""
    with db.atomic():
        for post_dict in posts_to_create:
            try:
                TransformedRecordWithAuthorModel(**post_dict)
                TransformedRecordWithAuthor.create(**post_dict)
            except PydanticValidationError as e:
                print(f"Pydantic error validating post with URI {post_dict['uri']}: {e}")  # noqa
                continue
            except peewee.IntegrityError as e:
                # can come from duplicate records, schema invalidation, etc.
                error_str = str(e)
                if "UNIQUE constraint failed" in error_str:
                    print(f"Post with URI {post_dict['uri']} already exists in DB.")  # noqa
                else:
                    print(f"Error inserting post with URI {post_dict['uri']} into DB: {e}")  # noqa
                continue
            except peewee.OperationalError as e:
                # generally comes from sqlite3 errors itself, not just
                # something with the schema or the DB.
                print(f"Error inserting post with URI {post_dict['uri']} into DB: {e}")  # noqa
                continue


def manage_post_deletes(posts_to_delete: list[str]) -> None:
    """Manage post deletion from DB."""
    TransformedRecordWithAuthor.delete().where(
        TransformedRecordWithAuthor.uri.in_(posts_to_delete)
    )


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
    """  # noqa
    post_updates = filter_incoming_posts(operations_by_type)
    posts_to_create = post_updates["posts_to_create"]
    posts_to_delete = post_updates["posts_to_delete"]

    # add to DB (TODO: will have to swap this out for a bulk insert)
    # or add only to S3 and then add to DB later. TBD.
    if posts_to_create:
        manage_post_creation(posts_to_create)
    if posts_to_delete:
        manage_post_deletes(posts_to_delete)

    has_written_data = True

    # write to storage: we may want to do this later but not for now. We do
    # this in case we want to store the raw data as JSONs somewhere, which
    # we may want to do?
    # has_written_data = False
    # if posts_to_create:
    #    has_written_data = write_posts_to_local_storage(posts_to_create)

    return has_written_data
