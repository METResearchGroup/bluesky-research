"""Database logic for all post records synced from Bluesky."""
import os
import sqlite3
from typing import Optional, Union

import pandas as pd
import peewee

from lib.db.bluesky_models.transformations import (
    TransformedRecordWithAuthorModel
)


current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "synced_record_posts.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


# database models for `TransformedRecordWithAuthor` tables.
class BaseModel(peewee.Model):
    class Meta:
        database = db


class TransformedRecordWithAuthor(BaseModel):
    """Class for the (transformed) raw posts."""
    uri = peewee.CharField(unique=True)
    created_at = peewee.TextField()
    # for long text. Technically a post can just be an image or video and
    # not have text.
    text = peewee.TextField(null=True)
    embed = peewee.TextField(null=True)  # for embedded content
    langs = peewee.CharField(null=True)  # sometimes the langs aren't provided
    entities = peewee.CharField(null=True)
    facets = peewee.CharField(null=True)  # https://www.pfrazee.com/blog/why-facets # noqa
    labels = peewee.CharField(null=True)
    reply = peewee.CharField(null=True)
    reply_parent = peewee.CharField(null=True)
    reply_root = peewee.CharField(null=True)
    tags = peewee.CharField(null=True)
    py_type = peewee.CharField()
    cid = peewee.CharField(index=True)
    author = peewee.CharField()
    synctimestamp = peewee.CharField()


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.IntegerField()


class DbMetadata(BaseModel):
    version = peewee.IntegerField()


def get_record_by_uri(uri: str) -> TransformedRecordWithAuthor:
    """Get a record by its URI."""
    return TransformedRecordWithAuthor.get(TransformedRecordWithAuthor.uri == uri)  # noqa


def get_record_as_dict_by_uri(uri: str) -> dict:
    """Get a record by its URI as a dictionary."""
    record = get_record_by_uri(uri)
    return record.__dict__['__data__']


def insert_new_record(record: TransformedRecordWithAuthorModel):
    """Insert a new record into the database."""
    with db.atomic():
        print(f"Inserting record with URI {record.uri} into DB.")
        record_dict = record.to_dict()
        TransformedRecordWithAuthor.create(**record_dict)


def get_posts(k: Optional[int] = None) -> list[TransformedRecordWithAuthor]:
    """Get all posts from the database."""
    if k:
        return list(TransformedRecordWithAuthor.select().limit(k))
    return list(TransformedRecordWithAuthor.select())


def get_most_recent_posts(k: Optional[int] = None) -> list[TransformedRecordWithAuthor]:  # noqa
    """Get the most recent posts from the database."""
    if k:
        return list(
            TransformedRecordWithAuthor
            .select()
            .order_by(TransformedRecordWithAuthor.synctimestamp.desc())
            .limit(k)
        )
    return list(
        TransformedRecordWithAuthor
        .select()
        .order_by(TransformedRecordWithAuthor.synctimestamp.desc())
    )


def get_posts_as_list_dicts(
    k: Optional[int] = None,
    order_by: Optional[str] = "synctimestamp",
    desc: Optional[bool] = True
) -> list[dict]:
    """Get all posts from the database as a list of dictionaries."""
    if order_by:
        if desc:
            if k:
                posts = (
                    TransformedRecordWithAuthor
                    .select()
                    .order_by(getattr(TransformedRecordWithAuthor, order_by).desc())  # noqa
                    .limit(k)
                )
            else:
                posts = (
                    TransformedRecordWithAuthor
                    .select()
                    .order_by(getattr(TransformedRecordWithAuthor, order_by).desc())  # noqa
                )
        else:
            if k:
                posts = (
                    TransformedRecordWithAuthor
                    .select()
                    .order_by(getattr(TransformedRecordWithAuthor, order_by))
                    .limit(k)
                )
            else:
                posts = (
                    TransformedRecordWithAuthor
                    .select()
                    .order_by(getattr(TransformedRecordWithAuthor, order_by))
                )
    else:
        posts = get_posts(k=k)
    return [post.__dict__['__data__'] for post in posts]


def get_posts_as_df(k: Optional[int] = None) -> pd.DataFrame:
    """Get all posts from the database as a pandas DataFrame."""
    return pd.DataFrame(get_posts_as_list_dicts(k=k))


def get_num_posts() -> int:
    """Get the number of posts in the database."""
    return TransformedRecordWithAuthor.select().count()


if db.is_closed():
    db.connect()
    db.create_tables(
        [TransformedRecordWithAuthor, SubscriptionState, DbMetadata]
    )

    # DB migration
    current_version = 1
    if DbMetadata.select().count() != 0:
        current_version = DbMetadata.select().first().version

    if current_version != db_version:
        with db.atomic():
            # V2
            # Drop cursors stored from the old bsky.social PDS
            if current_version == 1:
                SubscriptionState.delete().execute()

            # Update version in DB
            if DbMetadata.select().count() == 0:
                DbMetadata.insert({DbMetadata.version: db_version}).execute()
            else:
                DbMetadata.update({DbMetadata.version: db_version}).execute()

if __name__ == "__main__":
    num_posts = get_num_posts()
    print(f"Total number of posts: {num_posts}")
