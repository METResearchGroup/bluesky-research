"""Database logic for all post records synced from Bluesky."""
import ast
import os
import sqlite3
from typing import List, Optional

import pandas as pd
import peewee

from lib.db.bluesky_models.transformations import (
    TransformedRecordWithAuthorModel
)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SYNC_SQLITE_DB_NAME = "synced_record_posts.db"
SYNC_SQLITE_DB_PATH = os.path.join(current_file_directory, SYNC_SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SYNC_SQLITE_DB_PATH)
db_version = 2

sync_conn = sqlite3.connect(SYNC_SQLITE_DB_PATH)
sync_cursor = sync_conn.cursor()

default_firehose_timestamp = "2024-05-01"

# database models for `TransformedRecordWithAuthor` tables.
class BaseModel(peewee.Model):
    class Meta:
        database = db


class TransformedRecordWithAuthor(BaseModel):
    """Class for the (transformed) raw posts."""
    uri = peewee.CharField(unique=True)
    cid = peewee.CharField(index=True)
    author = peewee.CharField()
    metadata = peewee.TextField()
    record = peewee.TextField()


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


def get_posts(k: Optional[int] = None) -> List[TransformedRecordWithAuthor]:
    """Get all posts from the database."""
    if k:
        return list(TransformedRecordWithAuthor.select().limit(k))
    return list(TransformedRecordWithAuthor.select())


def get_most_recent_posts(k: Optional[int] = None) -> List[TransformedRecordWithAuthor]:  # noqa
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
) -> List[dict]:
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


def get_latest_firehose_posts(
    k: int = None, latest_preprocessing_timestamp: str = None
) -> list[dict]:
    """Get the latest firehose posts from the database."""
    query = TransformedRecordWithAuthor.select()
    if k:
        query = query.limit(k)
    res = list(query)
    res_dicts = [r.__dict__['__data__'] for r in res]
    if not latest_preprocessing_timestamp:
        # set a default to be sometime before when we expect this firehose
        # code to work (YYYY-MM-DD format)
        latest_preprocessing_timestamp = default_firehose_timestamp
    if latest_preprocessing_timestamp:
        # would be nice to include in the query as I assume that the DB will be
        # strictly faster and more efficient, but we'll see if this is OK.
        res_dicts = [
            {
                "uri": res_dict["uri"],
                "cid": res_dict["cid"],
                "author": res_dict["author"],
                "metadata": ast.literal_eval(res_dict["metadata"]),
                "record": ast.literal_eval(res_dict["record"])
            }
            for res_dict in res_dicts
            if ast.literal_eval(res_dict["metadata"]).get("synctimestamp", "") # noqa
            > latest_preprocessing_timestamp
        ]
    return res_dicts


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
                DbMetadata.insert({DbMetadata.version: db_version}).execute()  # noqa
            else:
                DbMetadata.update({DbMetadata.version: db_version}).execute()  # noqa

if __name__ == "__main__":
    # db.drop_tables([TransformedRecordWithAuthor])
    # db.create_tables([TransformedRecordWithAuthor])
    num_posts = get_num_posts()
    print(f"Total number of posts: {num_posts}")
