"""Database logic for storing streamed feeds

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/database.py

# TODO: we can convert this to a different DB eventually. This is OK for now.
"""  # noqa
import os
import sqlite3

import peewee

from services.sync.stream.constants import current_file_directory

SQLITE_DB_NAME = "raw_posts.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class RawPost(BaseModel):
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


if db.is_closed():
    db.connect()
    db.create_tables([RawPost, SubscriptionState, DbMetadata])

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
    pass
