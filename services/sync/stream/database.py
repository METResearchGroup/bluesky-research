"""Database logic for storing streamed feeds

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/database.py

# TODO: we can convert this to a different DB eventually. This is OK for now.
""" # noqa
from datetime import datetime
import os
import sqlite3

import peewee
from peewee import TextField
from playhouse.migrate import migrate, SqliteMigrator

from services.sync.stream.constants import current_file_directory

SQLITE_DB_NAME = "firehose.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class FirehosePost(BaseModel):
    uri = peewee.CharField(unique=True)
    created_at = peewee.TextField()
    text = peewee.TextField()  # for long text
    embed = peewee.TextField() # for embedded content
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
    indexed_at = peewee.DateTimeField(default=datetime.utcnow)


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.IntegerField()


class DbMetadata(BaseModel):
    version = peewee.IntegerField()


def add_new_column_to_table(colname: str) -> None:
    """Adds a new column to the existing firehosepost table and backfills
    existing records with a null default value."""
    print(f"Adding new column {colname} to firehosepost table")
    migrator = SqliteMigrator(db)
    migrate(
        migrator.add_column('firehosepost', colname, TextField(null=True))
    )
    print(f"Added new column {colname} to firehosepost table")
    current_table_cols = [col[1] for col in cursor.execute("PRAGMA table_info(firehosepost)")]
    print(f"Current columns in firehosepost table: {current_table_cols}")
    #cursor.execute(f"ALTER TABLE firehosepost ADD COLUMN {colname} TEXT DEFAULT NULL")
    #conn.commit()


if db.is_closed():
    db.connect()
    db.create_tables([FirehosePost, SubscriptionState, DbMetadata])

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
