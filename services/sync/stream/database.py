"""Database logic for storing streamed feeds

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/database.py

# NOTE: we can convert this to a different DB eventually. This is OK for now.
""" # noqa
from datetime import datetime

import peewee

db = peewee.SqliteDatabase('feed_database.db')
db_version = 2


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Post(BaseModel):
    uri = peewee.CharField(unique=True)
    created_at = peewee.TextField()
    text = peewee.TextField() # for long text
    langs = peewee.CharField(null=True) # sometimes the langs aren't provided
    entities = peewee.CharField(null=True)
    facets = peewee.CharField(null=True) # https://www.pfrazee.com/blog/why-facets
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


if db.is_closed():
    db.connect()
    db.create_tables([Post, SubscriptionState, DbMetadata])

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