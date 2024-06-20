"""Database logic for storing created feeds."""
import os
import sqlite3

import peewee

from lib.helper import create_batches
from services.create_feeds.models import CreatedFeedModel

current_file_directory = os.path.dirname(os.path.abspath(__file__))

SQLITE_DB_NAME = "created_feeds.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

insert_batch_size = 100


class BaseModel(peewee.Model):
    class Meta:
        database = db


class CreatedFeeds(BaseModel):
    """Table of all feeds generated per user."""
    id = peewee.AutoField()
    study_user_id = peewee.CharField()
    bluesky_user_did = peewee.CharField()
    bluesky_user_handle = peewee.CharField()
    condition = peewee.CharField()
    # comma-separated URIs. Could be long. Each URI is ~70 chars. If we have
    # 100 URIs, that's 7000 chars plus 99 commas, or 7,099 chars.
    feed_uris = peewee.TextField()
    timestamp = peewee.CharField()


if db.is_closed():
    db.connect()
    db.create_tables([CreatedFeeds])


def create_initial_created_feeds_table() -> None:
    """Create the initial created feeds table."""
    with db.atomic():
        db.create_tables([CreatedFeeds])


def write_created_feed(data: dict) -> None:
    """Write a created feed to the database."""
    with db.atomic():
        try:
            CreatedFeeds.create(**data)
        except peewee.IntegrityError as e:
            print(f"Error inserting created feed for user {data['bluesky_user_did']} into DB: {e}")  # noqa
            return


def batch_insert_created_feeds(feeds: list[CreatedFeedModel]) -> None:
    """Batch write created feeds to the database."""
    with db.atomic():
        batches = create_batches(feeds, insert_batch_size)
        for batch in batches:
            batch_dicts = [feed.dict() for feed in batch]
            CreatedFeeds.insert_many(batch_dicts).execute()
    print(f"Batch inserted {len(feeds)} created feeds into the database.")
    print(f"Latest timestamp for feed creation: {feeds[-1].timestamp}")
    print(f"Total feed count: {CreatedFeeds.select().count()}")
    for condition in ["reverse_chronological", "engagement", "representative_diversification"]:  # noqa
        total_count = CreatedFeeds.select().where(
            CreatedFeeds.condition == condition
        ).count()
        print(f"Total feed count for {condition} condition: {total_count}")


def get_created_feeds() -> list[dict]:
    """Get all created feeds."""
    with db.atomic():
        return CreatedFeeds.select().dicts()


def load_latest_created_feeds_per_user() -> list[CreatedFeedModel]:
    """For each unique study_user_id, load the latest created feed."""
    with db.atomic():
        query = CreatedFeeds.select(
            CreatedFeeds.study_user_id,
            CreatedFeeds.bluesky_user_did,
            CreatedFeeds.bluesky_user_handle,
            CreatedFeeds.condition,
            CreatedFeeds.feed_uris,
            CreatedFeeds.timestamp
        ).distinct(
            CreatedFeeds.study_user_id
        ).order_by(
            CreatedFeeds.timestamp.desc()
        )
        return [CreatedFeedModel(**feed) for feed in query.dicts()]


def get_num_created_feeds() -> int:
    """Get the number of created feeds."""
    with db.atomic():
        return CreatedFeeds.select().count()


if __name__ == "__main__":
    # create_initial_created_feeds_table()
    num_created_feeds = get_num_created_feeds()
    print(f"Total number of created feeds: {num_created_feeds}")
    latest_timestamp = CreatedFeeds.select().order_by(
        CreatedFeeds.timestamp.desc()
    ).first().timestamp
    print(f"Latest timestamp for feed creation: {latest_timestamp}")
    feeds = get_created_feeds()
    print(f"Total number of feeds: {len(feeds)}")
    if len(feeds) < 10:
        for feed in feeds:
            print(feed)
    else:
        print("Too many feeds to print.")
