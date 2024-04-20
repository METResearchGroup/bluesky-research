"""Database logic for storing created feeds."""
import os
import sqlite3

import peewee

current_file_directory = os.path.dirname(os.path.abspath(__file__))

SQLITE_DB_NAME = "created_feeds.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class CreatedFeeds(BaseModel):
    """Table of all feeds generated per user."""
    id = peewee.AutoField()
    bluesky_user_did = peewee.CharField()
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
            print(f"Error inserting created feed for user {data['bluesky_user_did']} into DB: {e}") # noqa
            return


def batch_write_created_feeds(data: list[dict]) -> None:
    """Batch write created feeds to the database."""
    with db.atomic():
        for feed in data:
            try:
                CreatedFeeds.create(**feed)
            except peewee.IntegrityError as e:
                print(f"Error inserting created feed for user {feed['bluesky_user_did']} into DB: {e}") # noqa
                continue


def get_created_feeds() -> list[dict]:
    """Get all created feeds."""
    with db.atomic():
        return CreatedFeeds.select().dicts()


def get_num_created_feeds() -> int:
    """Get the number of created feeds."""
    with db.atomic():
        return CreatedFeeds.select().count()


if __name__ == "__main__":
    num_created_feeds = get_num_created_feeds()
    print(f"Total number of created feeds: {num_created_feeds}")
    latest_timestamp = CreatedFeeds.select().order_by(
        CreatedFeeds.timestamp.desc()
    ).first().timestamp
    print(f"Latest timestamp for feed creation: {latest_timestamp}")
    feeds = get_created_feeds()
    for feed in feeds:
        print(feed)
