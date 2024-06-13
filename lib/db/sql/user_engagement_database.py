"""Database for tracking user engagement."""
import os
import peewee
import sqlite3
from typing import Optional

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SYNC_SQLITE_DB_NAME = "user_engagement.db"
SYNC_SQLITE_DB_PATH = os.path.join(current_file_directory, SYNC_SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SYNC_SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SYNC_SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class PostsWrittenByStudyUsers(BaseModel):
    """Class for the posts written by study users."""
    uri = peewee.CharField(unique=True)
    cid = peewee.CharField(index=True)
    indexed_at = peewee.CharField()
    created_at = peewee.CharField()
    author_did = peewee.CharField()
    author_handle = peewee.CharField()
    record = peewee.TextField()
    text = peewee.TextField()
    synctimestamp = peewee.CharField()
    url = peewee.CharField(null=True)
    like_count = peewee.IntegerField(null=True)
    reply_count = peewee.IntegerField(null=True)
    repost_count = peewee.IntegerField(null=True)


def create_initial_tables(drop_all_tables: bool = False) -> None:
    """Create the initial tables, optionally dropping all existing tables first.

    :param drop_all_tables: If True, drops all existing tables before creating new ones.
    """  # noqa
    with db.atomic():
        if drop_all_tables:
            print("Dropping all tables in database...")
            db.drop_tables([PostsWrittenByStudyUsers], safe=True)
        print("Creating tables in database...")
        db.create_tables([PostsWrittenByStudyUsers])


def get_most_recent_post_timestamp(author_handle: str) -> Optional[str]:
    """Get the most recent post timestamp for a given author handle."""
    query = f"""
    SELECT created_at
    FROM PostsWrittenByStudyUsers
    WHERE author_handle = '{author_handle}'
    ORDER BY created_at DESC
    LIMIT 1;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


if __name__ == "__main__":
    create_initial_tables(drop_all_tables=True)
