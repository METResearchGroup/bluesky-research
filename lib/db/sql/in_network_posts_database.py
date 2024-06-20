"""Database logic for storing in-network posts."""
import os
import peewee
import sqlite3
from typing import Optional

from lib.constants import current_datetime_str
from lib.helper import create_batches
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "in_network_posts.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)
DEFAULT_BATCH_WRITE_SIZE = 100

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

insert_batch_size = 100


class BaseModel(peewee.Model):
    class Meta:
        database = db


class InNetworkPosts(BaseModel):
    """Posts that are in-network to any users in the study. Includes only the
    posts that are in-network, not other posts. Matches the fields in the
    `FilteredPreprocessedPosts` table, but adds an additional field for the
    post's indexing timestamp.
    """
    uri = peewee.CharField(unique=True)
    cid = peewee.CharField(index=True)
    indexed_at = peewee.CharField(null=True)
    author = peewee.CharField()
    metadata = peewee.TextField()
    record = peewee.TextField()
    metrics = peewee.TextField(null=True)
    passed_filters = peewee.BooleanField(default=False)
    filtered_at = peewee.CharField()
    filtered_by_func = peewee.CharField(null=True)
    synctimestamp = peewee.CharField()
    preprocessing_timestamp = peewee.CharField()
    indexed_in_network_timestamp = peewee.CharField()


if db.is_closed():
    db.connect()
    db.create_tables([InNetworkPosts])


def create_initial_tables():
    with db.atomic():
        db.create_tables([InNetworkPosts])


def get_latest_indexed_in_network_timestamp() -> Optional[str]:
    """Load the timestamp of when the latest in-network post was indexed."""
    try:
        latest_post = InNetworkPosts.select().order_by(
            InNetworkPosts.indexed_in_network_timestamp.desc()
        ).get()
        return latest_post.indexed_in_network_timestamp
    except InNetworkPosts.DoesNotExist:
        return None


def batch_insert_in_network_posts(posts: list[FilteredPreprocessedPostModel]):
    with db.atomic():
        batches = create_batches(posts, insert_batch_size)
        for batch in batches:
            # TODO: these need to be adopted to fit the schema.
            batch_dicts = [
                {
                    **post.dict(),
                    **{"indexed_in_network_timestamp": current_datetime_str}
                } for post in batch
            ]
            InNetworkPosts.insert_many(batch_dicts).execute()  # noqa
    print(f"Inserted {len(posts)} in-network posts.")


if __name__ == "__main__":
    create_initial_tables()
