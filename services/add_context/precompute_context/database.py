import json
import os
import peewee
import sqlite3
from typing import Optional

from services.add_context.precompute_context.models import FullPostContextModel

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "post_contexts.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)
DEFAULT_BATCH_WRITE_SIZE = 100

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class FullPostContext(BaseModel):
    """Full context for a post."""
    uri = peewee.CharField(unique=True)
    context = peewee.TextField()  # will need to serialize
    text = peewee.TextField()
    timestamp = peewee.TextField()


if db.is_closed():
    db.connect()
    db.create_tables([FullPostContext])


def create_initial_context_table() -> None:
    """Create the initial filtered posts table."""
    with db.atomic():
        db.create_tables([FullPostContext])


def insert_post_context(post: FullPostContextModel):
    """Insert a post context into the database."""
    with db.atomic():
        FullPostContext.create(
            uri=post.uri,
            context=post.context.json(),
            text=post.text,
            timestamp=post.timestamp
        )
    print(f"Inserted post context for {post.uri} into DB.")


def get_post_context(uri: str) -> Optional[FullPostContextModel]:
    """Get a post context from the database."""
    post = FullPostContext.get_or_none(FullPostContext.uri == uri)
    if post:
        print(f"Fetching post context for {uri} from DB.")
        return FullPostContextModel(
            uri=post.uri,
            context=json.loads(post.context),
            text=post.text,
            timestamp=post.timestamp
        )
    return None


if __name__ == "__main__":
    create_initial_context_table()
