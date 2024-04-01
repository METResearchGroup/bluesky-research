import datetime
import os
import peewee
import sqlite3

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "filtered_posts.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)
DEFAULT_BATCH_WRITE_SIZE = 100

db = peewee.SqliteDatabase(SQLITE_DB_NAME)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_NAME)
cursor = conn.cursor()

class BaseModel(peewee.Model):
    class Meta:
        database = db


class FilteredFirehosePost(BaseModel):
    """Filtered version of Firehose post."""
    uri = peewee.CharField(unique=True)
    passed_filters = peewee.BooleanField(default=False)
    filtered_at = peewee.DateTimeField(default=datetime.utcnow)


if db.is_closed():
    db.connect()
    db.create_tables([FilteredFirehosePost])


def create_initial_filtered_posts_table() -> None:
    """Create the initial filtered posts table."""
    with db.atomic():
        db.create_tables([FilteredFirehosePost])


def create_filtered_post(uri: str, passed_filters: bool) -> None:
    """Create a filtered post in the database."""
    with db.atomic():
        try:
            FilteredFirehosePost.create(uri=uri, passed_filters=passed_filters)
        except peewee.IntegrityError:
            print(f"Post with URI {uri} already exists in DB.")
            return


def batch_create_filtered_posts(posts: list[dict]) -> None:
    """Batch create filtered posts in chunks.
    
    Uses peewee's chunking functionality to write posts in chunks.
    """
    with db.atomic():
        for idx in range(0, len(posts), DEFAULT_BATCH_WRITE_SIZE):
            FilteredFirehosePost.insert_many(posts[idx:idx + 100]).execute()
    print(f"Batch created {len(posts)} posts.")
        

def create_filtered_posts_from_df(df) -> None:
    """Create filtered posts from a dataframe."""
    with db.atomic():
        for _, row in df.iterrows():
            create_filtered_post(row["uri"], row["passed_filters"])


def get_previously_filtered_post_uris() -> list:
    """Get previous IDs from the DB."""
    previous_ids = FilteredFirehosePost.select(FilteredFirehosePost.uri)
    return [p.uri for p in previous_ids]


if __name__ == "__main__":
    create_initial_filtered_posts_table()
