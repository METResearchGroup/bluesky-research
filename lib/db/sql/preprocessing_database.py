"""Database logic for posts that we've preprocessed."""
import os
import peewee
import sqlite3
from typing import Optional

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

current_file_directory = os.path.dirname(os.path.abspath(__file__))
PREPROCESSING_SQLITE_DB_NAME = "filtered_posts.db"
PREPROCESSING_SQLITE_DB_PATH = os.path.join(
    current_file_directory, PREPROCESSING_SQLITE_DB_NAME
)
DEFAULT_BATCH_WRITE_SIZE = 100

db = peewee.SqliteDatabase(PREPROCESSING_SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(PREPROCESSING_SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class FilteredPreprocessedPosts(BaseModel):
    """Filtered version of Firehose post."""
    uri = peewee.CharField(unique=True)
    cid = peewee.CharField(index=True)
    indexed_at = peewee.CharField()
    author = peewee.CharField()
    metadata = peewee.TextField()
    record = peewee.TextField()
    metrics = peewee.TextField(null=True)
    passed_filters = peewee.BooleanField(default=False)
    filtered_at = peewee.CharField()
    filtered_by_func = peewee.CharField(null=True)
    preprocessing_timestamp = peewee.CharField()


if db.is_closed():
    db.connect()
    db.create_tables([FilteredPreprocessedPosts])


def create_initial_filtered_posts_table() -> None:
    """Create the initial filtered posts table."""
    with db.atomic():
        db.create_tables([FilteredPreprocessedPosts])


def create_filtered_post(post: FilteredPreprocessedPostModel) -> None:
    """Create a filtered post in the database."""
    with db.atomic():
        try:
            FilteredPreprocessedPosts.create(post.dict())
        except peewee.IntegrityError:
            print(f"Post with URI {post.uri} already exists in DB.")
            return


def batch_create_filtered_posts(
    posts: list[FilteredPreprocessedPostModel]
) -> None:
    """Batch create filtered posts in chunks.

    Uses peewee's chunking functionality to write posts in chunks.
    """
    with db.atomic():
        for idx in range(0, len(posts), DEFAULT_BATCH_WRITE_SIZE):
            FilteredPreprocessedPosts.insert_many(
                posts[idx:idx + DEFAULT_BATCH_WRITE_SIZE]
            ).on_conflict_ignore().execute()
    print(f"Batch created {len(posts)} posts.")


def get_previously_filtered_post_uris() -> set[str]:
    """Get previous IDs from the DB."""
    previous_ids = FilteredPreprocessedPosts.select(FilteredPreprocessedPosts.uri)  # noqa
    return set([p.uri for p in previous_ids])


def get_filtered_posts(
    k: Optional[int] = None,
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[dict]:  # noqa
    """Get filtered posts from the database."""
    query = FilteredPreprocessedPosts.select().where(
        FilteredPreprocessedPosts.passed_filters == True
    )
    if latest_preprocessing_timestamp:
        query = query.where(
            FilteredPreprocessedPosts.preprocessing_timestamp
            > latest_preprocessing_timestamp
        )
    if k:
        query = query.limit(k)
    res = list(query)
    res_dicts = [r.__dict__['__data__'] for r in res]
    return res_dicts


def get_filtered_posts_as_list_dicts(
    k: Optional[int] = None,
    order_by: Optional[str] = "synctimestamp",
    desc: Optional[bool] = True
) -> list[dict]:
    """Get all filtered posts from the database as a list of dictionaries."""
    base_query = FilteredPreprocessedPosts.select().where(
        FilteredPreprocessedPosts.passed_filters == True
    )
    # Apply ordering based on the order_by and desc parameters
    if order_by:
        if desc:
            order_expression = getattr(
                FilteredPreprocessedPosts, order_by
            ).desc()
        else:
            order_expression = getattr(FilteredPreprocessedPosts, order_by)
        base_query = base_query.order_by(order_expression)

    # Apply limit if k is specified
    if k:
        base_query = base_query.limit(k)
    posts = base_query.execute()
    return [post.__dict__['__data__'] for post in posts]


def load_latest_preprocessing_timestamp() -> Optional[str]:
    """Loads the last time that preprocessing was done.

    Assumes that we only want to load data after the last time that we've
    preprocessed data (i.e., we're not doing a backfill).
    """
    with db.atomic():
        try:
            latest_preprocessing_timestamp = (
                FilteredPreprocessedPosts
                .select(FilteredPreprocessedPosts.preprocessing_timestamp)
                .order_by(FilteredPreprocessedPosts.preprocessing_timestamp.desc())
                .get()
                .preprocessing_timestamp
            )
        except peewee.DoesNotExist:
            latest_preprocessing_timestamp = None
    return latest_preprocessing_timestamp


if __name__ == "__main__":
    create_initial_filtered_posts_table()
