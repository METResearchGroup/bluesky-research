import os
import peewee
import sqlite3
from typing import Optional

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "filtered_posts.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)
DEFAULT_BATCH_WRITE_SIZE = 100

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class FilteredRawPost(BaseModel):
    """Filtered version of Firehose post."""
    uri = peewee.CharField(unique=True)
    passed_filters = peewee.BooleanField(default=False)
    filtered_at = peewee.CharField()
    filtered_by_func = peewee.CharField(null=True)
    created_at = peewee.TextField()
    text = peewee.TextField()  # for long text
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


if db.is_closed():
    db.connect()
    db.create_tables([FilteredRawPost])


def create_initial_filtered_posts_table() -> None:
    """Create the initial filtered posts table."""
    with db.atomic():
        db.create_tables([FilteredRawPost])


def create_filtered_post(uri: str, passed_filters: bool) -> None:
    """Create a filtered post in the database."""
    with db.atomic():
        try:
            FilteredRawPost.create(uri=uri, passed_filters=passed_filters)
        except peewee.IntegrityError:
            print(f"Post with URI {uri} already exists in DB.")
            return


def batch_create_filtered_posts(posts: list[dict]) -> None:
    """Batch create filtered posts in chunks.

    Uses peewee's chunking functionality to write posts in chunks.
    """
    with db.atomic():
        for idx in range(0, len(posts), DEFAULT_BATCH_WRITE_SIZE):
            FilteredRawPost.insert_many(
                posts[idx:idx + DEFAULT_BATCH_WRITE_SIZE]
            ).on_conflict_ignore().execute()
    print(f"Batch created {len(posts)} posts.")


def create_filtered_posts_from_df(df) -> None:
    """Create filtered posts from a dataframe."""
    with db.atomic():
        for _, row in df.iterrows():
            create_filtered_post(row["uri"], row["passed_filters"])


def get_previously_filtered_post_uris() -> list:
    """Get previous IDs from the DB."""
    previous_ids = FilteredRawPost.select(FilteredRawPost.uri)
    return [p.uri for p in previous_ids]


def get_filtered_posts(k: Optional[int] = None) -> list[FilteredRawPost]:
    """Get filtered posts from the database.

    Grab only posts th  tpassed filteres ("passed_filters=True)
    """
    if k:
        return FilteredRawPost.select().limit(k)
    return FilteredRawPost.select()


def get_filtered_posts_as_list_dicts(
    k: Optional[int] = None,
    order_by: Optional[str] = "synctimestamp",
    desc: Optional[bool] = True
) -> list[dict]:
    """Get all filtered posts from the database as a list of dictionaries."""
    base_query = FilteredRawPost.select().where(FilteredRawPost.passed_filters == True)  # noqa
    # Apply ordering based on the order_by and desc parameters
    if order_by:
        if desc:
            order_expression = getattr(FilteredRawPost, order_by).desc()
        else:
            order_expression = getattr(FilteredRawPost, order_by)
        base_query = base_query.order_by(order_expression)

    # Apply limit if k is specified
    if k:
        base_query = base_query.limit(k)
    posts = base_query.execute()
    return [post.__dict__['__data__'] for post in posts]


if __name__ == "__main__":
    create_initial_filtered_posts_table()