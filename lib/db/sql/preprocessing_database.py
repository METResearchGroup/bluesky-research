"""Database logic for posts that we've preprocessed."""
import ast
import os
import peewee
import sqlite3
from typing import Literal, Optional, Union

from lib.db.bluesky_models.transformations import (
    TransformedProfileViewBasicModel, TransformedRecordModel
)
from lib.log.logger import Logger
from services.consolidate_post_records.models import (
    ConsolidatedPostRecordMetadataModel, ConsolidatedMetrics
)
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

logger = Logger(__name__)


class BaseModel(peewee.Model):
    class Meta:
        database = db


class FilteredPreprocessedPosts(BaseModel):
    """Filtered version of Firehose post."""
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

    Uses peewee's chunking functionality to write posts in chunks. Adds
    deduping on URI as well. This should already be done in various upstream
    places but is here for redundancy.
    """
    total_posts = FilteredPreprocessedPosts.select().count()
    logger.info(f"Total number of posts before inserting into preprocessed posts database: {total_posts}") # noqa
    seen_uris = set()
    unique_posts = []
    for post in posts:
        if post.uri not in seen_uris:
            seen_uris.add(post.uri)
            unique_posts.append(post)
    logger.info(f"Total posts to attempt to insert: {len(posts)}")
    logger.info(f"Total unique posts: {len(unique_posts)}")
    logger.info(f"Total duplicates: {len(posts) - len(unique_posts)}")
    with db.atomic():
        for idx in range(0, len(unique_posts), DEFAULT_BATCH_WRITE_SIZE):
            posts_to_insert = unique_posts[idx:idx + DEFAULT_BATCH_WRITE_SIZE]
            posts_to_insert_dicts = [post.dict() for post in posts_to_insert]
            FilteredPreprocessedPosts.insert_many(
                posts_to_insert_dicts
            ).on_conflict_ignore().execute()
    total_posts = FilteredPreprocessedPosts.select().count()
    logger.info(f"Total number of posts after inserting into preprocessed posts database: {total_posts}") # noqa


def get_previously_filtered_post_uris() -> set[str]:
    """Get previous IDs from the DB."""
    previous_ids = FilteredPreprocessedPosts.select(FilteredPreprocessedPosts.uri)  # noqa
    return set([p.uri for p in previous_ids])


def get_filtered_posts(
    k: Optional[int] = None,
    latest_preprocessing_timestamp: Optional[str] = None,
    export_format: Optional[Literal["model", "dict"]]="model"
) -> list[Union[FilteredPreprocessedPostModel, dict]]:
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
    res_dicts: list[dict] = [r.__dict__['__data__'] for r in res]
    transformed_res: list[FilteredPreprocessedPostModel] = [
        FilteredPreprocessedPostModel(
            uri=res_dict["uri"],
            cid=res_dict["cid"],
            indexed_at=res_dict["indexed_at"],
            author=TransformedProfileViewBasicModel(
                **ast.literal_eval(res_dict["author"])
            ),
            metadata=ConsolidatedPostRecordMetadataModel(
                **ast.literal_eval(res_dict["metadata"])
            ),
            record=TransformedRecordModel(
                **ast.literal_eval(res_dict["record"])
            ),
            metrics=(
                ConsolidatedMetrics(**ast.literal_eval(res_dict["metrics"]))
                if res_dict["metrics"] else None
            ),
            passed_filters=res_dict["passed_filters"],
            filtered_at=res_dict["filtered_at"],
            filtered_by_func=res_dict["filtered_by_func"],
            synctimestamp=res_dict["synctimestamp"],
            preprocessing_timestamp=res_dict["preprocessing_timestamp"]
        )
        for res_dict in res_dicts
    ]
    if export_format == "dict":
        # doing the conversion here since doing it upstream leads to JSON
        # processing errors due to the metadata and other fields being JSON
        # fields within JSON fields, which can lead to JSONDecodeError problems.
        return [post.dict() for post in transformed_res]
    return transformed_res


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


def load_latest_preprocessed_post_timestamp() -> Optional[str]:
    """Loads the timestamp of the latest preprocessed post, so that we can
    preprocess all posts more recent than said post.
    """
    with db.atomic():
        try:
            latest_synctimestamp = (
                FilteredPreprocessedPosts
                .select(FilteredPreprocessedPosts.synctimestamp)
                .order_by(FilteredPreprocessedPosts.preprocessing_timestamp.desc())
                .get()
                .synctimestamp
            )
        except peewee.DoesNotExist:
            latest_synctimestamp = None
    return latest_synctimestamp

if __name__ == "__main__":
    # create_initial_filtered_posts_table()
    total_posts = FilteredPreprocessedPosts.select().count()
    print(f"Total posts: {total_posts}")
