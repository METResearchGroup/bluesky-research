"""Stores functionalities for doing operations in MongoDB."""
from typing import Optional

from pymongo import InsertOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from pymongo.mongo_client import MongoClient

from lib.load_env_vars import EnvVarsContainer


mongodb_uri = EnvVarsContainer.get_env_var("MONGODB_URI")
mongo_db_name = "bluesky-research-posts"
mongodb_client = MongoClient(mongodb_uri)
mongo_db = mongodb_client[mongo_db_name]

# we changed the format of the posts, so any posts older than this will
# have the wrong format.
default_mongodb_timestamp = "2024-05-20-00:00:00"

task_to_mongodb_collection_name = {
    "get_most_liked_posts": "most_liked_posts",
    "llm_political_labeling": "bluesky_posts_political_labels",
    "perspective_api_labels": "bluesky_posts_perspective_api_labels",
}

DEFAULT_INSERT_CHUNK_SIZE = 100
DEFAULT_LOAD_LIMIT = 100


def get_mongodb_collection(task: str) -> tuple[str, Collection]:
    collection_name = task_to_mongodb_collection_name[task]
    return collection_name, mongo_db[collection_name]


def insert_chunks(
    mongo_collection,
    operations,
    chunk_size=100
) -> tuple[int, int]:
    """Insert collections into MongoDB in chunks, using the operations that
    are predefined."""
    total_successful_inserts = 0
    duplicate_key_count = 0

    for i in range(0, len(operations), chunk_size):
        chunk = operations[i:i + chunk_size]
        try:
            result = mongo_collection.bulk_write(chunk, ordered=False)
            total_successful_inserts += result.inserted_count
        except BulkWriteError as bwe:
            total_successful_inserts += bwe.details['nInserted']
            duplicate_key_count += len(bwe.details['writeErrors'])

    return total_successful_inserts, duplicate_key_count


def chunk_insert_posts(
    posts: list[dict],
    mongo_collection: Collection,
    chunk_size: int = DEFAULT_INSERT_CHUNK_SIZE
) -> tuple[int, int]:
    """Chunk insert posts into MongoDB."""
    mongodb_operations: list[InsertOne] = [InsertOne(post) for post in posts]
    total_successful_inserts, duplicate_key_count = insert_chunks(
        mongo_collection=mongo_collection,
        operations=mongodb_operations,
        chunk_size=chunk_size
    )
    return (total_successful_inserts, duplicate_key_count)


def load_collection(
    collection: Collection,
    limit: Optional[int] = DEFAULT_LOAD_LIMIT,
    latest_timestamp: Optional[str] = None,
    timestamp_fieldname: Optional[str] = "metadata.synctimestamp"
) -> list[dict]:
    """Loads content from a collection in MongoDB."""
    query = {}
    if not latest_timestamp:
        latest_timestamp = default_mongodb_timestamp
    query[timestamp_fieldname] = {"$gt": latest_timestamp}
    cursor = collection.find(query)
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)
