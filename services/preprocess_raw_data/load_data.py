"""Load raw data for preprocessing"""
import os
from typing import Literal, Optional

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import load_jsonl_data
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.sync.stream.export_data import s3_export_key_map
from services.sync.most_liked_posts.helper import root_most_liked_s3_key

logger = get_logger(__file__)

s3 = S3()

dynamodb_table_name = "preprocessingPipelineMetadata"
dynamodb = DynamoDB()
dynamodb_table = dynamodb.resource.Table(dynamodb_table_name)


def load_previous_session_metadata():
    """Loads previous session data from DynamoDB, if it exists.

    Loads latest based on "current_preprocessing_timestamp" field, which
    is the partition key for the table.
    """
    response = dynamodb_table.scan()
    items = response["Items"]
    if not items:
        return None
    latest_item = max(items, key=lambda x: x["current_preprocessing_timestamp"])  # noqa
    return latest_item


def load_uris_of_previously_preprocessed_posts(source: Literal["s3", "local"] = "s3") -> set:  # noqa
    """Loads URIs of previously preprocessed posts."""
    file_exists = False
    if not file_exists:
        return set()


previously_preprocessed_post_uris: set[str] = (
    load_uris_of_previously_preprocessed_posts()
)


def load_latest_firehose_posts(
    source: Literal["s3", "local"],
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    posts_sync_key = s3_export_key_map["create"]["posts"]
    if source == "s3":
        keys = s3.list_keys_greater_than_timestamp(
            prefix=posts_sync_key,
            timestamp=latest_preprocessing_timestamp
        )
        jsonl_data: list[dict] = []
        for key in keys:
            data = s3.read_jsonl_from_s3(key)
            jsonl_data.extend(data)
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]
    elif source == "local":
        full_import_filedir = os.path.join(root_local_data_directory, posts_sync_key)  # noqa
        files_to_load: list[str] = [
            file for file in os.listdir(full_import_filedir)
            if file > latest_preprocessing_timestamp
        ]
        jsonl_data: list[dict] = []
        for file in files_to_load:
            with open(os.path.join(full_import_filedir, file), "r") as f:
                jsonl_data.extend(load_jsonl_data(f))
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]

    # remove any posts that were previously preprocessed
    transformed_jsonl_data = [
        post for post in transformed_jsonl_data
        if post.uri not in previously_preprocessed_post_uris
    ]

    return transformed_jsonl_data


def load_latest_most_liked_posts(
    source: Literal["s3", "local"],
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    if source == "s3":
        keys = s3.list_keys_greater_than_timestamp(
            prefix=root_most_liked_s3_key,
            timestamp=latest_preprocessing_timestamp
        )
        jsonl_data: list[dict] = []
        for key in keys:
            data = s3.read_jsonl_from_s3(key)
            jsonl_data.extend(data)
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]
    elif source == "local":
        full_import_filedir = os.path.join(
            root_local_data_directory, root_most_liked_s3_key
        )
        files_to_load: list[str] = [
            file for file in os.listdir(full_import_filedir)
            if file > latest_preprocessing_timestamp
        ]
        jsonl_data: list[dict] = []
        for file in files_to_load:
            with open(os.path.join(full_import_filedir, file), "r") as f:
                jsonl_data.extend(load_jsonl_data(f))
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]

    transformed_jsonl_data = [
        post for post in transformed_jsonl_data
        if post.uri not in previously_preprocessed_post_uris
    ]

    return transformed_jsonl_data


@track_performance
def load_latest_posts(
    source: Literal["s3", "local"],
    source_feeds: list[str] = ["firehose", "most_liked"],
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    """Loads latest synced posts."""
    res: list[ConsolidatedPostRecordModel] = []
    for source_feed in source_feeds:
        if source_feed == "firehose":
            posts = load_latest_firehose_posts(
                source=source,
                latest_preprocessing_timestamp=latest_preprocessing_timestamp
            )
            res.extend(posts)
        elif source_feed == "most_liked":
            posts = load_latest_most_liked_posts(
                source=source,
                latest_preprocessing_timestamp=latest_preprocessing_timestamp
            )
    return res


def load_latest_likes(
    source: Literal["s3", "local"],
    latest_preprocessing_timestamp: Optional[str] = None
):
    return []


def load_latest_follows(
    source: Literal["s3", "local"],
    latest_preprocessing_timestamp: Optional[str] = None
):
    return []
