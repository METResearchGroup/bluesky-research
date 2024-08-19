"""Load raw data for preprocessing"""
import os
from typing import Literal, Optional

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    load_jsonl_data, find_files_after_timestamp
)
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


def load_latest_firehose_posts(
    source: Literal["s3", "local"],
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    posts_sync_key = s3_export_key_map["create"]["post"]
    latest_partition_timestamp = (
        S3.create_partition_key_based_on_timestamp(latest_preprocessing_timestamp)  # noqa
    )
    # NOTE: might have to revisit later on account of having to load all the
    # keys which is probably pretty inefficient.
    # NOTE: might need to investigate CDC for s3 later so we can just load the
    # latest data.
    if source == "s3":
        keys = s3.list_keys_given_prefix(prefix=posts_sync_key)
        keys = [
            key for key in keys
            if key > os.path.join(posts_sync_key, latest_partition_timestamp)
        ]
        jsonl_data: list[dict] = []
        for key in keys:
            data = s3.read_jsonl_from_s3(key)
            jsonl_data.extend(data[0])
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]
    elif source == "local":
        full_import_filedir = os.path.join(root_local_data_directory, posts_sync_key)  # noqa

        # crawl through the directory and load the files that are newer than
        # the latest partition timestamp. Since "latest_partition_timestamp"
        # is a directory path, we need to recursively crawl through the
        # directory to find the files that are newer than the timestamp.
        files_to_load: list[str] = find_files_after_timestamp(
            base_path=full_import_filedir,
            target_timestamp_path=latest_partition_timestamp
        )
        jsonl_data: list[dict] = []
        for filepath in files_to_load:
            data = load_jsonl_data(filepath)
            jsonl_data.extend(data)
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]

    return transformed_jsonl_data


def load_latest_most_liked_posts(
    source: Literal["s3", "local"],
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    latest_partition_timestamp = (
        S3.create_partition_key_based_on_timestamp(latest_preprocessing_timestamp)  # noqa
    )
    if source == "s3":
        keys = s3.list_keys_given_prefix(prefix=root_most_liked_s3_key)
        keys = [
            key for key in keys
            if key > os.path.join(
                root_most_liked_s3_key, latest_partition_timestamp
            )
        ]
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
        files_to_load: list[str] = find_files_after_timestamp(
            base_path=full_import_filedir,
            target_timestamp_path=latest_partition_timestamp
        )
        jsonl_data: list[dict] = []
        for filepath in files_to_load:
            data = load_jsonl_data(filepath)
            jsonl_data.extend(data)
        transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
            ConsolidatedPostRecordModel(**post) for post in jsonl_data
        ]

    return transformed_jsonl_data


# @track_performance
# def load_latest_posts(
#     source: Literal["s3", "local"],
#     source_feeds: list[str] = ["firehose", "most_liked"],
#     latest_preprocessing_timestamp: Optional[str] = None
# ) -> list[ConsolidatedPostRecordModel]:
#     """Loads latest synced posts."""
#     res: list[ConsolidatedPostRecordModel] = []
#     for source_feed in source_feeds:
#         if source_feed == "firehose":
#             posts = load_latest_firehose_posts(
#                 source=source,
#                 latest_preprocessing_timestamp=latest_preprocessing_timestamp
#             )
#             res.extend(posts)
#         elif source_feed == "most_liked":
#             posts = load_latest_most_liked_posts(
#                 source=source,
#                 latest_preprocessing_timestamp=latest_preprocessing_timestamp
#             )
#             res.extend(posts)
#     return res


@track_performance
def load_latest_posts(post_keys: list[str]) -> list[ConsolidatedPostRecordModel]:  # noqa
    jsonl_data: list[dict] = []
    for key in post_keys:
        data = s3.read_jsonl_from_s3(key)
        if not data:
            logger.warning(f"No data found for key {key}. Check if keys are correct. For example, a firehose post likely should end in '.gz'.")  # noqa
            continue
        else:
            if key.endswith(".gz"):
                # need special processing for any compressed files.
                jsonl_data.extend(data[0])
            else:
                jsonl_data.extend(data)
    transformed_jsonl_data: list[ConsolidatedPostRecordModel] = [
        ConsolidatedPostRecordModel(**post) for post in jsonl_data
    ]
    return transformed_jsonl_data


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
