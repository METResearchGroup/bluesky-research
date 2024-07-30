"""Tooling for managing the export of firehose data, both to local cache and to
S3.

The intended tree structure should look something like this:

    bluesky-research/sync/firehose
        /all
            /create
                /follow
                    /year={year}
                        /month={month}
                            /day={day}
                                /hour={hour}
                                    /minute={minute}
                                        /{hash}.jsonl.gz
                /like
                /post
            /delete
                /follow
                /like
                /post
        /study_user_activity
            /author_did={author_did}
                /create
                    /follow
                        /follower
                        /followee
                    /like
                    /post
                        /{ID}.json
                    /likes_on_user_posts
                /delete
                    /follow
                    /like
                    /post
"""
import json
import os
import shutil
from typing import Literal, Optional
from uuid import uuid4

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import root_local_data_directory
from lib.db.bluesky_models.raw import FirehoseSubscriptionStateCursorModel
from lib.db.manage_local_data import write_jsons_to_local_store
from lib.helper import generate_current_datetime_str

current_file_directory = os.path.dirname(os.path.abspath(__file__))
root_write_path = os.path.join(current_file_directory, "cache")
root_create_path = os.path.join(root_write_path, "create")
root_delete_path = os.path.join(root_write_path, "delete")
operation_types = ["post", "like", "follow"]

# helper paths for generic firehose writes.
export_filepath_map = {
    "create": {
        "post": os.path.join(root_create_path, "post"),
        "like": os.path.join(root_create_path, "like"),
        "follow": os.path.join(root_create_path, "follow")
    },
    "delete": {
        "post": os.path.join(root_delete_path, "post"),
        "like": os.path.join(root_delete_path, "like"),
        "follow": os.path.join(root_delete_path, "follow")
    }
}

root_s3_key = os.path.join("sync", "firehose")

s3_export_key_map = {
    "create": {
        "post": os.path.join(root_s3_key, "create", "post"),
        "like": os.path.join(root_s3_key, "create", "like"),
        "follow": os.path.join(root_s3_key, "create", "follow")
    },
    "delete": {
        "post": os.path.join(root_s3_key, "delete", "post"),
        "like": os.path.join(root_s3_key, "delete", "like"),
        "follow": os.path.join(root_s3_key, "delete", "follow")
    }
}

# helper paths for writing user activity data.
study_user_activity_root_local_path = os.path.join(root_write_path, "study_user_activity")  # noqa
study_user_activity_relative_path_map = {  # actual full path is {root}/{author_did}/{record_type}/{operation}
    "create": {
        "post": os.path.join("create", "post"),
        "like": os.path.join("create", "like"),
        "follow": {
            "follower": os.path.join("create", "follow", "follower"),
            "followee": os.path.join("create", "follow", "followee")
        }
    },
    "delete": {
        "post": os.path.join("delete", "post"),
        "like": os.path.join("delete", "like"),
        "follow": os.path.join("delete", "follow")
    }
}

s3 = S3()
dynamodb = DynamoDB()

SUBSCRIPTION_STATE_TABLE_NAME = "firehoseSubscriptionState"


def rebuild_cache_paths():
    """Rebuild the paths for the cache, if necessary."""
    if not os.path.exists(root_write_path):
        os.makedirs(root_write_path)

    for path in [root_create_path, root_delete_path]:
        if not os.path.exists(path):
            os.makedirs(path)
        for op_type in operation_types:
            op_path = os.path.join(path, op_type)
            if not os.path.exists(op_path):
                os.makedirs(op_path)


def delete_cache_paths():
    """Deletes the cache paths. Recursively removes from the root path."""
    if os.path.exists(root_write_path):
        shutil.rmtree(root_write_path)


rebuild_cache_paths()


def write_data_to_json(data: dict, path: str):
    with open(path, 'w') as f:
        json.dump(data, f)


def compress_cached_files_and_write_to_storage(
    directory: str,
    operation: Literal["create", "delete"],
    operation_type: Literal["post", "like", "follow"],
    compressed: bool = True,
    external_store: Literal["local", "s3"] = "local"
):
    """For a given set of files in a directory, compress them into a single
    cached file and write to S3.

    Loops through all the JSON files in the directory and loads them into
    memory. Then writes to a single .jsonl file (.jsonl.gz if compressed).
    """
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=generate_current_datetime_str()
    )
    # needs a random filename since it is possible that within a single minute,
    # we might have multiple batches that need to be written to S3
    uuid = str(uuid4())
    filename = f"{uuid}.jsonl"
    s3_export_key = s3_export_key_map[operation][operation_type]
    full_key = os.path.join(s3_export_key, partition_key, filename)
    if external_store == "s3":
        s3.write_local_jsons_to_s3(
            directory=directory, key=full_key, compressed=compressed
        )
    elif external_store == "local":
        full_export_filepath = os.path.join(root_local_data_directory, full_key)  # noqa
        write_jsons_to_local_store(
            source_directory=directory, export_filepath=full_export_filepath
        )
    else:
        raise ValueError("Invalid export store.")


def export_batch(
    compressed: bool = True,
    clear_cache: bool = True,
    external_store: list[Literal["local", "s3"]] = ["local", "s3"]
):
    """Writes the batched data to external stores (local store and S3 store).

    Crawls the "created" and "deleted" folders and updates the records
    where necessary.

    Then deletes the local cache.
    """
    for operation in ["create", "delete"]:
        for operation_type in operation_types:
            directory = export_filepath_map[operation][operation_type]
            for store in external_store:
                compress_cached_files_and_write_to_storage(
                    directory=directory,
                    operation=operation,
                    operation_type=operation_type,
                    compressed=compressed,
                    external_store=store
                )
    if clear_cache:
        delete_cache_paths()
    rebuild_cache_paths()


def update_cursor_state_dynamodb(
    cursor_model: FirehoseSubscriptionStateCursorModel
) -> None:
    """Updates the cursor state in DynamoDB."""
    item = cursor_model.dict()
    dynamodb.insert_item_into_table(
        table_name="firehoseSubscriptionState", item=item
    )


def load_cursor_state_dynamodb(service_name: str) -> Optional[FirehoseSubscriptionStateCursorModel]:  # noqa
    """Loads the cursor state from DynamoDB, if it exists. If not, return
    None."""
    key = {
        "service": {"S": service_name}
    }
    result: Optional[dict] = dynamodb.get_item_from_table(
        table_name=SUBSCRIPTION_STATE_TABLE_NAME, key=key
    )
    if not result:
        return None
    return FirehoseSubscriptionStateCursorModel(**result)


def update_cursor_state_s3(
    cursor_model: FirehoseSubscriptionStateCursorModel
) -> None:
    """Updates the cursor state in S3."""
    key = os.path.join("sync", "firehose", "cursor", f"{cursor_model.service}.json")  # noqa
    s3.write_dict_json_to_s3(data=cursor_model.dict(), key=key)


def load_cursor_state_s3(service_name: str) -> Optional[FirehoseSubscriptionStateCursorModel]:  # noqa
    """Loads the cursor state from S3, if it exists. If not, return None."""
    key = os.path.join("sync", "firehose", "cursor", f"{service_name}.json")
    result: Optional[dict] = s3.read_json_from_s3(key=key)
    if not result:
        return None
    return FirehoseSubscriptionStateCursorModel(**result)


def export_study_user_data_local(
    record: dict,
    record_type: Literal["post", "follow", "like"],
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str,
    follow_status: Optional[Literal["follower", "followee"]] = None
):
    """Writes study user activity to local cache storage.

    The data is written to the local storage in the following format:
    {root}/{author_did}/{record_type}/{operation}/{ID}.json
    """
    if record_type == "follow":
        if not follow_status:
            raise ValueError("Follow status must be provided for follow records.")
        relative_path = study_user_activity_relative_path_map[operation][record_type][follow_status]
    else:
        relative_path = study_user_activity_relative_path_map[operation][record_type]  # noqa
    full_path = os.path.join(
        study_user_activity_root_local_path,
        author_did,
        relative_path,
        filename
    )
    write_data_to_json(data=record, path=full_path)


def export_study_user_data_s3():
    pass
