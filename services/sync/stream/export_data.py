"""Tooling for managing the export of firehose data, both to local cache and to
S3."""
import json
import os
import shutil
from typing import Literal, Optional

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.db.bluesky_models.raw import FirehoseSubscriptionStateCursorModel

current_file_directory = os.path.dirname(os.path.abspath(__file__))
root_write_path = os.path.join(current_file_directory, "cache")
root_create_path = os.path.join(root_write_path, "create")
root_delete_path = os.path.join(root_write_path, "delete")
operation_types = ["post", "like", "follow"]

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


def compress_cached_files(
    directory: str,
    operation: Literal["create", "delete"],
    operation_type: Literal["post", "like", "follow"],
    compressed: bool = True,
    clear_cache: bool = True
):
    """For a given set of files in a directory, compress them into a single
    cached file.

    Loops through all the JSON files in the directory and loads them into
    memory. Then writes to a single .jsonl file (.jsonl.gz if compressed).
    """
    filename = f"{current_datetime_str}.jsonl"
    s3_export_key = s3_export_key_map[operation][operation_type]
    full_key = os.path.join(s3_export_key, filename)
    s3.write_local_jsons_to_s3(
        directory=directory, key=full_key,
        clear_cache=clear_cache, compressed=compressed
    )


def write_batch_to_s3(compressed: bool = True, clear_cache: bool = True):
    """Writes the batched data to S3.

    Crawls the "created" and "deleted" folders and updates the records
    where necessary.

    Then deletes the local cache.
    """
    for operation in ["create", "delete"]:
        for operation_type in operation_types:
            directory = export_filepath_map[operation][operation_type]
            compress_cached_files(
                directory=directory,
                operation=operation,
                operation_type=operation_type,
                compressed=compressed,
                clear_cache=clear_cache
            )
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
