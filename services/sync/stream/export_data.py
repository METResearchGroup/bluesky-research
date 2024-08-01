"""Tooling for managing the export of firehose data, both to local cache and to
S3.

The intended tree structure should look something like this:

    bluesky-research/sync/firehose
        /all
            /create
                /follow
                    (within local cache, it will only be top-level
                    since they will get compressed upon write to S3)
                    'follower_did={follower_did}_followee_did={followee_did}.json'

                    (within S3, there will be this partitioning)
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
                        /{some name, tbh probably just the ID}.json
                    /like_on_user_post
                    /reply_to_user_post
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
study_user_activity_create_path = os.path.join(study_user_activity_root_local_path, "create")  # noqa
study_user_activity_delete_path = os.path.join(study_user_activity_root_local_path, "delete")  # noqa
study_user_activity_relative_path_map = {  # actual full path is {root}/{author_did}/{record_type}/{operation}
    "create": {
        "post": os.path.join("create", "post"),
        "like": os.path.join("create", "like"),
        "follow": {
            "follower": os.path.join("create", "follow", "follower"),
            "followee": os.path.join("create", "follow", "followee")
        },
        "like_on_user_post": os.path.join("create", "like_on_user_post"),
        "reply_to_user_post": os.path.join("create", "reply_to_user_post")
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

    # create helper paths for generic firehose writes.
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


def export_study_user_post(
    record: dict,
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str
):
    """Exports a post record for a study user.

    Whenever a study participant creates a post, we want to track it.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/post/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json

    Where `author_did` is the DID of the person who created the post (which
    should be a person in the study). The `post_uri_suffix` is the last part of
    the URI of the post.
    """
    relative_path = study_user_activity_relative_path_map[operation]["post"]
    root_path = os.path.join(study_user_activity_root_local_path, author_did)
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_study_user_follow(
    record: dict,
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str,
    follow_status: Optional[Literal["follower", "followee"]] = None
):
    """Exports a follow record for a study user.

    Whenever a study participant follows another user, we want to track
    them as a "follower". Whenever a study participant is followed by another
    user, we want to track them as a "followee".

    The key will be structured as follows:
    {root}/{author_did}/{operation}/follow/{follower or followee}/{follower_did={follower_did}_followee_did={followee_did}.json

    Where `author_did` is the DID of the person who is being followed or is
    following someone, whichever is the study user. The `follower_did` is the
    DID of the user that is following the account. The `followee_did` is the
    DID of the user that is being followed.
    """
    if not follow_status:
        raise ValueError("Follow status must be provided for follow records.")
    relative_path = study_user_activity_relative_path_map[operation]["follow"][follow_status]  # noqa
    root_path = os.path.join(study_user_activity_root_local_path, author_did)
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_study_user_like(
    record: dict,
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str
):
    """Exports a like record for a study user.

    The key will be structured as follows:
    {root}/{like_author_did}/{operation}/like/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    Where `like_author_did` is the DID of the person who liked the post (which
    should be a person in the study). The `post_uri_suffix` is the last part of
    the post URI that was liked. The `like_uri_suffix` is the last part of the
    URI of the like record.
    """
    relative_path = study_user_activity_relative_path_map[operation]["like"]
    post_uri_suffix = record["record"]["subject"]["uri"].split('/')[-1]
    root_path = os.path.join(study_user_activity_root_local_path, author_did)
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path, post_uri_suffix)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_like_on_study_user_post(
    record: dict,
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str
):
    """Exports a like on a user post.

    Unlike other exports, this also has the additional key level of the post URI.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/like_on_user_post/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    Where `author_did` is the author of the post that was liked. This should be
    a person in the study. The `post_uri_suffix` is the last part of the URI of
    the post that was liked. The `like_author_did` is the DID of the person who
    liked the post. The `uri_suffix` is the last part of the URI of the like
    record.
    """
    relative_path = study_user_activity_relative_path_map[operation]["like_on_user_post"]  # noqa
    post_uri_suffix = record["record"]["subject"]["uri"].split('/')[-1]
    root_path = os.path.join(study_user_activity_root_local_path, author_did)
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path, post_uri_suffix)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_reply_to_study_user_post(
    record: dict,
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str,
    user_post_type: Literal["root", "parent"]
):
    """Exports a reply to a user post.

    The key will be structured as follows:
    {root}/{root/parent_author_did}/{operation}/reply_to_user_post/{root/parent_post_uri_suffix}/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json

    If the user post was the first post in a thread, then the thread type is
    "root". If the user post was a reply to another post, then the thread type
    is "parent". The `root/parent_author_did` is the DID of the post that the
    study user wrote and that the post was replying to (this study user post is
    either the parent of the post or the root post of the thread). The
    `root/parent_post_uri_suffix` is the last part of the URI of the study user's
    post (which is either the parent or the root post of the thread). The
    `author_did` is the DID of the person who wrote the reply. The
    `post_uri_suffix` is the last part of the URI of the reply record.
    """  # noqa
    relative_path = study_user_activity_relative_path_map[operation]["reply_to_user_post"]
    original_study_user_post_uri_suffix = (
        record["reply_root"].split('/')[-1]
        if user_post_type == "root"
        else record["reply_parent"].split('/')[-1]
    )
    root_path = os.path.join(study_user_activity_root_local_path, author_did)
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path, original_study_user_post_uri_suffix)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_study_user_data_local(
    record: dict,
    record_type: Literal[
        "post", "follow", "like", "like_on_user_post", "reply_to_user_post"
    ],
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str,
    kwargs: Optional[dict] = None
):
    """Writes study user activity to local cache storage."""
    if not kwargs:
        kwargs = {}
    if record_type == "post":
        export_study_user_post(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename
        )
    elif record_type == "follow":
        export_study_user_follow(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename,
            follow_status=kwargs.get("follow_status")
        )
    elif record_type == "like":
        export_study_user_like(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename
        )
    elif record_type == "like_on_user_post":
        export_like_on_study_user_post(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename
        )
    elif record_type == "reply_to_user_post":
        export_reply_to_study_user_post(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename,
            user_post_type=kwargs.get("user_post_type")
        )


def export_study_user_data_s3():
    pass
