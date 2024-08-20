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
import uuid

from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import S3
from lib.aws.sqs import SQS
from lib.constants import root_local_data_directory
from lib.db.bluesky_models.raw import FirehoseSubscriptionStateCursorModel
from lib.db.manage_local_data import write_jsons_to_local_store
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.participant_data.study_users import get_study_user_manager

logger = get_logger(__name__)

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
sqs = SQS("firehoseSyncsToBeProcessedQueue")

SUBSCRIPTION_STATE_TABLE_NAME = "firehoseSubscriptionState"

study_user_manager = get_study_user_manager()


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

    # create helper path for writing user activity data.
    if not os.path.exists(study_user_activity_root_local_path):
        os.makedirs(study_user_activity_root_local_path)


def delete_cache_paths():
    """Deletes the cache paths. Recursively removes from the root path."""
    if os.path.exists(root_write_path):
        shutil.rmtree(root_write_path)


# if not running in a lambda container, rebuild the cache paths.
if not os.path.exists("/app"):
    rebuild_cache_paths()


def write_data_to_json(data: dict, path: str):
    with open(path, 'w') as f:
        json.dump(data, f)


def compress_cached_files_and_write_to_storage(
    directory: str,
    operation: Literal["create", "delete"],
    operation_type: Literal["post", "like", "follow"],
    compressed: bool = True,
    external_store: Literal["local", "s3"] = "s3",
    send_sqs_message: bool = True
):
    """For a given set of files in a directory, compress them into a single
    cached file and write to S3.

    Loops through all the JSON files in the directory and loads them into
    memory. Then writes to a single .jsonl file (.jsonl.gz if compressed).

    Then, sends an SQS message indicating that the corresponding file has been
    synced, for the downstream preprocessing module to process.
    """
    timestamp_str = generate_current_datetime_str()
    partition_key = S3.create_partition_key_based_on_timestamp(
        timestamp_str=timestamp_str
    )
    filename = f"{timestamp_str}-{str(uuid.uuid4())}.jsonl"
    s3_export_key = s3_export_key_map[operation][operation_type]
    full_key = os.path.join(s3_export_key, partition_key, filename)
    if external_store == "s3":
        json_dicts: list[dict] = s3.write_local_jsons_to_s3(
            directory=directory, key=full_key, compressed=compressed
        )
        # NOTE: for now, we will only preprocess posts, so we'll send the SQS
        # message only for posts.
        if send_sqs_message and operation_type == "post" and operation == "create":  # noqa
            s3_key = full_key
            if compressed:
                s3_key += ".gz"
            sqs_data_payload = {
                "sync": {
                    "source": "firehose",
                    "operation": operation,
                    "operation_type": operation_type,
                    "s3_key": s3_key
                }
            }
            custom_log = f"Sending message to SQS queue from firehose feed for new posts at {s3_key}"  # noqa
            sqs.send_message(
                source="firehose", data=sqs_data_payload, custom_log=custom_log
            )
        if operation_type == "post" and operation == "create":
            # write post to separate location to track daily posts (for daily
            # superposter calculation)
            daily_posts_dicts: list[dict] = []
            for post in json_dicts:
                author_did = post["author_did"]
                uri = post["uri"]
                payload = {
                    "author_did": author_did,
                    "uri": uri
                }
                daily_posts_dicts.append(payload)
            # daily-posts is hardcoded in the Glue tf config.
            daily_posts_key = os.path.join("daily-posts", filename)
            s3.write_dicts_jsonl_to_s3(data=daily_posts_dicts, key=daily_posts_key)  # noqa
            logger.info(f"Exported {len(daily_posts_dicts)} daily posts to S3 for daily superposter calculation")  # noqa

    elif external_store == "local":
        full_export_filepath = os.path.join(root_local_data_directory, full_key)  # noqa
        write_jsons_to_local_store(
            source_directory=directory, export_filepath=full_export_filepath
        )
    else:
        raise ValueError("Invalid export store.")


def export_general_firehose_sync(
    compressed: bool = True,
    external_store: list[Literal["local", "s3"]] = ["local", "s3"]
):
    """Exports the general firehose sync data to external store."""
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


def export_study_user_post_s3(base_path: str):
    """Exports the post data of study users to external S3 store.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/post/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json

    We want to match the structure of the local cache to the S3 store.

    The S3 key will be structured as follows:
    /study_user_activity/{author_did}/create/post/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json
    """  # noqa
    key_root: list[str] = base_path.split('/')[-3:]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("post")
    base_key = '/'.join(key_root)

    # loop through all the files in the post directory and write them to S3.
    posts_filenames: list[str] = os.listdir(os.path.join(base_path, "post"))
    for path in posts_filenames:
        full_path = os.path.join(base_path, "post", path)
        full_key = os.path.join(base_key, path)
        with open(full_path, 'r') as f:
            data = json.load(f)
            s3.write_dict_json_to_s3(data=data, key=full_key)

    logger.info(f"Exported {len(posts_filenames)} post records to S3 for study user DID {key_root[1]}.")  # noqa


def export_study_user_follow_s3(base_path: str):
    """Exports the follow data of study users to external S3 store.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/follow/{follower or followee}/{follower_did={follower_did}_followee_did={followee_did}.json

    We want to match the structure of the local cache to the S3 store.

    The S3 key will be structured as follows:
    /study_user_activity/{author_did}/create/follow/{follower or followee}/{follower_did={follower_did}_followee_did={followee_did}.json
    """  # noqa
    key_root: list[str] = base_path.split('/')[-3:]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("follow")
    base_key = '/'.join(key_root)

    # loop through follow/follower subdirectories and export follow data to S3.
    total_records = 0
    follows_path = os.path.join(base_path, "follow")
    for follow_type in os.listdir(follows_path):
        follow_path = os.path.join(base_path, "follow", follow_type)
        follow_records = os.listdir(follow_path)
        for filepath in follow_records:
            full_path = os.path.join(follow_path, filepath)
            full_key = os.path.join(base_key, follow_type, filepath)
            with open(full_path, 'r') as f:
                data = json.load(f)
                s3.write_dict_json_to_s3(data=data, key=full_key)
            total_records += 1

    logger.info(f"Exported {total_records} follow records to S3 for study user DID {key_root[1]}.")  # noqa


def export_study_user_like_s3(base_path: str):
    """Exports the like data of study users to external S3 store.

    The key will be structured as follows:
    {root}/{like_author_did}/{operation}/like/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    `like_author_did` is the DID of the person who liked the post (which should
    be a person in the study). The `post_uri_suffix` is the last part of the
    post URI that was liked. The `like_uri_suffix` is the last part of the URI
    of the like record.

    We want to match the structure of the local cache to the S3 store.

    The S3 key will be structured as follows:
    /study_user_activity/{like_author_did}/create/like/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json
    """  # noqa
    key_root: list[str] = base_path.split('/')[-3:]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("like")
    base_key = '/'.join(key_root)

    # loop through all the files in the like directory and write them to S3.
    total_records = 0
    liked_posts_path = os.path.join(base_path, "like")
    for post_uri in os.listdir(liked_posts_path):
        post_path = os.path.join(base_path, "like", post_uri)
        for like_record in os.listdir(post_path):
            full_path = os.path.join(post_path, like_record)
            full_key = os.path.join(base_key, post_uri, like_record)
            with open(full_path, 'r') as f:
                data = json.load(f)
                s3.write_dict_json_to_s3(data=data, key=full_key)
            total_records += 1

    logger.info(f"Exported {total_records} like records to S3 for study user DID {key_root[1]}.")  # noqa


def export_like_on_study_user_post_s3(base_path: str):
    """Exports a like on a post by a user in the study.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/like_on_user_post/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    We want to match the structure of the local cache to the S3 store.

    The S3 key will be structured as follows:
    /study_user_activity/{author_did}/create/like_on_user_post/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json
    """  # noqa
    key_root: list[str] = base_path.split('/')[-3:]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("like_on_user_post")
    base_key = '/'.join(key_root)

    # loop through all the files in the like_on_user_post directory and write them to S3.
    total_records = 0
    liked_posts_path = os.path.join(base_path, "like_on_user_post")
    for post_uri in os.listdir(liked_posts_path):
        post_path = os.path.join(base_path, "like_on_user_post", post_uri)
        for like_record in os.listdir(post_path):
            full_path = os.path.join(post_path, like_record)
            full_key = os.path.join(base_key, post_uri, like_record)
            with open(full_path, 'r') as f:
                data = json.load(f)
                s3.write_dict_json_to_s3(data=data, key=full_key)
            total_records += 1

    logger.info(f"Exported {total_records} like on user post records to S3 for study user DID {key_root[1]}.")  # noqa


def export_reply_to_study_user_post_s3(base_path: str):
    """Exports a reply to a study user's post.

    The key will be structured as follows:
    {root}/{root/parent_author_did}/{operation}/reply_to_user_post/{root/parent_post_uri_suffix}/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json

    We want to match the structure of the local cache to the S3 store.

    The S3 key will be structured as follows:
    /study_user_activity/{root/parent_author_did}/create/reply_to_user_post/{root/parent_post_uri_suffix}/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json

    Where `root/parent_author_did` is the DID of the author of the root/parent
    post (which should be the study user) and `root/parent_post_uri_suffix` is
    the last part of the URI of the root/parent post. The `author_did` is the
    DID of the person who wrote the reply. The `post_uri_suffix` is the last
    part of the URI of the reply record.
    """  # noqa
    key_root: list[str] = base_path.split('/')[-3:]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("reply_to_user_post")
    base_key = '/'.join(key_root)

    # loop through all the files in the reply_to_user_post directory and write them to S3.
    total_records = 0
    reply_posts_path = os.path.join(base_path, "reply_to_user_post")
    for post_uri in os.listdir(reply_posts_path):
        post_path = os.path.join(base_path, "reply_to_user_post", post_uri)
        for reply_record in os.listdir(post_path):
            full_path = os.path.join(post_path, reply_record)
            full_key = os.path.join(base_key, post_uri, reply_record)
            with open(full_path, 'r') as f:
                data = json.load(f)
                s3.write_dict_json_to_s3(data=data, key=full_key)
            total_records += 1

    logger.info(f"Exported {total_records} reply to user post records to S3 for study user DID {key_root[1]}.")  # noqa


def export_study_user_activity_local_data():
    """Exports the activity data of study users to external S3 store.

    Steps:
    1. Recursively list all the user DIDs in the study_user_activity directory.
    2. Takes the files for each user DID.
    3. Dump to s3.

    list out all study user DIDs that appeared in this batch. These are
    all the users for whom we got activities for in this batch of the
    firehose.
    """
    study_users = os.listdir(study_user_activity_root_local_path)
    for author_did in study_users:
        logger.info(f"Exporting study user activity data for author DID {author_did}.")  # noqa
        author_path = os.path.join(study_user_activity_root_local_path, author_did)
        for operation in ["create", "delete"]:
            if operation == "create":
                create_path = os.path.join(author_path, "create")
                # ["post", "like", "follow", "like_on_user_post", "reply_to_user_post"]
                # (but might not be, and likely won't be, all available at once)
                # (it is much more likely that a subset, maybe only 1 of these,
                # is actually available in a given batch).
                record_types = os.listdir(create_path)
                for record_type in record_types:
                    if record_type == "post":
                        export_study_user_post_s3(base_path=create_path)
                    elif record_type == "like":
                        export_study_user_like_s3(base_path=create_path)
                    elif record_type == "follow":
                        export_study_user_follow_s3(base_path=create_path)
                    elif record_type == "like_on_user_post":
                        export_like_on_study_user_post_s3(base_path=create_path)  # noqa
                    elif record_type == "reply_to_user_post":
                        export_reply_to_study_user_post_s3(base_path=create_path)  # noqa
            elif operation == "delete":
                # deletions are more of an edge case, might have to deal with
                # it at some point but it's much lower priority.
                pass


def export_batch(
    compressed: bool = True,
    clear_cache: bool = True,
    external_store: list[Literal["local", "s3"]] = ["local", "s3"]
):
    """Writes the batched data to external stores (local store and/or S3 store).

    Crawls the "created" and "deleted" folders and updates the records
    where necessary.

    Then deletes the local cache.

    Exports both the general firehose sync data and the study user activity data.
    that has been tracked in this batch.
    """  # noqa
    export_general_firehose_sync(
        compressed=compressed, external_store=external_store
    )
    export_study_user_activity_local_data()

    # clears cache for next batch.
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

    # export JSON file.
    write_data_to_json(data=record, path=full_path)

    # update StudyUserManager store with new post.
    study_user_manager.insert_study_user_post(
        post_uri=record["uri"],
        user_did=author_did
    )


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
