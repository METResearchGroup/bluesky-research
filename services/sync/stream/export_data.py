"""Tooling for managing the export of firehose data, both to local cache and to
S3.

The intended tree structure should look something like this:

    bluesky-research/sync/firehose
        /all
            /create
                /follow
                    /followee
                        {timestamp}.jsonl
                    /follower
                        {timestamp}.jsonl
                /like
                    {timestamp}.jsonl
                /post
                    {timestamp}.jsonl
            /delete
                /follow
                /like
                /post
        /study_user_activity
            /create
                # note, we're storing follows
                # in 'scraped-user-social-network'
                /like
                    {timestamp}.jsonl
                /post
                    {timestamp}.jsonl
                /follow (NOTE: the /follow and subdirectories won't exist
                in S3 as we're writing them to 'scraped-user-social-network')
                    /followee
                        {timestamp}.jsonl
                    /follower
                        {timestamp}.jsonl
                /like_on_user_post
                    {timestamp}.jsonl
                /reply_to_user_post
                    {timestamp}.jsonl
        /in_network_user_activity
            /create
                /post
                    {timestamp}.jsonl
        /scraped-user-social-network
            {timestamp}.jsonl
"""

import json
import os
import shutil
from typing import Literal, Optional
import uuid

import pandas as pd

from lib.aws.dynamodb import DynamoDB
from lib.aws.glue import Glue
from lib.aws.s3 import S3
from lib.aws.sqs import SQS
from lib.constants import root_local_data_directory, timestamp_format
from lib.db.bluesky_models.raw import FirehoseSubscriptionStateCursorModel
from lib.db.manage_local_data import (
    write_jsons_to_local_store,
    export_data_to_local_storage,
    get_local_prefixes_for_service,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.compact_all_services.helper import delete_empty_folders
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
        "follow": os.path.join(root_create_path, "follow"),
    },
    "delete": {
        "post": os.path.join(root_delete_path, "post"),
        "like": os.path.join(root_delete_path, "like"),
        "follow": os.path.join(root_delete_path, "follow"),
    },
}

root_s3_key = os.path.join("sync", "firehose")

s3_export_key_map = {
    "create": {
        "post": os.path.join(root_s3_key, "create", "post"),
        "like": os.path.join(root_s3_key, "create", "like"),
        "follow": os.path.join(root_s3_key, "create", "follow"),
    },
    "delete": {
        "post": os.path.join(root_s3_key, "delete", "post"),
        "like": os.path.join(root_s3_key, "delete", "like"),
        "follow": os.path.join(root_s3_key, "delete", "follow"),
    },
}

# helper paths for writing user activity data.
study_user_activity_root_local_path = os.path.join(
    root_write_path, "study_user_activity"
)  # noqa
study_user_activity_create_path = os.path.join(
    study_user_activity_root_local_path, "create"
)  # noqa
study_user_activity_delete_path = os.path.join(
    study_user_activity_root_local_path, "delete"
)  # noqa
study_user_activity_relative_path_map = {
    "create": {
        "post": os.path.join("create", "post"),
        "like": os.path.join("create", "like"),
        "follow": {
            "followee": os.path.join("create", "follow", "followee"),
            "follower": os.path.join("create", "follow", "follower"),
        },
        "like_on_user_post": os.path.join("create", "like_on_user_post"),
        "reply_to_user_post": os.path.join("create", "reply_to_user_post"),
    },
    "delete": {
        "post": os.path.join("delete", "post"),
        "like": os.path.join("delete", "like"),
    },
}

# helper paths for writing in-network user activity data.
in_network_user_activity_root_local_path = os.path.join(
    root_write_path, "in_network_user_activity"
)  # noqa
in_network_user_activity_create_post_local_path = os.path.join(
    in_network_user_activity_root_local_path, "create", "post"
)

glue = Glue()
s3 = S3()
dynamodb = DynamoDB()
sqs = SQS("firehoseSyncsToBeProcessedQueue")

SUBSCRIPTION_STATE_TABLE_NAME = "firehoseSubscriptionState"

study_user_manager = get_study_user_manager(load_from_aws=False)


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
                # logger.info(f"Creating new cache directory at {op_path}")
                os.makedirs(op_path)

    # create helper path for writing user activity data.
    if not os.path.exists(study_user_activity_root_local_path):
        os.makedirs(study_user_activity_root_local_path)
        for op in ["create", "delete"]:
            for op_type in [
                "post",
                "like",
                "follow",
                "like_on_user_post",
                "reply_to_user_post",
            ]:
                op_path = os.path.join(study_user_activity_root_local_path, op, op_type)
                if not os.path.exists(op_path):
                    # logger.info(f"Creating new cache directory at {op_path}")
                    os.makedirs(op_path)
                if op_type == "follow":
                    for follow_type in ["followee", "follower"]:
                        follow_path = os.path.join(op_path, follow_type)
                        if not os.path.exists(follow_path):
                            os.makedirs(follow_path)

    # create helper path for writing in-network activity
    if not os.path.exists(in_network_user_activity_create_post_local_path):
        # logger.info(f"Creating new cache directory at {in_network_user_activity_create_post_local_path}")
        os.makedirs(in_network_user_activity_create_post_local_path)


def delete_cache_paths():
    """Deletes the cache paths. Recursively removes from the root path."""
    if os.path.exists(root_write_path):
        shutil.rmtree(root_write_path)


# if not running in a lambda container, rebuild the cache paths.
if not os.path.exists("/app"):
    rebuild_cache_paths()


def write_data_to_json(data: dict, path: str):
    with open(path, "w") as f:
        json.dump(data, f)


def compress_cached_files_and_write_to_storage(
    directory: str,
    operation: Literal["create", "delete"],
    operation_type: Literal["post", "like", "follow"],
    compressed: bool = True,
    external_store: Literal["local", "s3"] = "s3",
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
        if operation_type == "post" and operation == "create":
            # write post to separate location to track daily posts (for daily
            # superposter calculation)
            daily_posts_dicts: list[dict] = []
            for post in json_dicts:
                author_did = post["author_did"]
                uri = post["uri"]
                payload = {"author_did": author_did, "uri": uri}
                daily_posts_dicts.append(payload)
            # daily-posts is hardcoded in the Glue tf config.
            daily_posts_key = os.path.join("daily-posts", filename)
            s3.write_dicts_jsonl_to_s3(data=daily_posts_dicts, key=daily_posts_key)  # noqa
            logger.info(
                f"Exported {len(daily_posts_dicts)} daily posts to S3 for daily superposter calculation"
            )  # noqa

    elif external_store == "local":
        full_export_filepath = os.path.join(root_local_data_directory, full_key)  # noqa
        write_jsons_to_local_store(
            source_directory=directory, export_filepath=full_export_filepath
        )
    else:
        raise ValueError("Invalid export store.")


def export_general_firehose_sync(
    compressed: bool = True,
    external_store: list[Literal["local", "s3"]] = ["local", "s3"],
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
                    external_store=store,
                )


def export_study_user_post_local(base_path: str) -> tuple[list[dict], list[str]]:
    """Exports the post data of study users to external S3 store.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/post/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json
    """  # noqa
    key_root: list[str] = base_path.split("/")[-2:]  # ['study_user_activity', 'create']
    key_root.append("post")
    # base_key = "/".join(key_root)

    jsons: list[dict] = []
    filepaths: list[str] = []

    # loop through all the files in the post directory and write them to S3.
    posts_filenames: list[str] = os.listdir(os.path.join(base_path, "post"))
    for path in posts_filenames:
        full_path = os.path.join(base_path, "post", path)
        # full_key = os.path.join(base_key, path)
        # superposter_key = os.path.join("daily-posts", path)
        with open(full_path, "r") as f:
            data = json.load(f)
            # s3.write_dict_json_to_s3(data=data, key=full_key)
            jsons.append(data)
            filepaths.append(full_path)
            # TODO: migrate superposter daily posts implementation off
            # this approach and query the firehose Athena table directly.
            # Write post to superposter daily posts.
            # superposter_dict = {"author_did": data["author_did"], "uri": data["uri"]}
            # s3.write_dict_json_to_s3(data=superposter_dict, key=superposter_key)

    # logger.info(
    #     f"Exported {len(posts_filenames)} post records to S3 for study user DID {key_root[1]}."
    # )  # noqa
    return jsons, filepaths


def export_study_user_follow_local(base_path: str) -> tuple[list[dict], list[str]]:
    """Exports the follow data of study users to external S3 store."""
    key_root: list[str] = base_path.split("/")[-2:]  # ['study_user_activity', 'create']
    key_root.append("follow")
    base_key = "/".join(key_root)
    scraped_user_social_network: list[dict] = []
    filepaths: list[str] = []
    # loop through follow/follower subdirectories and export follow data to S3.
    total_records = 0
    follows_path = os.path.join(base_path, "follow")
    for follow_type in os.listdir(follows_path):
        follow_path = os.path.join(base_path, "follow", follow_type)
        follow_records = os.listdir(follow_path)
        for filepath in follow_records:
            # Write 1: write to study_user_activity
            full_path = os.path.join(follow_path, filepath)
            full_key = os.path.join(base_key, follow_type, filepath)
            # Write 2: write to scraped-user-social-network (so we can add it
            # to the running list of in-network users).
            # TODO: resync the study_user_manager singleton.
            split_key = full_key.split("/")
            # filename = split_key[-1]
            follow_type = split_key[-2]  # followee/follower
            # user_social_network_key = os.path.join(
            #     "scraped-user-social-network", filename
            # )
            with open(full_path, "r") as f:
                data = json.load(f)
                filepaths.append(full_path)
                # Write 1: write to study_user_activity
                # s3.write_dict_json_to_s3(data=data, key=full_key)
                # logger.info(f"Wrote a new follow record to S3 at {full_key}")
                # Write 2: write to scraped-user-social-network
                # NOTE: I implemented this backwards: from the firehose side,
                # I wrote followee/follower from the PoV of the study user (i.e.,
                # is the study user the followee, or is the study user the follower).
                # Therefore, "study_user_activity/{user_did}/create/follow/followee"
                # actually gives the list of all followers, since these are the profiles
                # for which the study user is the followee. Conversely,
                # "study_user_activity/{user_did}/create/follow/follower"
                # actually gives the list of all followees, since these are the
                # profiles for which the study user is the follower. Confusing,
                # yes, but not worth going back and changing for now tbh.
                if follow_type == "followee":
                    # user is followee (so someone is following them)
                    # if user is followee, then the other person is a follower
                    user_social_network_data = {
                        "follow_did": data["followee_did"],
                        "follow_handle": None,
                        "follow_url": None,
                        "follower_did": data["follower_did"],
                        "follower_handle": None,
                        "follower_url": None,
                        "insert_timestamp": generate_current_datetime_str(),
                        "relationship_to_study_user": "follower",
                    }
                elif follow_type == "follower":
                    # user is follower (so they're following someone else)
                    user_social_network_data = {
                        "follow_did": data["followee_did"],
                        "follow_handle": None,
                        "follow_url": None,
                        "follower_did": data["follower_did"],
                        "follower_handle": None,
                        "follower_url": None,
                        "insert_timestamp": generate_current_datetime_str(),
                        "relationship_to_study_user": "follow",  # same as 'followee'
                    }
                # logger.info(
                #     f"Writing a new {'follow' if follow_type == 'follower' else 'follower'} for user social network data to S3 for {user_social_network_key}"
                # )  # noqa
                # s3.write_dict_json_to_s3(
                #     data=user_social_network_data, key=user_social_network_key
                # )
                # follows.append(data)
                scraped_user_social_network.append(user_social_network_data)
            total_records += 1

    # logger.info(
    #     f"Exported {total_records} follow records to S3 for study user DID {key_root[1]}."
    # )  # noqa
    return scraped_user_social_network, filepaths


def export_study_user_like_local(base_path: str) -> tuple[list[dict], list[str]]:
    """Exports the like data of study users to external S3 store.

    The key will be structured as follows:
    {root}/{like_author_did}/{operation}/like/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    `like_author_did` is the DID of the person who liked the post (which should
    be a person in the study). The `post_uri_suffix` is the last part of the
    post URI that was liked. The `like_uri_suffix` is the last part of the URI
    of the like record.
    """  # noqa
    key_root: list[str] = base_path.split("/")[
        -2:
    ]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("like")
    # base_key = "/".join(key_root)

    # loop through all the files in the like directory and write them to S3.
    # total_records = 0
    likes: list[dict] = []
    filepaths: list[str] = []
    liked_posts_path = os.path.join(base_path, "like")
    for post_uri in os.listdir(liked_posts_path):
        post_path = os.path.join(base_path, "like", post_uri)
        for like_record in os.listdir(post_path):
            full_path = os.path.join(post_path, like_record)
            # full_key = os.path.join(base_key, post_uri, like_record)
            with open(full_path, "r") as f:
                data = json.load(f)
                likes.append(data)
                filepaths.append(full_path)
                # s3.write_dict_json_to_s3(data=data, key=full_key)
            # total_records += 1

    # logger.info(
    #     f"Exported {total_records} like records to S3 for study user DID {key_root[1]}."
    # )  # noqa
    return likes, filepaths


def export_like_on_study_user_post_local(
    base_path: str,
) -> tuple[list[dict], list[str]]:
    """Exports a like on a post by a user in the study.

    The key will be structured as follows:
    {root}/{author_did}/{operation}/like_on_user_post/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json
    """  # noqa
    key_root: list[str] = base_path.split("/")[
        -2:
    ]  # ['study_user_activity', '{author_did}', 'create']
    key_root.append("like_on_user_post")
    # base_key = "/".join(key_root)

    # loop through all the files in the like_on_user_post directory and write them to S3.
    # total_records = 0
    likes_on_user_posts: list[dict] = []
    filepaths: list[str] = []
    liked_posts_path = os.path.join(base_path, "like_on_user_post")
    for post_uri in os.listdir(liked_posts_path):
        post_path = os.path.join(base_path, "like_on_user_post", post_uri)
        for like_record in os.listdir(post_path):
            full_path = os.path.join(post_path, like_record)
            # full_key = os.path.join(base_key, post_uri, like_record)
            with open(full_path, "r") as f:
                data = json.load(f)
                # s3.write_dict_json_to_s3(data=data, key=full_key)
                likes_on_user_posts.append(data)
                filepaths.append(full_path)
    # logger.info(
    #     f"Exported {total_records} like on user post records to S3 for study user DID {key_root[1]}."
    # )  # noqa
    return likes_on_user_posts, filepaths


def export_reply_to_study_user_post_local(
    base_path: str,
) -> tuple[list[dict], list[str]]:
    """Exports a reply to a study user's post.

    The key will be structured as follows:
    {root}/{root/parent_author_did}/{operation}/reply_to_user_post/{root/parent_post_uri_suffix}/author_did={author_did}_post_uri_suffix={post_uri_suffix}.json

    Where `root/parent_author_did` is the DID of the author of the root/parent
    post (which should be the study user) and `root/parent_post_uri_suffix` is
    the last part of the URI of the root/parent post. The `author_did` is the
    DID of the person who wrote the reply. The `post_uri_suffix` is the last
    part of the URI of the reply record.
    """  # noqa
    key_root: list[str] = base_path.split("/")[-2:]  # ['study_user_activity', 'create']
    key_root.append("reply_to_user_post")
    # base_key = "/".join(key_root)
    replies_to_user_posts: list[dict] = []
    filepaths: list[str] = []
    # loop through all the files in the reply_to_user_post directory and write them to S3.
    # total_records = 0
    reply_posts_path = os.path.join(base_path, "reply_to_user_post")
    for post_uri in os.listdir(reply_posts_path):
        post_path = os.path.join(base_path, "reply_to_user_post", post_uri)
        for reply_record in os.listdir(post_path):
            full_path = os.path.join(post_path, reply_record)
            # full_key = os.path.join(base_key, post_uri, reply_record)
            with open(full_path, "r") as f:
                data = json.load(f)
                # s3.write_dict_json_to_s3(data=data, key=full_key)
                replies_to_user_posts.append(data)
                filepaths.append(full_path)
            # total_records += 1

    # logger.info(
    #     f"Exported {total_records} reply to user post records to S3 for study user DID {key_root[1]}."
    # )  # noqa
    return replies_to_user_posts, filepaths


def export_study_user_activity_local_data() -> list[str]:
    """Exports the activity data of study users to external S3 store.

    Steps:
    1. Recursively list all the user DIDs in the study_user_activity directory.
    2. Takes the files for each user DID.
    3. Dump to s3.

    list out all study user DIDs that appeared in this batch. These are
    all the users for whom we got activities for in this batch of the
    firehose.
    """
    all_posts: list[dict] = []
    all_likes: list[dict] = []
    all_follows: list[dict] = []
    all_likes_on_user_posts: list[dict] = []
    all_replies_to_user_posts: list[dict] = []

    all_filepaths: list[str] = []

    for operation in ["create", "delete"]:
        if operation == "create":
            create_path = os.path.join(study_user_activity_root_local_path, "create")
            # ["post", "like", "follow", "like_on_user_post", "reply_to_user_post"]
            # (but might not be, and likely won't be, all available at once)
            # (it is much more likely that a subset, maybe only 1 of these,
            # is actually available in a given batch).
            record_types = os.listdir(create_path)
            for record_type in record_types:
                if record_type == "post":
                    posts, filepaths = export_study_user_post_local(
                        base_path=create_path
                    )
                    all_posts.extend(posts)
                    all_filepaths.extend(filepaths)
                elif record_type == "like":
                    likes, filepaths = export_study_user_like_local(
                        base_path=create_path
                    )
                    all_likes.extend(likes)
                    all_filepaths.extend(filepaths)
                elif record_type == "follow":
                    # NOTE: we export this to "scraped-user-social-network" to update
                    # the user's social network.
                    scraped_user_social_network, filepaths = (
                        export_study_user_follow_local(base_path=create_path)
                    )
                    all_follows.extend(scraped_user_social_network)
                    all_filepaths.extend(filepaths)
                elif record_type == "like_on_user_post":
                    likes_on_user_posts, filepaths = (
                        export_like_on_study_user_post_local(base_path=create_path)
                    )  # noqa
                    all_likes_on_user_posts.extend(likes_on_user_posts)
                    all_filepaths.extend(filepaths)
                elif record_type == "reply_to_user_post":
                    replies_to_user_posts, filepaths = (
                        export_reply_to_study_user_post_local(base_path=create_path)
                    )  # noqa
                    all_replies_to_user_posts.extend(replies_to_user_posts)
                    all_filepaths.extend(filepaths)
        elif operation == "delete":
            # deletions are more of an edge case, might have to deal with
            # it at some point but it's much lower priority.
            pass

    # export data
    if len(all_posts) > 0:
        logger.info(f"Exporting {len(all_posts)} study user post records.")
        dtypes_map = MAP_SERVICE_TO_METADATA["study_user_activity"]["dtypes_map"]
        df = pd.DataFrame(all_posts)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtypes_map)
        custom_args = {"record_type": "post"}
        export_data_to_local_storage(
            df=df, service="study_user_activity", custom_args=custom_args
        )
    if len(all_likes) > 0:
        logger.info(f"Exporting {len(all_likes)} study user like records    .")
        df = pd.DataFrame(all_likes)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        custom_args = {"record_type": "like"}
        dtypes_map = MAP_SERVICE_TO_METADATA["study_user_likes"]["dtypes_map"]
        df = df.astype(dtypes_map)
        export_data_to_local_storage(
            df=df, service="study_user_activity", custom_args=custom_args
        )
    if len(all_follows) > 0:
        logger.info(f"Exporting {len(all_follows)} study user follow records.")
        dtypes_map = MAP_SERVICE_TO_METADATA["scraped_user_social_network"][
            "dtypes_map"
        ]
        df = pd.DataFrame(all_follows)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtypes_map)
        export_data_to_local_storage(df=df, service="scraped_user_social_network")
        # if there are any new followed accounts, then we want to reload the list
        # of followed accounts.
        if any(
            [follow["relationship_to_study_user"] == "follow" for follow in all_follows]
        ):
            # reload the list of study follows/followers.
            study_user_manager.in_network_user_dids_set = (
                study_user_manager._load_in_network_user_dids(is_refresh=True)  # noqa
            )
    if len(all_likes_on_user_posts) > 0:
        logger.info(
            f"Exporting {len(all_likes_on_user_posts)} study user like on user post records."
        )
        df = pd.DataFrame(all_likes_on_user_posts)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        custom_args = {"record_type": "like_on_user_post"}
        export_data_to_local_storage(
            df=df, service="study_user_activity", custom_args=custom_args
        )
    if len(all_replies_to_user_posts) > 0:
        logger.info(
            f"Exporting {len(all_replies_to_user_posts)} study user reply to user post records."
        )
        df = pd.DataFrame(all_replies_to_user_posts)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        custom_args = {"record_type": "reply_to_user_post"}
        export_data_to_local_storage(
            df=df, service="study_user_activity", custom_args=custom_args
        )
    return all_filepaths


def export_in_network_user_activity_local_data():
    """Exports the activity data of in-network users to external S3 store."""
    key_root = os.path.join("in_network_user_activity", "create", "post")
    author_dids: list[str] = os.listdir(os.path.join(root_write_path, key_root))  # noqa
    all_s3_keys: list[str] = []
    jsons: list[dict] = []
    filepaths: list[str] = []
    for author_did in author_dids:
        post_filenames: list[str] = os.listdir(
            os.path.join(root_write_path, key_root, author_did)
        )
        for post_filename in post_filenames:
            full_key = os.path.join(key_root, author_did, post_filename)
            all_s3_keys.append(full_key)
            filepath = os.path.join(root_write_path, full_key)
            with open(filepath, "r") as f:
                data = json.load(f)
                jsons.append(data)
                filepaths.append(filepath)
    if len(jsons) > 0:
        # timestamp = generate_current_datetime_str()
        # filename = f"{timestamp}.jsonl"
        # full_s3_path = os.path.join(key_root, filename)
        # s3.write_dicts_jsonl_to_s3(data=jsons, key=full_s3_path)
        dtype_map = MAP_SERVICE_TO_METADATA["in_network_user_activity"]["dtypes_map"]
        df = pd.DataFrame(jsons)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        df = df.astype(dtype_map)
        export_data_to_local_storage(df=df, service="in_network_user_activity")
        logger.info(f"Exported {len(jsons)} in-network user post records.")
    return filepaths


def export_batch(
    compressed: bool = True,
    clear_cache: bool = True,
    external_store: list[Literal["local", "s3"]] = ["local", "s3"],
    clear_filepaths: bool = False,
):
    """Writes the batched data to external stores (local store and/or S3 store).

    Crawls the "created" and "deleted" folders and updates the records
    where necessary.

    Then deletes the local cache.

    Exports both the general firehose sync data and the study user activity data.
    that has been tracked in this batch.
    """  # noqa
    logger.info("Exporting batch.")
    write_all_data_to_s3 = False
    if write_all_data_to_s3:
        export_general_firehose_sync(
            compressed=compressed, external_store=external_store
        )
    logger.info("Exporting study user activity data.")
    study_user_activity_filepaths = export_study_user_activity_local_data()
    logger.info("Finished exporting study user activity data.")
    logger.info("Exporting in-network user activity data.")
    in_network_user_activity_filepaths = export_in_network_user_activity_local_data()
    logger.info("Finished exporting in-network user activity data.")
    all_filepaths = study_user_activity_filepaths + in_network_user_activity_filepaths

    if clear_filepaths:
        logger.info(f"Clearing {len(all_filepaths)} filepaths.")
        logger.info(
            f"# of study user activity filepaths: {len(study_user_activity_filepaths)}"
        )
        logger.info(
            f"# of in-network user activity filepaths: {len(in_network_user_activity_filepaths)}"
        )
        for filepath in all_filepaths:
            os.remove(filepath)
        services = ["study_user_activity", "in_network_user_activity"]
        local_prefixes = []
        for service in services:
            local_prefixes.extend(get_local_prefixes_for_service(service))
        for prefix in local_prefixes:
            delete_empty_folders(prefix)
        logger.info("Cleared all empty folders and deleted old files..")
    else:
        # clears cache for next batch.
        if clear_cache:
            delete_cache_paths()
        rebuild_cache_paths()


def update_cursor_state_dynamodb(
    cursor_model: FirehoseSubscriptionStateCursorModel,
) -> None:
    """Updates the cursor state in DynamoDB."""
    item = cursor_model.dict()
    dynamodb.insert_item_into_table(table_name="firehoseSubscriptionState", item=item)


def load_cursor_state_dynamodb(
    service_name: str,
) -> Optional[FirehoseSubscriptionStateCursorModel]:  # noqa
    """Loads the cursor state from DynamoDB, if it exists. If not, return
    None."""
    key = {"service": {"S": service_name}}
    result: Optional[dict] = dynamodb.get_item_from_table(
        table_name=SUBSCRIPTION_STATE_TABLE_NAME, key=key
    )
    if not result:
        return None
    return FirehoseSubscriptionStateCursorModel(**result)


def update_cursor_state_s3(cursor_model: FirehoseSubscriptionStateCursorModel) -> None:
    """Updates the cursor state in S3."""
    key = os.path.join("sync", "firehose", "cursor", f"{cursor_model.service}.json")  # noqa
    s3.write_dict_json_to_s3(data=cursor_model.dict(), key=key)


def load_cursor_state_s3(
    service_name: str,
) -> Optional[FirehoseSubscriptionStateCursorModel]:  # noqa
    """Loads the cursor state from S3, if it exists. If not, return None."""
    key = os.path.join("sync", "firehose", "cursor", f"{service_name}.json")
    result: Optional[dict] = s3.read_json_from_s3(key=key)
    if not result:
        return None
    return FirehoseSubscriptionStateCursorModel(**result)


def export_study_user_post(
    record: dict, operation: Literal["create", "delete"], author_did: str, filename: str
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
    root_path = study_user_activity_root_local_path
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
        post_uri=record["uri"], user_did=author_did
    )


def export_study_user_follow(
    record: dict,
    operation: Literal["create", "delete"],
    author_did: str,
    filename: str,
    follow_status: Optional[Literal["follower", "followee"]] = None,
):
    """Exports a follow record for a study user.

    Whenever a study participant follows another user, we want to track
    them as a "follower". Whenever a study participant is followed by another
    user, we want to track them as a "followee".

    The key will be structured as follows:
    {root}/{operation}/follow/{follower or followee}/{follower_did={follower_did}_followee_did={followee_did}.json

    Where `author_did` is the DID of the person who is being followed or is
    following someone, whichever is the study user. The `follower_did` is the
    DID of the user that is following the account. The `followee_did` is the
    DID of the user that is being followed.
    """
    if not follow_status:
        raise ValueError("Follow status must be provided for follow records.")
    relative_path = study_user_activity_relative_path_map[operation]["follow"][
        follow_status
    ]
    root_path = study_user_activity_root_local_path
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    print(f"Writing follow record to {full_path}")
    breakpoint()
    write_data_to_json(data=record, path=full_path)


def export_study_user_like(
    record: dict, operation: Literal["create", "delete"], author_did: str, filename: str
):
    """Exports a like record for a study user.

    The key will be structured as follows:
    {root}/{operation}/like/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    Where `like_author_did` is the DID of the person who liked the post (which
    should be a person in the study). The `post_uri_suffix` is the last part of
    the post URI that was liked. The `like_uri_suffix` is the last part of the
    URI of the like record.
    """
    relative_path = study_user_activity_relative_path_map[operation]["like"]
    post_uri_suffix = record["record"]["subject"]["uri"].split("/")[-1]
    record["record"] = json.dumps(record["record"])
    root_path = study_user_activity_root_local_path
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(root_path, relative_path, post_uri_suffix)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_like_on_study_user_post(
    record: dict, operation: Literal["create", "delete"], author_did: str, filename: str
):
    """Exports a like on a user post.

    Unlike other exports, this also has the additional key level of the post URI.

    The key will be structured as follows:
    {root}/{operation}/like_on_user_post/{post_uri_suffix}/like_author_did={like_author_did}_like_uri_suffix={uri_suffix}.json

    Where `author_did` is the author of the post that was liked. This should be
    a person in the study. The `post_uri_suffix` is the last part of the URI of
    the post that was liked. The `like_author_did` is the DID of the person who
    liked the post. The `uri_suffix` is the last part of the URI of the like
    record.
    """
    relative_path = study_user_activity_relative_path_map[operation][
        "like_on_user_post"
    ]  # noqa
    post_uri_suffix = record["record"]["subject"]["uri"].split("/")[-1]
    record["record"] = json.dumps(record["record"])
    root_path = study_user_activity_root_local_path
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
    user_post_type: Literal["root", "parent"],
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
    relative_path = study_user_activity_relative_path_map[operation][
        "reply_to_user_post"
    ]
    original_study_user_post_uri_suffix = (
        record["reply_root"].split("/")[-1]
        if user_post_type == "root"
        else record["reply_parent"].split("/")[-1]
    )
    root_path = study_user_activity_root_local_path
    if not os.path.exists(root_path):
        os.makedirs(root_path)
    folder_path = os.path.join(
        root_path, relative_path, original_study_user_post_uri_suffix
    )
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
    kwargs: Optional[dict] = None,
):
    """Writes study user activity to local cache storage."""
    if not kwargs:
        kwargs = {}
    if record_type == "post":
        export_study_user_post(
            record=record, operation=operation, author_did=author_did, filename=filename
        )
    elif record_type == "follow":
        export_study_user_follow(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename,
            follow_status=kwargs.get("follow_status"),
        )
    elif record_type == "like":
        export_study_user_like(
            record=record, operation=operation, author_did=author_did, filename=filename
        )
    elif record_type == "like_on_user_post":
        export_like_on_study_user_post(
            record=record, operation=operation, author_did=author_did, filename=filename
        )
    elif record_type == "reply_to_user_post":
        export_reply_to_study_user_post(
            record=record,
            operation=operation,
            author_did=author_did,
            filename=filename,
            user_post_type=kwargs.get("user_post_type"),
        )


def export_in_network_user_post(record: dict, author_did: str, filename: str):
    """Exports a post record for an in-network user."""
    folder_path = os.path.join(
        in_network_user_activity_root_local_path, "create", "post", author_did
    )
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    full_path = os.path.join(folder_path, filename)
    write_data_to_json(data=record, path=full_path)


def export_in_network_user_data_local(
    record: dict,
    record_type: Literal["post", "follow", "like"],
    author_did: str,
    filename: str,
):
    """Writes in-network user activity to local cache storage."""
    if record_type == "post":
        export_in_network_user_post(
            record=record, author_did=author_did, filename=filename
        )
    else:
        # NOTE: no use to implement follow/like yet, since we currently
        # only care about when an in-network user writes a post (so that,
        # for example, we can recommend users posts that others in their
        # network published)
        pass
