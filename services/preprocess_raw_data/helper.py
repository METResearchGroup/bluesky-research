"""Helper code for running filters on raw data."""

from datetime import datetime, timedelta, timezone
from typing import Literal

from lib.aws.sqs import SQS
from lib.constants import current_datetime_str, timestamp_format
from lib.helper import track_performance
from lib.log.logger import Logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.export_data import (
    export_latest_follows,
    export_latest_likes,
    export_latest_preprocessed_posts,
    export_session_metadata,
)
from services.preprocess_raw_data.load_data import (
    load_latest_posts,
    load_previous_session_metadata,
)
from services.preprocess_raw_data.preprocess import (
    preprocess_latest_posts,
    preprocess_latest_likes,
    preprocess_latest_follows,
)

DEFAULT_BATCH_SIZE = 100000
num_days_lookback = 1
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
).strftime(timestamp_format)

logger = Logger(__name__)

firehose_sqs_queue = SQS("firehoseSyncsToBeProcessedQueue")
most_liked_sqs_queue = SQS("mostLikedSyncsToBeProcessedQueue")


def init_session_data(previous_timestamp: str) -> dict:
    """Initializes the session data for the current preprocessing run."""
    return {
        "previous_preprocessing_timestamp": previous_timestamp,
        "current_preprocessing_timestamp": current_datetime_str,
        "num_raw_records": {"posts": 0, "likes": 0, "follows": 0},
        "num_records_after_filtering": {
            "posts": {
                "passed": 0,
                "failed_total": 0,
                "failed_breakdown": {
                    "not_english": 0,
                    "has_spam": 0,
                    "has_hate_speech": 0,
                    "has_nsfw": 0,
                    "not_written_by_bot": 0,
                },
            }
        },
    }


def load_latest_firehose_sqs_sync_messages() -> list[dict]:
    """Load the latest Firehose SQS sync messages."""
    messages: list[dict] = firehose_sqs_queue.receive_all_messages()
    # TODO: should be parsed and process to split up based on post/likes/follows
    # as well as study user activity.
    return messages


def load_latest_most_liked_sqs_sync_messages() -> list[dict]:
    """Load the latest Most Liked SQS sync messages."""
    messages: list[dict] = most_liked_sqs_queue.receive_all_messages()
    return messages


# TODO: process latest study user activity as well?
def load_latest_sqs_sync_messages(
    sources: list[str] = Literal["in-network-user-activity", "most_liked"],
) -> list[dict]:
    """Load the latest SQS sync messages.

    Output dictionary follows the same tree structure as in S3.
    """
    res = {
        "in-network-user-activity": {
            "create": {"post": [], "like": [], "follow": []},
            "delete": {"post": [], "like": [], "follow": []},
        },
        "most_liked": [],
    }
    if "in-network-user-activity" in sources:
        latest_firehose_sqs_sync_messages: list[dict] = (
            load_latest_firehose_sqs_sync_messages()
        )  # noqa
        for message in latest_firehose_sqs_sync_messages:
            object_body = message["Body"]["data"]["sync"]
            operation = object_body["operation"]
            operation_type = object_body["operation_type"]
            res["in-network-user-activity"][operation][operation_type].append(message)
    if "most_liked" in sources:
        latest_most_liked_sqs_sync_messages: list[dict] = (
            load_latest_most_liked_sqs_sync_messages()
        )  # noqa
        for message in latest_most_liked_sqs_sync_messages:
            res["most_liked"].append(message)
    return res


def get_latest_post_filenames_from_sqs(sqs_sync_messages: dict) -> dict[str, list[str]]:  # noqa
    """Process SQS messages to get the filenames to load."""
    res = {"in-network-user-activity": [], "most_liked": []}
    firehose_post_sqs_messages = sqs_sync_messages["in-network-user-activity"][
        "create"
    ]["post"]  # noqa
    most_liked_post_sqs_messages = sqs_sync_messages["most_liked"]
    for message in firehose_post_sqs_messages:
        keys: list[str] = message["Body"]["data"]["sync"]["s3_keys"]
        res["in-network-user-activity"].extend(keys)
    for message in most_liked_post_sqs_messages:
        res["most_liked"].append(message["Body"]["data"]["sync"]["s3_key"])
    return res


@track_performance
def preprocess_latest_raw_data():
    """Preprocesses the latest raw data."""
    logger.info(f"Preprocessing the latest raw data at {current_datetime_str}.")
    previous_session_metadata: dict = load_previous_session_metadata()
    if previous_session_metadata:
        previous_timestamp = previous_session_metadata[
            "current_preprocessing_timestamp"
        ]  # noqa
    else:
        previous_timestamp = None

    session_metadata: dict = init_session_data(previous_timestamp=previous_timestamp)  # noqa

    if not previous_timestamp:
        previous_timestamp = default_latest_timestamp

    # can do this in lieu of the logic below to load latest_posts, latest_likes, and latest_follows.
    # TODO: add firehose back in once we have the firehose data in S3.
    sqs_sync_messages: dict = load_latest_sqs_sync_messages(
        sources=["in-network-user-activity", "most_liked"]
    )  # noqa
    total_messages_start_of_job = (
        len(sqs_sync_messages["in-network-user-activity"]["create"]["post"])
        + len(sqs_sync_messages["in-network-user-activity"]["create"]["like"])
        + len(sqs_sync_messages["in-network-user-activity"]["create"]["follow"])
        + len(sqs_sync_messages["most_liked"])
    )

    # get latest filenames to load.
    latest_post_file_keys: dict = get_latest_post_filenames_from_sqs(
        sqs_sync_messages=sqs_sync_messages
    )  # noqa
    latest_firehose_post_file_keys: list[str] = latest_post_file_keys[
        "in-network-user-activity"
    ]
    latest_most_liked_post_file_keys: list[str] = latest_post_file_keys["most_liked"]
    logger.info(
        f"Processing {len(latest_firehose_post_file_keys)} in-network-user-activity post files and {len(latest_most_liked_post_file_keys)} most-liked post files..."
    )  # noqa
    latest_likes_file_keys: list[str] = []
    latest_follows_file_keys: list[str] = []
    logger.info(
        f"Processing {len(latest_likes_file_keys)} like files and {len(latest_follows_file_keys)} follow files..."
    )  # noqa

    # load latest posts based on filenames in the sqs messages.
    post_keys = latest_firehose_post_file_keys + latest_most_liked_post_file_keys  # noqa
    if len(post_keys) == 0:
        logger.warning("No posts to process. Check if this is expected behavior.")  # noqa
    latest_posts: list[ConsolidatedPostRecordModel] = load_latest_posts(
        post_keys=post_keys
    )  # noqa
    latest_likes = []
    latest_follows = []

    # we export only the posts that have passed preprocessing
    passed_posts, posts_metadata = preprocess_latest_posts(latest_posts=latest_posts)  # noqa
    preprocessed_likes, likes_metadata = preprocess_latest_likes(
        latest_likes=latest_likes
    )  # noqa
    preprocessed_follows, follows_metadata = preprocess_latest_follows(
        latest_follows=latest_follows
    )  # noqa

    session_metadata["num_raw_records"]["posts"] = posts_metadata["num_posts"]
    session_metadata["num_records_after_filtering"]["posts"] = posts_metadata[
        "num_records_after_filtering"
    ]["posts"]  # noqa
    session_metadata["num_raw_records"]["likes"] = likes_metadata["num_likes"]
    session_metadata["num_raw_records"]["follows"] = follows_metadata["num_follows"]

    export_latest_preprocessed_posts(
        latest_posts=passed_posts,
        session_metadata=session_metadata,
        external_stores=["s3"],
    )
    export_latest_likes(preprocessed_likes)
    export_latest_follows(preprocessed_follows)
    export_session_metadata(session_metadata)
    logger.info(f"Preprocessing completed at {current_datetime_str}.")

    # clear the SQS queue of the processed messages.
    total_messages_end_of_job = (
        len(sqs_sync_messages["in-network-user-activity"]["create"]["post"])
        + len(sqs_sync_messages["in-network-user-activity"]["create"]["like"])
        + len(sqs_sync_messages["in-network-user-activity"]["create"]["follow"])
        + len(sqs_sync_messages["most_liked"])
    )
    if total_messages_start_of_job == total_messages_end_of_job:
        logger.info(
            "All messages have been processed, deleting all messages from the queue."
        )  # noqa
    else:
        # TODO: if the messages expire, might have to re-fetch the messages
        # and then filter those against the IDs of the ones that we processed,
        # in order to delete the ones that we've processed but whose visibility
        # timeout has expired.
        logger.warning(
            "Some messages might have timed out. Need to modify the visibility timeout."
        )  # noqa
    firehose_sqs_queue.delete_messages(
        messages=sqs_sync_messages["in-network-user-activity"]["create"]["post"]
    )
    firehose_sqs_queue.delete_messages(
        messages=sqs_sync_messages["in-network-user-activity"]["create"]["like"]
    )
    firehose_sqs_queue.delete_messages(
        messages=sqs_sync_messages["in-network-user-activity"]["create"]["follow"]
    )
    firehose_sqs_queue.delete_messages(
        messages=sqs_sync_messages["in-network-user-activity"]["delete"]["post"]
    )
    firehose_sqs_queue.delete_messages(
        messages=sqs_sync_messages["in-network-user-activity"]["delete"]["like"]
    )
    firehose_sqs_queue.delete_messages(
        messages=sqs_sync_messages["in-network-user-activity"]["delete"]["follow"]
    )
    most_liked_sqs_queue.delete_messages(messages=sqs_sync_messages["most_liked"])
