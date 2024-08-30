"""Helper code for running filters on raw data."""

from datetime import datetime, timedelta, timezone
import json
from typing import Literal, Optional

from lib.aws.athena import Athena
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

athena = Athena()


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
        "latest_processed_insert_timestamp": None,
    }


def load_latest_sqs_messages_from_athena(
    queue_name: str,
    limit: Optional[int] = None,
    latest_processed_insert_timestamp: Optional[str] = None,
) -> list[dict]:
    """Load the latest Firehose SQS sync messages."""
    where_condition = (
        f"insert_timestamp > '{latest_processed_insert_timestamp}'"
        if latest_processed_insert_timestamp
        else "1=1"
    )
    where_clause = f"""
    WHERE queue_name = '{queue_name}'
    AND {where_condition}
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    # get the oldest messages first.
    query = f"""
    SELECT * FROM sqs_messages
    {where_clause}
    {limit_clause}
    ORDER BY insert_timestamp ASC
    """
    response = athena.query_results_as_df(query)
    messages: list[dict] = response.to_dict(orient="records")
    messages: list[dict] = athena.parse_converted_pandas_dicts(messages)
    for message in messages:
        # data is a stringified json object.
        if "data" in message:
            message["data"] = json.loads(message["data"])
    return messages


def load_latest_sqs_sync_messages(
    sources: list[str] = Literal["in-network-user-activity", "most_liked"],
    latest_processed_insert_timestamp: Optional[str] = None,
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
            load_latest_sqs_messages_from_athena(
                queue_name="firehoseSyncsToBeProcessedQueue",
                limit=None,
                latest_processed_insert_timestamp=latest_processed_insert_timestamp,  # noqa
            )
        )  # noqa
        for message in latest_firehose_sqs_sync_messages:
            object_body = message["data"]["sync"]
            operation = object_body["operation"]
            operation_type = object_body["operation_type"]
            res["in-network-user-activity"][operation][operation_type].append(message)  # noqa
    if "most_liked" in sources:
        latest_most_liked_sqs_sync_messages: list[dict] = (
            load_latest_sqs_messages_from_athena(
                queue_name="mostLikedSyncsToBeProcessedQueue",
                limit=None,
                latest_processed_insert_timestamp=latest_processed_insert_timestamp,
            )
        )  # noqa
        res["most_liked"].extend(latest_most_liked_sqs_sync_messages)
    return res


def get_latest_post_filenames_from_sqs(sqs_sync_messages: dict) -> dict[str, list[str]]:  # noqa
    """Process SQS messages to get the filenames to load."""
    res = {"in-network-user-activity": [], "most_liked": []}
    firehose_post_sqs_messages = sqs_sync_messages["in-network-user-activity"][
        "create"
    ]["post"]  # noqa
    most_liked_post_sqs_messages = sqs_sync_messages["most_liked"]
    for message in firehose_post_sqs_messages:
        keys: list[str] = message["data"]["sync"].get("s3_keys", [])
        if len(keys) == 0:
            # could have old key structure. Should be well-deprecated but
            # some SQS messages before 2024-08-20 might have this. Shouldn't
            # be a problem after that.
            logger.info("Message using old key structure...")
            key = message["data"]["sync"].get("s3_key", [])
            res["in-network-user-activity"].append(key)
        res["in-network-user-activity"].extend(keys)
    for message in most_liked_post_sqs_messages:
        res["most_liked"].append(message["data"]["sync"]["s3_key"])
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

    # used to process posts that were inserted to the SQS queue after the
    # last preprocessing session.
    latest_processed_insert_timestamp = previous_session_metadata.get(
        "latest_processed_insert_timestamp", None
    )

    # can do this in lieu of the logic below to load latest_posts, latest_likes, and latest_follows.
    # TODO: add firehose back in once we have the firehose data in S3.
    sqs_sync_messages: dict = load_latest_sqs_sync_messages(
        sources=["in-network-user-activity", "most_liked"],
        latest_processed_insert_timestamp=latest_processed_insert_timestamp,
    )  # noqa

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
    # get the latest timestamp of the posts that have been processed.
    firehose_posts = sqs_sync_messages["in-network-user-activity"]["create"]["post"]
    firehose_insert_timestamps = [res["insert_timestamp"] for res in firehose_posts]
    if firehose_insert_timestamps:
        max_firehose_insert_timestamp = max(firehose_insert_timestamps)
    else:
        max_firehose_insert_timestamp = None
    most_liked_posts = sqs_sync_messages["most_liked"]
    most_liked_insert_timestamps = [res["insert_timestamp"] for res in most_liked_posts]
    if most_liked_insert_timestamps:
        max_most_liked_insert_timestamp = max(most_liked_insert_timestamps)
    else:
        max_most_liked_insert_timestamp = None
    max_processed_insert_timestamp = (
        max_firehose_insert_timestamp
        if max_most_liked_insert_timestamp is None
        else max_most_liked_insert_timestamp
        if max_firehose_insert_timestamp is None
        else max(max_firehose_insert_timestamp, max_most_liked_insert_timestamp)
    )

    session_metadata["num_raw_records"]["posts"] = posts_metadata["num_posts"]
    session_metadata["num_records_after_filtering"]["posts"] = posts_metadata[
        "num_records_after_filtering"
    ]["posts"]  # noqa
    session_metadata["num_raw_records"]["likes"] = likes_metadata["num_likes"]
    session_metadata["num_raw_records"]["follows"] = follows_metadata["num_follows"]
    session_metadata["latest_processed_insert_timestamp"] = (
        max_processed_insert_timestamp
    )

    export_latest_preprocessed_posts(
        latest_posts=passed_posts,
        session_metadata=session_metadata,
        external_stores=["s3"],
    )
    export_latest_likes(preprocessed_likes)
    export_latest_follows(preprocessed_follows)
    export_session_metadata(session_metadata)
    logger.info(f"Preprocessing completed at {current_datetime_str}.")
