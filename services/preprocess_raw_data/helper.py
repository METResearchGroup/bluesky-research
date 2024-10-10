"""Helper code for running filters on raw data."""

from datetime import datetime, timedelta, timezone
import json
import re
from typing import Optional

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.sqs import SQS
from lib.constants import current_datetime_str, timestamp_format
from lib.helper import track_performance
from lib.log.logger import Logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.export_data import (
    # export_latest_follows,
    # export_latest_likes,
    export_latest_preprocessed_posts,
    export_session_metadata,
)
from services.preprocess_raw_data.load_data import (
    load_latest_firehose_posts,
    load_latest_most_liked_posts,
    load_previous_session_metadata,
)
from services.preprocess_raw_data.preprocess import (
    preprocess_latest_posts,
    # preprocess_latest_likes,
    # preprocess_latest_follows,
)

DEFAULT_BATCH_SIZE = 100000
max_firehose_posts_to_load = None
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
        # let's set, by default, the latest processed insert_timestamp
        # to be equal to the timestamp of the previous session.
        "latest_processed_insert_timestamp": previous_timestamp,
    }


def quote_s3_keys(match):
    keys = match.group(1).split(",")
    quoted_keys = [f'"{key.strip()}"' for key in keys]
    return "[" + ", ".join(quoted_keys) + "]"


def quote_keys(match):
    key = match.group(1)
    valid_keys = ["sync", "source", "operation", "operation_type", "s3_keys"]
    if key in valid_keys:
        return f'"{key}":'


def quote_values(match):
    key, value = match.groups()
    if key in ["source", "operation", "operation_type"]:
        return f'"{key}":"{value.strip()}"'
    return f'"{key}":{value}'


def preprocess_queue_message(message: dict) -> dict:
    """Preprocess the queue message.

    Example:
    >>> message = '{sync:{source:in-network-user-activity, operation:create, operation_type:post, s3_keys:[in_network_user_activity/create/post/did:plc:oky5czdrnfjpqslsw2a5iclo/author_did=did:plc:oky5czdrnfjpqslsw2a5iclo_post_uri_suffix=3l35d7xvwt22z.json]}}'
    >>> preprocess_queue_message(message)
    {'sync': {'source': 'in-network-user-activity', 'operation': 'create', 'operation_type': 'post', 's3_keys': ['in_network_user_activity/create/post/oky5czdrnfjpqslsw2a5iclo/author_did=oky5czdrnfjpqslsw2a5iclo_post_uri_suffix=3l35d7xvwt22z.json']}}
    """
    data = message["data"]
    pattern = r"=(?![^\[]*\])"
    # replace all '=' with ':'
    data = re.sub(pattern, ":", data)
    # surround the items in the s3_keys array with quotes
    result = re.sub(r"\[(.*?)\]", quote_s3_keys, data)
    # Enclose all the keys, which are before the ':', with quotes
    result = re.sub(r"(\w+):", quote_keys, result)
    # Enclose the values, which are after the ':', with quotes
    result = re.sub(r'"(\w+)":\s*([^{[\s][^,}]*)', quote_values, result)
    return json.loads(result)


def load_latest_sqs_messages_from_athena(
    source: str,
    limit: Optional[int] = None,
    latest_processed_insert_timestamp: Optional[str] = None,
) -> list[dict]:
    """Load the latest sync messages."""
    logger.info("Getting latest queue messages.")
    where_condition = (
        f"insert_timestamp > '{latest_processed_insert_timestamp}'"
        if latest_processed_insert_timestamp
        else "1=1"
    )
    where_clause = f"""
    WHERE source = '{source}'
    AND {where_condition}
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    # get the oldest messages first.
    query = f"""
    SELECT * FROM queue_messages
    {where_clause}
    {limit_clause}
    ORDER BY insert_timestamp ASC
    """
    response = athena.query_results_as_df(query)
    messages: list[dict] = response.to_dict(orient="records")
    messages: list[dict] = athena.parse_converted_pandas_dicts(messages)
    for message in messages:
        data = preprocess_queue_message(message)
        message["data"] = data
    return messages


@track_performance
def preprocess_latest_raw_data(
    backfill_period: Optional[str] = None, backfill_duration: Optional[int] = None
):
    """Preprocesses the latest raw data."""
    logger.info(f"Preprocessing the latest raw data at {current_datetime_str}.")
    if backfill_duration is not None and backfill_period in ["days", "hours"]:
        current_time = datetime.now(timezone.utc)
        if backfill_period == "days":
            backfill_time = current_time - timedelta(days=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} days of data.")
        elif backfill_period == "hours":
            backfill_time = current_time - timedelta(hours=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} hours of data.")
    else:
        backfill_time = None
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

    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = latest_processed_insert_timestamp

    # load latest posts
    latest_firehose_posts: list[ConsolidatedPostRecordModel] = (
        load_latest_firehose_posts(
            timestamp=timestamp, limit=max_firehose_posts_to_load
        )
    )
    latest_most_liked_posts: list[ConsolidatedPostRecordModel] = (
        load_latest_most_liked_posts(timestamp=timestamp, limit=None)
    )  # noqa
    latest_posts: list[ConsolidatedPostRecordModel] = (
        latest_firehose_posts + latest_most_liked_posts
    )  # noqa
    logger.info(f"Loaded {len(latest_posts)} posts for preprocessing.")

    # artificially increase the number of posts to 10000x
    new_latest_posts = []
    latest_posts = latest_posts[:800]
    multiplier = 10000  # total of 800 * 10000 = 8mil posts
    for post in latest_posts:
        for idx in range(multiplier):
            new_post = ConsolidatedPostRecordModel(
                uri=f"{post.uri}_{idx}",
                cid=post.cid,
                indexed_at=post.indexed_at,
                author_did=post.author_did,
                author_handle=post.author_handle,
                author_avatar=post.author_avatar,
                author_display_name=post.author_display_name,
                created_at=post.created_at,
                text=post.text,
                embed=post.embed,
                entities=post.entities,
                facets=post.facets,
                labels=post.labels,
                langs=post.langs,
                reply_parent=post.reply_parent,
                reply_root=post.reply_root,
                tags=post.tags,
                synctimestamp=post.synctimestamp,
                url=post.url,
                source=post.source,
                like_count=post.like_count,
                reply_count=post.reply_count,
                repost_count=post.repost_count,
            )
            new_latest_posts.append(new_post)
    latest_posts = new_latest_posts

    latest_posts_df = pd.DataFrame([post.dict() for post in latest_posts])

    # we export only the posts that have passed preprocessing
    if len(latest_posts_df) > 0:
        print(f"Preprocessing {len(latest_posts_df)} posts.")
        passed_posts, posts_metadata = preprocess_latest_posts(
            latest_posts=latest_posts_df
        )  # noqa
        print(f"Preprocessed {len(passed_posts)} posts.")
        # get the max insert timestamp for the firehose posts
        firehose_insert_timestamps = latest_posts_df[
            latest_posts_df["source"] == "firehose"
        ]["synctimestamp"].max()
        if firehose_insert_timestamps:
            max_firehose_insert_timestamp = max(firehose_insert_timestamps)
        else:
            max_firehose_insert_timestamp = None

        # get the max insert timestamp for the most liked posts
        most_liked_insert_timestamps = latest_posts_df[
            latest_posts_df["source"] == "most_liked"
        ]["synctimestamp"].max()

        if most_liked_insert_timestamps:
            max_most_liked_insert_timestamp = max(most_liked_insert_timestamps)
        else:
            max_most_liked_insert_timestamp = None

        # get the max insert timestamp for the posts
        max_processed_insert_timestamp = (
            max_firehose_insert_timestamp
            if max_most_liked_insert_timestamp is None
            else max_most_liked_insert_timestamp
            if max_firehose_insert_timestamp is None
            else max(max_firehose_insert_timestamp, max_most_liked_insert_timestamp)
        )

        # update session metadata
        session_metadata["num_raw_records"]["posts"] = posts_metadata["num_posts"]
        session_metadata["num_records_after_filtering"]["posts"] = posts_metadata[
            "num_records_after_filtering"
        ]["posts"]  # noqa
        session_metadata["latest_processed_insert_timestamp"] = (
            max_processed_insert_timestamp
        )

        breakpoint()

        # export the posts
        export_latest_preprocessed_posts(latest_posts=passed_posts)
    else:
        logger.info("No posts to process.")
    export_session_metadata(session_metadata)
    logger.info(f"Preprocessing completed at {current_datetime_str}.")
