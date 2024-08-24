"""Helper tooling for ML inference."""

from typing import Literal

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.log.logger import get_logger

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

dynamodb = DynamoDB()
athena = Athena()

logger = get_logger(__name__)
dynamodb_table_name = "ml_inference_labeling_sessions"
MIN_POST_TEXT_LENGTH = 5


def get_latest_labeling_session(
    inference_type: Literal["llm", "perspective_api"],
) -> dict:  # noqa
    """Get the latest labeling session for the inference type."""
    # Query the table to get items by inference_type
    filtered_items = dynamodb.query_items_by_inference_type(
        table_name=dynamodb_table_name,
        inference_type=inference_type,
    )

    if not filtered_items:
        logger.info(
            f"No previous labeling sessions found for inference type {inference_type}."
        )  # noqa
        return None

    # Sort filtered items by inference_timestamp in descending order
    sorted_items = sorted(
        filtered_items,
        key=lambda x: x.get("inference_timestamp", {}).get("S", ""),
        reverse=True,
    )  # noqa

    logger.info(f"Latest labeling session: {sorted_items[0]}")

    # Return the most recent item
    return sorted_items[0]


def insert_labeling_session(labeling_session: dict):
    """Insert a labeling session."""
    try:
        # check that required fields are present
        if "inference_type" not in labeling_session:
            raise ValueError("inference_type is required")
        if "inference_timestamp" not in labeling_session:
            raise ValueError("inference_timestamp is required")
        dynamodb.insert_item_into_table(
            item=labeling_session, table_name=dynamodb_table_name
        )
        logger.info(f"Successfully inserted labeling session: {labeling_session}")
    except Exception as e:
        logger.error(f"Failed to insert labeling session: {e}")
        raise


def get_posts_to_classify(
    inference_type: Literal["llm", "perspective_api"],
    source_tables: list[str] = [
        "preprocessed_firehose_posts",
        "preprocessed_most_liked_posts",
    ],  # noqa
) -> list[FilteredPreprocessedPostModel]:
    """Get posts to classify.

    Steps:
    - Takes the inference type
    - Loads in the latest labeling session for the inference type (from DynamoDB)
        - I can create one table for all labeling sessions and just change the “inference_type” value.
    - Get the latest inference timestamp
    - Use that as a filter for labeling.
    - Get the rows of data to label.
    """
    latest_labeling_session = get_latest_labeling_session(inference_type)
    if latest_labeling_session is None:
        logger.info(
            "No latest labeling session found. By default, labeling all posts..."
        )  # noqa
        latest_inference_timestamp = None
    else:
        latest_inference_timestamp = latest_labeling_session["inference_timestamp"]  # noqa

    logger.info(f"Getting posts to classify for inference type {inference_type}.")  # noqa
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")

    where_filter = (
        f"synctimestamp > '{latest_inference_timestamp}'"
        if latest_inference_timestamp
        else "1=1"
    )  # noqa

    # default syncs both firehose and most-liked tables, but doesn't have to
    # be the case.
    query = " UNION ALL ".join(
        [f"SELECT * FROM {table} WHERE {where_filter}" for table in source_tables]
    )

    df: pd.DataFrame = athena.query_results_as_df(query)

    logger.info(f"Number of posts to classify: {len(df)}")

    df_dicts = df.to_dict(orient="records")
    # convert NaN values to None, remove extra fields.
    fields_to_remove = [
        "year",
        "month",
        "day",
        "hour",
        "minute",
    ]  # extra fields from preprocessing partitions.
    df_dicts_cleaned = [
        {
            k: (None if pd.isna(v) else v)
            for k, v in post.items()
            if k not in fields_to_remove
        }
        for post in df_dicts
    ]
    # remove values without text
    df_dicts_cleaned = [post for post in df_dicts_cleaned if post["text"] is not None]

    # remove posts whose text fields are too short
    df_dicts_cleaned = [
        post for post in df_dicts_cleaned if len(post["text"]) > MIN_POST_TEXT_LENGTH
    ]  # noqa

    # convert to pydantic model
    posts_to_classify = [
        FilteredPreprocessedPostModel(**post) for post in df_dicts_cleaned
    ]

    return posts_to_classify
