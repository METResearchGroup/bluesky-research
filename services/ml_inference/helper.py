"""Helper tooling for ML inference."""

from typing import Literal


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
        latest_inference_timestamp: str = latest_labeling_session[
            "inference_timestamp"
        ]["S"]  # noqa

    logger.info(f"Getting posts to classify for inference type {inference_type}.")  # noqa
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")
    posts = athena.get_latest_preprocessed_posts(
        timestamp=latest_inference_timestamp, source_tables=source_tables
    )
    return posts
