"""Helper tooling for ML inference."""

from typing import Literal

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.log.logger import get_logger


dynamodb = DynamoDB()
athena = Athena()

logger = get_logger(__name__)


def get_latest_labeling_session(
    inference_type: Literal["llm", "perspective_api"],
) -> dict:  # noqa
    """Get the latest labeling session for the inference type."""
    return dynamodb.get_latest_labeling_session(inference_type)


def insert_labeling_session(labeling_session: dict):
    """Insert a labeling session."""
    dynamodb.insert_labeling_session(labeling_session)


def get_posts_to_classify(inference_type: Literal["llm", "perspective_api"]):
    """Get posts to classify.

    Steps:
    - Takes the inference type
    - Loads in the latest labeling session for the inference type (from DynamoDB)
        - I can create one table for all labeling sessions and just change the “inference_type” value.
    - Get the latest inference timestamp
    - Use that as a filter for labeling.
    - Get the rows of data to label.
    """
    # TODO: implement.
    latest_labeling_session = get_latest_labeling_session(inference_type)
    if latest_labeling_session is None:
        logger.info(
            "No latest labeling session found. By default, labeling all posts..."
        )  # noqa
        latest_inference_timestamp = None
    else:
        latest_inference_timestamp = latest_labeling_session[
            "latest_inference_timestamp"
        ]  # noqa

    logger.info(f"Getting posts to classify for inference type {inference_type}.")  # noqa
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")

    where_filter = (
        f"synctimestamp > '{latest_inference_timestamp}'"
        if latest_inference_timestamp
        else "1=1"
    )  # noqa

    query = f"""
    SELECT * FROM preprocessed_firehose_posts
    WHERE {where_filter}
    UNION ALL
    SELECT * FROM preprocessed_most_liked_posts
    WHERE {where_filter}
    """

    df: pd.DataFrame = athena.query_results_as_df(query)

    logger.info(f"Number of posts to classify: {len(df)}")

    return df
