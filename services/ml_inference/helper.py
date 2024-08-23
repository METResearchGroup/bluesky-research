"""Helper tooling for ML inference."""

from typing import Literal

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.log.logger import get_logger


dynamodb = DynamoDB()
athena = Athena()

logger = get_logger(__name__)


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
    latest_labeling_session = dynamodb.get_latest_labeling_session(inference_type)
    latest_inference_timestamp = latest_labeling_session["latest_inference_timestamp"]  # noqa

    logger.info(f"Getting posts to classify for inference type {inference_type}.")  # noqa
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")

    query = f"""
    SELECT * FROM preprocessed_firehose_posts
    WHERE synctimestamp > '{latest_inference_timestamp}'
    UNION ALL
    SELECT * FROM preprocessed_most_liked_posts
    WHERE synctimestamp > '{latest_inference_timestamp}'
    """

    df: pd.DataFrame = athena.query_results_as_df(query)

    logger.info(f"Number of posts to classify: {len(df)}")

    return df
