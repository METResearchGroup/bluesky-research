"""Helper tooling for ML inference."""

import json
from multiprocessing import Pool, cpu_count
from typing import Literal, Optional

import pandas as pd

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.db.manage_local_data import load_latest_data
from lib.helper import track_performance
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
    timestamp: Optional[str] = None,
    max_per_source: Optional[int] = None,
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

    if timestamp is not None:
        logger.info(
            f"Using backfill timestamp {timestamp} instead of latest inference timestamp: {latest_inference_timestamp}"
        )  # noqa
    else:
        timestamp = latest_inference_timestamp
    logger.info(f"Getting posts to classify for inference type {inference_type}.")  # noqa
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")
    posts_df: pd.DataFrame = load_latest_data(
        service=(
            "ml_inference_perspective_api"
            if inference_type == "perspective_api"
            else "ml_inference_sociopolitical"
        ),
        latest_timestamp=timestamp,
        max_per_source=max_per_source,
    )
    posts_df = posts_df.drop_duplicates(subset=["uri"])
    df_dicts = posts_df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    posts = [FilteredPreprocessedPostModel(**post_dict) for post_dict in df_dicts]  # noqa
    return posts


def json_file_reader(file_paths):
    for path in file_paths:
        with open(path, "r") as file:
            yield json.loads(file.read())


def process_file(file_path) -> list[dict]:
    """Loads the .jsonl files at a given path."""
    results = []
    with open(file_path, 'r') as file:
        for line in file:
            results.append(json.loads(line))
    return results


@track_performance
def load_cached_jsons_as_df(filepaths: list[str], dtypes_map: dict) -> Optional[pd.DataFrame]:
    """Loads a list of JSON filepaths into a pandas DataFrame."""
    num_processes = cpu_count()
    logger.info(f"Using {num_processes} processes to load data...")
    with Pool(num_processes) as pool:
        results = pool.map(process_file, filepaths)
    flattened_results = [item for sublist in results for item in sublist]
    if len(flattened_results) == 0:
        return None
    df = pd.DataFrame(flattened_results)
    df = df.astype(dtypes_map)
    df = df.drop_duplicates(subset="uri")
    return df
