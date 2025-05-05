"""Helper tooling for ML inference."""

import json
from typing import Literal, Optional

import pandas as pd

from lib.db.queue import Queue
from lib.log.logger import get_logger
from lib.helper import track_performance
from lib.utils import filter_posts_df
from services.ml_inference.models import PostToLabelModel


logger = get_logger(__name__)


@track_performance
def get_posts_to_classify(
    inference_type: Literal["llm", "perspective_api", "ime"],
    timestamp: Optional[str] = None,
    previous_run_metadata: Optional[dict] = None,
    columns: Optional[list[str]] = None,
) -> list[dict]:
    """Retrieves posts from the appropriate queue for classification.

    This is the single entry point for getting data for inference. All inference modules
    should use this function to get their data. The function handles queue selection,
    post filtering, and data formatting.

    Args:
        inference_type (Literal["llm", "perspective_api", "ime"]): Type of inference to run.
            Maps to specific queue names:
            - "perspective_api" -> "input_ml_inference_perspective_api"
            - "llm" -> "input_ml_inference_sociopolitical"
            - "ime" -> "input_ml_inference_ime"
        timestamp (Optional[str]): Optional timestamp in YYYY-MM-DD-HH:MM:SS format to
            override latest inference timestamp for filtering posts.
        previous_run_metadata (Optional[dict]): Metadata from previous run containing:
            - metadata (str): JSON string with:
                - latest_id_classified (Optional[int]): ID of last processed post
                - inference_timestamp (Optional[str]): Timestamp of last inference
        columns (Optional[list[str]]): List of columns to return in output.
            Defaults to ["uri", "text", "preprocessing_timestamp", "batch_id", "batch_metadata"].

    Returns:
        list[dict]: List of posts to classify, where each post is a dictionary containing
            the requested columns. Posts are filtered to remove:
            - Duplicates (based on uri)
            - Invalid text (missing, empty, or too short)
            - Missing timestamps
            If a requested column doesn't exist, it will be included with None values.

    Control Flow:
        1. Maps inference type to appropriate queue name
        2. Creates Queue instance for specified inference type
        3. Extracts metadata from previous run if provided
        4. Loads posts from queue with filters:
            - After latest_id_classified (if provided)
            - After latest_inference_timestamp or override timestamp
            - With status="pending"
        5. If no posts found, returns empty list
        6. Creates DataFrame from posts
        7. Drops duplicate URIs
        8. Filters posts using filter_posts_df()
        9. Ensures all requested columns exist (adds missing ones with None)
        10. Returns filtered posts as list of dicts with requested columns
    """
    # Map inference types to queue names
    queue_mapping = {
        "perspective_api": "input_ml_inference_perspective_api",
        "llm": "input_ml_inference_sociopolitical",
        "ime": "input_ml_inference_ime",
    }

    if inference_type not in queue_mapping:
        raise ValueError(f"Invalid inference type: {inference_type}")

    queue = Queue(queue_name=queue_mapping[inference_type])

    if previous_run_metadata is not None:
        latest_job_metadata = json.loads(previous_run_metadata.get("metadata", "{}"))
        latest_id_classified = latest_job_metadata.get("latest_id_classified", None)
        latest_inference_timestamp = latest_job_metadata.get(
            "inference_timestamp", None
        )
    else:
        latest_id_classified = None
        latest_inference_timestamp = None

    if timestamp is not None:
        logger.info(
            f"Using backfill timestamp {timestamp} instead of latest inference timestamp: {latest_inference_timestamp}"
        )
        latest_inference_timestamp = timestamp

    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None,
        min_id=latest_id_classified,
        min_timestamp=latest_inference_timestamp,
        status="pending",
    )
    logger.info(f"Loaded {len(latest_payloads)} posts to classify.")
    logger.info(f"Getting posts to classify for inference type {inference_type}.")
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")

    if not latest_payloads:
        logger.info("No posts to classify.")
        return []

    if columns is None:
        columns = [
            "uri",
            "text",
            "preprocessing_timestamp",
            "batch_id",
            "batch_metadata",
        ]

    # Create DataFrame with all columns from the payloads
    posts_df = pd.DataFrame(latest_payloads)

    # Drop duplicates before any other processing
    if "uri" in posts_df.columns:
        posts_df = posts_df.drop_duplicates(subset=["uri"])
    logger.info(f"After dropping duplicates, {len(posts_df)} posts remain.")

    # Filter posts if needed
    posts_df = filter_posts_df(posts_df)
    logger.info(f"After filtering, {len(posts_df)} posts remain.")

    # Verify required columns exist and add missing ones with None values
    missing_columns = [col for col in columns if col not in posts_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Select only requested columns
    dicts = posts_df[columns].to_dict(orient="records")
    dict_models = [PostToLabelModel(**d) for d in dicts]  # to verify fields
    dicts = [d.model_dump() for d in dict_models]
    return dicts
