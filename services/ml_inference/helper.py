"""Helper tooling for ML inference."""

from datetime import datetime, timedelta, timezone
import json
from typing import Literal, Optional

import pandas as pd

from lib.constants import timestamp_format
from lib.db.queue import Queue
from lib.log.logger import get_logger
from lib.helper import track_performance


logger = get_logger(__name__)

dynamodb_table_name = "integration_run_metadata"
MIN_POST_TEXT_LENGTH = 5


def determine_backfill_latest_timestamp(
    backfill_duration: int,
    backfill_period: Literal["days", "hours"],
) -> str:
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
    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = None
    return timestamp


def filter_posts_df(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out posts with invalid text.

    Args:
        df: DataFrame containing posts with 'text' column and 'created_at' column

    Returns:
        DataFrame with posts removed that have:
        - Missing text (if text column exists)
        - Empty text (if text column exists)
        - Text shorter than MIN_POST_TEXT_LENGTH characters (if text column exists)
        - Missing created_at timestamp (if created_at column exists)
    """
    filtered_df = df.copy()

    # Only apply text filters if text column exists
    if "text" in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df["text"].notna()
            & (filtered_df["text"] != "")
            & (filtered_df["text"].str.len() >= MIN_POST_TEXT_LENGTH)
        ]

    # Only apply timestamp filter if created_at column exists
    if "created_at" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["created_at"].notna()]

    return filtered_df


@track_performance
def get_posts_to_classify(
    inference_type: Literal["llm", "perspective_api", "ime"],
    timestamp: Optional[str] = None,
    previous_run_metadata: Optional[dict] = None,
    columns: Optional[list[str]] = None,
) -> list[dict]:
    """Get posts to classify from the queue.

    This is the single entry point for getting data for inference. All inference modules
    should use this function to get their data.

    Args:
        inference_type: Type of inference to run
        timestamp: Optional timestamp to override latest inference timestamp
        previous_run_metadata: Optional metadata from previous run
        columns: Optional list of columns to return

    Returns:
        List of posts to classify
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
        columns = ["uri", "text", "created_at", "batch_id", "batch_metadata"]

    # Create DataFrame with all columns from the payloads
    posts_df = pd.DataFrame(latest_payloads)

    # Drop duplicates before any other processing
    if "uri" in posts_df.columns:
        posts_df = posts_df.drop_duplicates(subset=["uri"])

    # Filter posts if needed
    posts_df = filter_posts_df(posts_df)

    # Verify required columns exist and add missing ones with None values
    missing_columns = [col for col in columns if col not in posts_df.columns]
    if missing_columns:
        logger.warning(f"Missing required columns: {missing_columns}")
        for col in missing_columns:
            posts_df[col] = None

    # Select only requested columns
    return posts_df[columns].to_dict(orient="records")
