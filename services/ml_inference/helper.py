"""Helper tooling for ML inference."""

from datetime import datetime, timedelta, timezone
import json
from multiprocessing import Pool, cpu_count
from typing import Literal, Optional

import pandas as pd

from lib.aws.dynamodb import DynamoDB
from lib.constants import timestamp_format
from lib.db.queue import Queue, QueueItem
from lib.log.logger import get_logger
from lib.helper import track_performance


dynamodb = DynamoDB()
logger = get_logger(__name__)

dynamodb_table_name = "ml_inference_labeling_sessions"
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


def get_posts_to_classify(
    inference_type: Literal["llm", "perspective_api", "ime"],
    timestamp: Optional[str] = None,
    previous_run_metadata: Optional[dict] = None,
) -> list[dict]:
    """Get posts to classify.

    Steps:
    - Takes the inference type
    - Loads in the latest labeling session for the inference type (from DynamoDB)
        - I can create one table for all labeling sessions and just change the “inference_type” value.
    - Get the latest inference timestamp
    - Use that as a filter for labeling.
    - Get the rows of data to label.
    """
    queue_name = (
        "ml_inference_perspective_api"
        if inference_type == "perspective_api"
        else "ml_inference_sociopolitical"
        if inference_type == "llm"
        else "ml_inference_ime"
        if inference_type == "ime"
        else None
    )
    if queue_name is None:
        raise ValueError(f"Invalid inference type: {inference_type}")

    queue = Queue(queue_name=f"input_{queue_name}")
    if not previous_run_metadata:
        previous_run_metadata = {}
    if previous_run_metadata:
        latest_job_metadata: dict = json.loads(
            previous_run_metadata.get("metadata", "")
        )
        if latest_job_metadata:
            latest_id_classified = latest_job_metadata.get("latest_id_classified", None)  # noqa
        latest_inference_timestamp = latest_job_metadata.get(
            "inference_timestamp", None
        )  # noqa
    else:
        latest_id_classified = None
        latest_inference_timestamp = None

    if timestamp is not None:
        logger.info(
            f"Using backfill timestamp {timestamp} instead of latest inference timestamp: {latest_inference_timestamp}"
        )  # noqa
        latest_inference_timestamp = timestamp

    latest_queue_items: list[QueueItem] = queue.load_items_from_queue(
        limit=None,
        min_id=latest_id_classified,
        min_timestamp=latest_inference_timestamp,
        status="pending",
    )

    latest_payload_strings: list[str] = [item.payload for item in latest_queue_items]
    latest_payloads: list[dict] = []
    for payload_string in latest_payload_strings:
        payloads: list[dict] = json.loads(payload_string)
        latest_payloads.extend(payloads)
    logger.info(f"Loaded {len(latest_payloads)} posts to classify.")
    logger.info(f"Getting posts to classify for inference type {inference_type}.")  # noqa
    logger.info(f"Latest inference timestamp: {latest_inference_timestamp}")
    posts_df = pd.DataFrame(latest_payloads, columns=["uri", "text"])
    if len(posts_df) == 0:
        logger.info("No posts to classify.")
        return []
    posts_df = posts_df.drop_duplicates(subset=["uri"])
    return posts_df[["uri", "text"]].to_dict(orient="records")


def json_file_reader(file_paths):
    for path in file_paths:
        with open(path, "r") as file:
            yield json.loads(file.read())


def process_file(file_path) -> list[dict]:
    """Loads the .jsonl files at a given path."""
    results = []
    with open(file_path, "r") as file:
        for line in file:
            results.append(json.loads(line))
    return results


@track_performance
def load_cached_jsons_as_df(
    filepaths: list[str], dtypes_map: dict
) -> Optional[pd.DataFrame]:
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
