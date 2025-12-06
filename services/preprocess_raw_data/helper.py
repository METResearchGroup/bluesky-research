"""Helper code for running filters on raw data."""

import json
from typing import Optional

from lib.helper import determine_backfill_latest_timestamp
from lib.constants import current_datetime_str
from lib.db.queue import Queue
from lib.helper import track_performance, generate_current_datetime_str
from lib.log.logger import get_logger
from services.preprocess_raw_data.preprocess import preprocess_latest_posts

logger = get_logger(__name__)


def get_posts_to_preprocess(
    timestamp: Optional[str] = None,
    previous_run_metadata: Optional[dict] = None,
) -> list[dict]:
    """Retrieves posts to preprocess.

    Args:
        timestamp (Optional[str]): Optional timestamp in YYYY-MM-DD-HH:MM:SS format to
            override latest preprocessing timestamp for filtering posts.
        previous_run_metadata (Optional[dict]): Metadata from previous run containing:
            - metadata (str): JSON string with:
                - latest_id_classified (Optional[int]): ID of last processed post.
                - inference_timestamp (Optional[str]): Timestamp of last preprocessing run.
    """
    queue = Queue(queue_name="input_preprocess_raw_data")

    if previous_run_metadata is not None:
        latest_job_metadata = json.loads(previous_run_metadata.get("metadata", "{}"))
        latest_id_classified = latest_job_metadata.get("latest_id_classified", None)
        latest_preprocessing_timestamp = latest_job_metadata.get(
            "preprocessing_timestamp", None
        )
    else:
        latest_id_classified = None
        latest_preprocessing_timestamp = None

    if timestamp is not None:
        logger.info(
            f"Using backfill timestamp {timestamp} instead of latest preprocessing timestamp: {latest_preprocessing_timestamp}"
        )
        latest_preprocessing_timestamp = timestamp

    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None,
        min_id=latest_id_classified,
        min_timestamp=latest_preprocessing_timestamp,
        status="pending",
    )
    logger.info(f"Loaded {len(latest_payloads)} posts to preprocess.")
    logger.info(f"Latest preprocessing timestamp: {latest_preprocessing_timestamp}")

    # TODO: check if I need to transform the posts to fit the format expected
    # by the preprocess_latest_posts function.

    if not latest_payloads:
        logger.info("No posts to preprocess.")
        return []

    return latest_payloads


@track_performance
def preprocess_latest_raw_data(
    backfill_period: Optional[str] = None,
    backfill_duration: Optional[int] = None,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
):
    """Preprocesses the latest raw data."""
    logger.info(f"Preprocessing the latest raw data at {current_datetime_str}.")
    backfill_latest_timestamp: str = determine_backfill_latest_timestamp(
        backfill_duration=backfill_duration,
        backfill_period=backfill_period,
    )
    posts_to_preprocess: list[dict] = get_posts_to_preprocess(
        timestamp=backfill_latest_timestamp,
        previous_run_metadata=previous_run_metadata,
    )
    custom_args = {}
    if event.get("overwrite_preprocessing_timestamp", False):
        custom_args["new_timestamp_field"] = event.get(
            "new_timestamp_field", "synctimestamp"
        )
    logger.info(f"Preprocessing {len(posts_to_preprocess)} posts...")  # noqa
    if len(posts_to_preprocess) == 0:
        logger.warning("No posts to preprocess. Exiting...")
        return {
            "service": "preprocess_raw_data",
            "preprocessing_timestamp": current_datetime_str,
            "status_code": 200,
            "total_preprocessed_posts": 0,
            "body": json.dumps("No posts to preprocess."),
            "event": event,
            "metadata": {},
        }
    preprocessing_metadata: dict = preprocess_latest_posts(
        posts=posts_to_preprocess, custom_args=custom_args
    )
    total_preprocessed: int = len(posts_to_preprocess)

    timestamp = generate_current_datetime_str()
    preprocessing_session = {
        "service": "preprocess_raw_data",
        "preprocessing_timestamp": timestamp,
        "total_preprocessed_posts": total_preprocessed,
        "event": event,
        "metadata": preprocessing_metadata,
    }
    return preprocessing_session
