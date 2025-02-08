"""Base file for classifying posts in batch using the Perspective API."""

from typing import Optional

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from ml_tooling.perspective_api.model import run_batch_classification
from services.ml_inference.helper import (
    determine_backfill_latest_timestamp,
    get_posts_to_classify,
)

logger = get_logger(__file__)


@track_performance
def classify_latest_posts(
    backfill_period: Optional[str] = None,
    backfill_duration: Optional[int] = None,
    run_classification: bool = True,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
) -> dict:
    """Classifies posts using the Perspective API and manages classification results.

    This function orchestrates the end-to-end process of retrieving posts from a queue,
    classifying them using the Perspective API, and managing the results. It supports
    both backfilling historical data and processing new posts.

    Args:
        backfill_period (Optional[str]): The time unit for historical data processing.
            Must be either "days" or "hours". Required if backfill_duration is provided.
        backfill_duration (Optional[int]): Number of time units to process historically.
            Required if backfill_period is provided.
        run_classification (bool): Controls whether to execute new classifications (True)
            or only export cached results (False). Defaults to True.
        previous_run_metadata (Optional[dict]): Metadata from prior classification runs
            containing 'metadata' key with JSON string of:
            - latest_id_classified: ID of last processed post
            - inference_timestamp: Timestamp of last inference
            Used to prevent duplicate processing.
        event (Optional[dict]): Original event/payload from the handler function,
            included in return metadata for traceability.

    Returns:
        dict: Classification session summary containing:
            - inference_type (str): Fixed value "perspective_api"
            - inference_timestamp (str): Execution timestamp in format YYYY-MM-DD-HH:MM:SS
            - total_classified_posts (int): Count of processed posts (0 if run_classification=False)
            - event (dict): Original input event data
            - inference_metadata (dict): Classification results from run_batch_classification()
                if run_classification=True, containing:
                - total_batches (int): Number of batches processed
                - total_posts_successfully_labeled (int): Count of successfully labeled posts
                - total_posts_failed_to_label (int): Count of failed post classifications

    Control Flow:
        1. If run_classification=True:
            a. Calculates latest timestamp for backfilling using backfill parameters
            b. Retrieves candidate posts via get_posts_to_classify()
            c. If no posts found, returns early with zero counts
            d. Processes posts through Perspective API via run_batch_classification()
        2. If run_classification=False:
            a. Skips classification and prepares empty metadata
        3. Generates current timestamp
        4. Returns session summary with classification results or empty metadata
    """
    if run_classification:
        backfill_latest_timestamp: str = determine_backfill_latest_timestamp(
            backfill_duration=backfill_duration,
            backfill_period=backfill_period,
        )
        posts_to_classify: list[dict] = get_posts_to_classify(
            inference_type="perspective_api",
            timestamp=backfill_latest_timestamp,
            previous_run_metadata=previous_run_metadata,
        )
        logger.info(
            f"Classifying {len(posts_to_classify)} posts with the Perspective API..."
        )
        if len(posts_to_classify) == 0:
            logger.warning("No posts to classify with Perspective API. Exiting...")
            return {
                "inference_type": "perspective_api",
                "inference_timestamp": generate_current_datetime_str(),
                "total_classified_posts": 0,
                "event": event,
                "inference_metadata": {},
            }
        classification_metadata = run_batch_classification(posts=posts_to_classify)
        total_classified = len(posts_to_classify)
    else:
        logger.info("Skipping classification and exporting cached results...")
        classification_metadata = {}
        total_classified = 0

    timestamp = generate_current_datetime_str()
    labeling_session = {
        "inference_type": "perspective_api",
        "inference_timestamp": timestamp,
        "total_classified_posts": total_classified,
        "event": event,
        "inference_metadata": classification_metadata,
    }
    return labeling_session


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
