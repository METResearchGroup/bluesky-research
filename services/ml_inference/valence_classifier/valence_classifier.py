"""Classify the valence of text using the Vader classifier."""

from typing import Optional

from api.integrations_router.helper import determine_backfill_latest_timestamp
from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from ml_tooling.valence_classifier.model import run_batch_classification
from services.ml_inference.helper import (
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
    """
    Classifies the latest posts using the Vader classifier.

    Args:
        backfill_period (Optional[str]): The unit for backfilling - either "days" or "hours".
            Must be provided together with backfill_duration.
        backfill_duration (Optional[int]): The number of time units to backfill.
            Must be provided together with backfill_period.
        run_classification (bool): Whether to run the classification model on posts.
            If False, will only export cached results. Defaults to True.
        previous_run_metadata (Optional[dict]): Metadata from previous classification runs
            to avoid reprocessing posts.
        event (Optional[dict]): The original event/payload passed to the handler function.
            This is included in the returned session metadata for tracking purposes.

    Returns:
        dict: A labeling session summary containing:
            - inference_type (str): Always "valence_classifier"
            - inference_timestamp (str): Timestamp of when inference was run
            - total_classified_posts (int): Total number of posts classified
            - inference_metadata (dict): Metadata about the classification run
            - event (dict): The original event/payload passed to the function
    """
    if run_classification:
        backfill_latest_timestamp: str = determine_backfill_latest_timestamp(
            backfill_duration=backfill_duration,
            backfill_period=backfill_period,
        )
        posts_to_classify: list[dict] = get_posts_to_classify(
            inference_type="valence_classifier",
            timestamp=backfill_latest_timestamp,
            previous_run_metadata=previous_run_metadata,
        )
        logger.info(
            f"Classifying {len(posts_to_classify)} posts with Vader classifier..."
        )  # noqa
        if len(posts_to_classify) == 0:
            logger.warning("No posts to classify with Vader classifier. Exiting...")
            return {
                "inference_type": "valence_classifier",
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
        "inference_type": "valence_classifier",
        "inference_timestamp": timestamp,
        "total_classified_posts": total_classified,
        "event": event,
        "inference_metadata": classification_metadata,
    }
    return labeling_session


if __name__ == "__main__":
    classify_latest_posts()
