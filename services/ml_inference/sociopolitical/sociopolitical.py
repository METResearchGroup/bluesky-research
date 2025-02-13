"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""

from typing import Optional

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from ml_tooling.llm.model import run_batch_classification
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
    """
    Classifies the latest posts using LLM inference and generates a labeling session summary.

    Args:
        backfill_period (Optional[str]): Unit for backfilling (e.g., "days" or "hours"). Required if run_classification is True.
        backfill_duration (Optional[int]): Duration to backfill in units specified by backfill_period. Required if run_classification is True.
        run_classification (bool): Flag indicating whether to execute LLM classification. If False, returns cached results.
        previous_run_metadata (Optional[dict]): Metadata from prior classification runs to skip reprocessing posts.
        event (Optional[dict]): Original event or payload provided to the function, which is returned as part of the session metadata.

    Returns:
        dict: A dictionary containing:
            - inference_type (str): Always set to "sociopolitical".
            - inference_timestamp (str): The timestamp when the labeling session was generated.
            - total_classified_posts (int): Total number of posts processed for classification.
            - inference_metadata (dict): Metadata produced during batch classification, or an empty dict if classification is skipped.
            - event (Optional[dict]): The input event payload.

    Behavior:
        1. If run_classification is True, validates that both backfill_period and backfill_duration are provided.
        2. Determines the latest timestamp using determine_backfill_latest_timestamp.
        3. Retrieves posts via get_posts_to_classify using the determined timestamp and previous_run_metadata.
        4. Logs the number of posts retrieved.
        5. If no posts are found, logs a warning and returns a summary with zero classified posts.
        6. Otherwise, runs batch classification and records the number of classified posts.
        7. If run_classification is False, skips classification and returns cached results.

    [Suggestions]:
        - Consider wrapping critical calls in try/except blocks for improved error handling.

    [Clarifications]:
        - What should be the behavior if backfill parameters are missing when classification is enabled?
    """
    if run_classification:
        backfill_latest_timestamp: str = determine_backfill_latest_timestamp(
            backfill_duration=backfill_duration,
            backfill_period=backfill_period,
        )
        posts_to_classify: list[dict] = get_posts_to_classify(
            inference_type="llm",
            timestamp=backfill_latest_timestamp,
            previous_run_metadata=previous_run_metadata,
        )
        logger.info(f"Classifying {len(posts_to_classify)} posts with an LLM...")  # noqa
        if len(posts_to_classify) == 0:
            logger.warning(
                "No posts to classify with sociopolitical LLM classifier. Exiting..."
            )
            return {
                "inference_type": "sociopolitical",
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
        "inference_type": "sociopolitical",
        "inference_timestamp": timestamp,
        "total_classified_posts": total_classified,
        "event": event,
        "inference_metadata": classification_metadata,
    }
    return labeling_session


if __name__ == "__main__":
    classify_latest_posts()
    print("LLM classification complete.")
