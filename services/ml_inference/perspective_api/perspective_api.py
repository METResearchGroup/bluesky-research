"""Base file for classifying posts in batch using the Perspective API."""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.perspective_api.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import (
    classify_latest_posts as orchestrate_classification,
)

PERSPECTIVE_API_CONFIG = InferenceConfig(
    inference_type="perspective_api",
    queue_inference_type="perspective_api",
    classification_func=run_batch_classification,
    log_message_template="Classifying {count} posts with the Perspective API...",
    empty_result_message="No posts to classify with Perspective API. Exiting...",
)


@track_performance
def classify_latest_posts(
    backfill_period: Optional[Literal["days", "hours"]] = None,
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
    return orchestrate_classification(
        config=PERSPECTIVE_API_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
