"""Classify the valence of text using the Vader classifier."""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.valence_classifier.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import (
    classify_latest_posts as orchestrate_classification,
)

# Define configuration for this inference type
VALENCE_CLASSIFIER_CONFIG = InferenceConfig(
    inference_type="valence_classifier",
    queue_inference_type="valence_classifier",
    classification_func=run_batch_classification,
    log_message_template="Classifying {count} posts with Vader classifier...",
    empty_result_message="No posts to classify with Vader classifier. Exiting...",
)


@track_performance
def classify_latest_posts(
    backfill_period: Optional[Literal["days", "hours"]] = None,
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
    return orchestrate_classification(
        config=VALENCE_CLASSIFIER_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
