"""Classify posts using the IME classification model."""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.ime.constants import default_hyperparameters
from ml_tooling.ime.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import (
    classify_latest_posts as orchestrate_classification,
)


class IMEConfig(InferenceConfig):
    """Configuration for IME inference with hyperparameter extraction."""

    def extract_classification_kwargs(self, event: Optional[dict]) -> dict:
        """Extract hyperparameters from event if provided."""
        hyperparameters = default_hyperparameters
        if event is not None and "hyperparameters" in event:
            hyperparameters = event["hyperparameters"]
        return {"hyperparameters": hyperparameters}


# Define configuration for this inference type
IME_CONFIG = IMEConfig(
    inference_type="ime",
    queue_inference_type="ime",
    classification_func=run_batch_classification,
    log_message_template="Classifying {count} posts with IME classifier...",
    empty_result_message="No posts to classify with IME classifier. Exiting...",
)


@track_performance
def classify_latest_posts(
    backfill_period: Optional[Literal["days", "hours"]] = None,
    backfill_duration: Optional[int] = None,
    run_classification: bool = True,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
) -> dict:
    """Classifies the latest posts using the IME classification model.

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
            - inference_type (str): Always "ime"
            - inference_timestamp (str): Timestamp of when inference was run
            - total_classified_posts (int): Total number of posts classified
            - inference_metadata (dict): Metadata about the classification run
            - event (dict): The original event/payload passed to the function

    The function:
    1. Determines the time range for post selection based on backfill parameters
    2. Retrieves posts to classify using get_posts_to_classify()
    3. Runs batch classification on posts if run_classification is True
    4. Exports and returns the results

    If no posts are found to classify, returns a summary with zero counts.
    """
    return orchestrate_classification(
        config=IME_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
