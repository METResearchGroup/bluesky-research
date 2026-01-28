"""Base file for classifying posts in batch using the Perspective API."""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.perspective_api.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import orchestrate_classification
from services.ml_inference.models import ClassificationSessionModel

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
    max_records_per_run: Optional[int] = None,
    run_classification: bool = True,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
) -> ClassificationSessionModel:
    """Classifies posts using the Perspective API.

    This is a convenience wrapper around orchestrate_classification with
    Perspective API-specific configuration. See orchestrate_classification
    for full parameter and return documentation.
    """
    return orchestrate_classification(
        config=PERSPECTIVE_API_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        max_records_per_run=max_records_per_run,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
