"""Classify the valence of text using the Vader classifier."""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.valence_classifier.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import orchestrate_classification
from services.ml_inference.models import ClassificationSessionModel

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
) -> ClassificationSessionModel:
    """Classifies the latest posts using the Vader classifier.

    This is a convenience wrapper around orchestrate_classification with
    valence classifier-specific configuration. See orchestrate_classification
    for full parameter and return documentation.
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
