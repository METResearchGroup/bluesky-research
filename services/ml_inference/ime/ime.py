"""Classify posts using the IME classification model."""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.ime.constants import default_hyperparameters
from ml_tooling.ime.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import orchestrate_classification
from services.ml_inference.models import ClassificationSessionModel


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
    max_records_per_run: Optional[int] = None,
    run_classification: bool = True,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
) -> ClassificationSessionModel:
    """Classifies the latest posts using the IME classification model.

    This is a convenience wrapper around orchestrate_classification with
    IME-specific configuration. See orchestrate_classification for full
    parameter and return documentation.
    """
    return orchestrate_classification(
        config=IME_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        max_records_per_run=max_records_per_run,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
