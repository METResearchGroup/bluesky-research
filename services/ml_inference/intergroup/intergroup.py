"""Orchestration wrapper for intergroup classification."""

from typing import Literal, Optional

from lib.helper import track_performance
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import orchestrate_classification
from services.ml_inference.intergroup.batch_classifier import run_batch_classification
from services.ml_inference.models import ClassificationSessionModel


INTERGROUP_CONFIG = InferenceConfig(
    inference_type="intergroup",
    queue_inference_type="intergroup",
    classification_func=run_batch_classification,
    log_message_template="Classifying {count} posts with intergroup classifier...",
    empty_result_message="No posts to classify with intergroup classifier. Exiting...",
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
    """Classifies the latest posts using intergroup classification."""
    return orchestrate_classification(
        config=INTERGROUP_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        max_records_per_run=max_records_per_run,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
