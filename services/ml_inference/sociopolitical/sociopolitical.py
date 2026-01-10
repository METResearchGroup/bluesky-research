"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""

from typing import Literal, Optional

from lib.helper import track_performance
from services.ml_inference.sociopolitical.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import orchestrate_classification
from services.ml_inference.models import ClassificationSessionModel

SOCIOPOLITICAL_CONFIG = InferenceConfig(
    inference_type="sociopolitical",
    queue_inference_type="sociopolitical",
    classification_func=run_batch_classification,
    log_message_template="Classifying {count} posts with an LLM...",
    empty_result_message="No posts to classify with sociopolitical LLM classifier. Exiting...",
)


@track_performance
def classify_latest_posts(
    backfill_period: Optional[Literal["days", "hours"]] = None,
    backfill_duration: Optional[int] = None,
    run_classification: bool = True,
    previous_run_metadata: Optional[dict] = None,
    event: Optional[dict] = None,
) -> ClassificationSessionModel:
    """Classifies the latest posts using LLM inference.

    This is a convenience wrapper around orchestrate_classification with
    sociopolitical-specific configuration. See orchestrate_classification
    for full parameter and return documentation.
    """
    return orchestrate_classification(
        config=SOCIOPOLITICAL_CONFIG,
        backfill_period=backfill_period,
        backfill_duration=backfill_duration,
        run_classification=run_classification,
        previous_run_metadata=previous_run_metadata,
        event=event,
    )


if __name__ == "__main__":
    classify_latest_posts()
    print("LLM classification complete.")
