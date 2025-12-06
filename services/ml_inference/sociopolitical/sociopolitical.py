"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""

from typing import Literal, Optional

from lib.helper import track_performance
from ml_tooling.llm.model import run_batch_classification
from services.ml_inference.config import InferenceConfig
from services.ml_inference.helper import (
    classify_latest_posts as orchestrate_classification,
)

# Define configuration for this inference type
# Note: queue_inference_type is "llm" but inference_type is "sociopolitical"
SOCIOPOLITICAL_CONFIG = InferenceConfig(
    inference_type="sociopolitical",
    queue_inference_type="llm",  # get_posts_to_classify uses "llm"
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
