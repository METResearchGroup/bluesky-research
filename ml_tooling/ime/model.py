"""Model class for deploying IME classifier.

This is named "model.py" but the model logic is actually in "classifier.py".
This is to keep up with the naming conventions used for the other integrations,
in which the batch classification logic is encapsulated in the "model.py" file
but not neccessarily the model inference itself.
"""

from typing import Optional

from lib.helper import create_batches, track_performance
from lib.log.logger import get_logger
from lib.telemetry.cometml import log_batch_classification_to_cometml
from ml_tooling.ime.constants import default_hyperparameters
from ml_tooling.ime.inference import process_ime_batch
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)

logger = get_logger(__file__)


# TODO: create a pydantic class for the responses, just so that I can have
# a consistency for the fields. Then I can model_dump() those to dicts
# and consolidate them here.
def create_labels(posts: list[dict], responses: list[dict]) -> list[dict]:
    """Create label models from posts and responses."""
    pass


@track_performance
def batch_classify_posts(
    posts: list[dict],
    batch_size: int,
    minibatch_size: int,
) -> dict:
    """Run batch classification on the given posts."""
    batches: list[list[dict]] = create_batches(batch_list=posts, batch_size=batch_size)
    total_batches = len(batches)
    total_posts_successfully_labeled = 0
    total_posts_failed_to_label = 0
    for i, batch in enumerate(batches):
        if i % 10 == 0:
            logger.info(f"Processing batch {i}/{total_batches}")
        responses: list[dict] = process_ime_batch(
            post_batch=batch,
            minibatch_size=minibatch_size,
        )
        labels: list[dict] = create_labels(posts=batch, responses=responses)

        successful_labels: list[dict] = []
        failed_labels: list[dict] = []

        total_failed_labels = 0
        total_successful_labels = 0

        for post, label in zip(batch, labels):
            post_batch_id = post["batch_id"]
            label["batch_id"] = post_batch_id
            if label["was_successfully_labeled"]:
                successful_labels.append(label)
                total_successful_labels += 1
            else:
                failed_labels.append(label)
                total_failed_labels += 1

        if total_failed_labels > 0:
            logger.error(
                f"Failed to label {total_failed_labels} posts. Re-inserting these into queue."
            )
            return_failed_labels_to_input_queue(
                inference_type="ime",
                failed_labels=failed_labels,
                batch_size=batch_size,
            )
            total_posts_failed_to_label += total_failed_labels
        else:
            logger.info(f"Successfully labeled {total_successful_labels} posts.")
            write_posts_to_cache(
                inference_type="ime",
                posts=successful_labels,
                batch_size=batch_size,
            )
            total_posts_successfully_labeled += total_successful_labels
        del successful_labels
        del failed_labels

    metadata = {
        "total_posts_successfully_labeled": total_posts_successfully_labeled,
        "total_posts_failed_to_label": total_posts_failed_to_label,
    }
    classification_breakdown = {
        "emotion": {"title": "Emotion", "description": "", "probs": [], "labels": []},
        "integroup": {
            "title": "Intergroup",
            "description": "",
            "probs": [],
            "labels": [],
        },
        "moral": {"title": "Moral", "description": "", "probs": [], "labels": []},
        "other": {"title": "Other", "description": "", "probs": [], "labels": []},
    }
    return {
        "metadata": metadata,
        "classification_breakdown": classification_breakdown,
    }


@track_performance
@log_batch_classification_to_cometml(service="ml_inference_ime")
def run_batch_classification(
    posts: list[dict], hyperparameters: Optional[dict] = default_hyperparameters
) -> dict:
    """Run batch classification on the given posts and logs to W&B."""
    results: dict = batch_classify_posts(
        posts=posts,
        batch_size=hyperparameters["batch_size"],
        minibatch_size=hyperparameters["minibatch_size"],
    )
    # TODO: check the nature of the metadata, is this run metadata?
    metadata: dict = results["metadata"]
    classification_breakdown: dict = results["classification_breakdown"]
    experiment_metrics = {}  # TODO: add metrics.
    telemetry_metadata = {
        "hyperparameters": hyperparameters,
        "metrics": experiment_metrics,
        "classification_breakdown": classification_breakdown,
    }
    metadata = {"run_metadata": metadata, "telemetry_metadata": telemetry_metadata}
    return metadata
