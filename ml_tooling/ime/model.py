"""Model class for deploying IME classifier.

This is named "model.py" but the model logic is actually in "classifier.py".
This is to keep up with the naming conventions used for the other integrations,
in which the batch classification logic is encapsulated in the "model.py" file
but not neccessarily the model inference itself.
"""

from typing import Optional

from lib.helper import create_batches, track_performance
from lib.log.logger import get_logger
from ml_tooling.ime.classifier import process_ime_batch
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)

default_batch_size = 100  # TODO: check this in the interface itself.

logger = get_logger(__file__)


def create_labels(posts: list[dict], responses: list[dict]) -> list[dict]:
    """Create label models from posts and responses."""
    pass


@track_performance
def batch_classify_posts(
    posts: list[dict],
    batch_size: Optional[int] = default_batch_size,
) -> dict:
    """Run batch classification on the given posts."""
    batches: list[list[dict]] = create_batches(batch_list=posts, batch_size=batch_size)
    total_batches = len(batches)
    total_posts_successfully_labeled = 0
    total_posts_failed_to_label = 0
    for i, batch in enumerate(batches):
        if i % 10 == 0:
            logger.info(f"Processing batch {i}/{total_batches}")
        responses: list[dict] = process_ime_batch(post_batch=batch)
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
    return {
        "total_posts_successfully_labeled": total_posts_successfully_labeled,
        "total_posts_failed_to_label": total_posts_failed_to_label,
    }


def run_batch_classification(
    posts: list[dict],
    batch_size: Optional[int] = default_batch_size,
) -> dict:
    """Run batch classification on the given posts."""
    metadata = batch_classify_posts(posts=posts, batch_size=batch_size)
    return metadata
