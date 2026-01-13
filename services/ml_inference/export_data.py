"""Exports the results of classifying posts."""

from typing import Optional, Sequence, TypeVar

from lib.db.queue import Queue
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from services.ml_inference.config import QueueInferenceType

logger = get_logger(__name__)

# Queue name mapping (no initialization at import time)
_INPUT_QUEUE_NAMES = {
    "perspective_api": "input_ml_inference_perspective_api",
    "sociopolitical": "input_ml_inference_sociopolitical",
    "ime": "input_ml_inference_ime",
    "valence_classifier": "input_ml_inference_valence_classifier",
    "intergroup": "input_ml_inference_intergroup",
}

_OUTPUT_QUEUE_NAMES = {
    "perspective_api": "output_ml_inference_perspective_api",
    "sociopolitical": "output_ml_inference_sociopolitical",
    "ime": "output_ml_inference_ime",
    "valence_classifier": "output_ml_inference_valence_classifier",
    "intergroup": "output_ml_inference_intergroup",
}

T = TypeVar("T")


# Default implementation factories (lazy loading to avoid import-time initialization)
def _default_input_queue(
    inference_type: QueueInferenceType,
) -> Queue:
    """Factory for default input queue (lazy-loaded, no import-time creation).

    Args:
        inference_type: The type of inference (perspective_api, sociopolitical, etc.)

    Returns:
        Queue: A Queue instance for the specified inference type
    """
    queue_name = _INPUT_QUEUE_NAMES[inference_type]
    return Queue(queue_name=queue_name, create_new_queue=True)


def _default_output_queue(
    inference_type: QueueInferenceType,
) -> Queue:
    """Factory for default output queue (lazy-loaded, no import-time creation).

    Args:
        inference_type: The type of inference (perspective_api, sociopolitical, etc.)

    Returns:
        Queue: A Queue instance for the specified inference type
    """
    queue_name = _OUTPUT_QUEUE_NAMES[inference_type]
    return Queue(queue_name=queue_name, create_new_queue=True)


def return_failed_labels_to_input_queue(
    inference_type: QueueInferenceType,
    failed_label_models: list[dict],
    batch_size: Optional[int] = None,
    input_queue: Optional[Queue] = None,
):
    """Returns failed labels to the input queue.

    If there are no failed labels, do nothing.

    If there are failed labels, add them to the input queue. For the 'reason'
    metadata, pick the first failed label's reason, since they all likely
    failed for the same reason.

    Args:
        inference_type: The type of inference being processed
        failed_label_models: List of failed label models to return to queue
        batch_size: Optional batch size for queue operations
        input_queue: Optional Queue instance for dependency injection (defaults to real queue)
    """
    if not failed_label_models:
        return

    queue = (
        input_queue if input_queue is not None else _default_input_queue(inference_type)
    )
    queue.batch_add_items_to_queue(
        items=[
            {"uri": post["uri"], "text": post["text"]} for post in failed_label_models
        ],
        batch_size=batch_size,
        metadata={
            "reason": f"failed_label_{inference_type}",
            "model_reason": failed_label_models[0]["reason"],
            "label_timestamp": generate_current_datetime_str(),
        },
    )


def write_posts_to_cache(
    inference_type: QueueInferenceType,
    posts: list[dict],
    batch_size: Optional[int] = None,
    input_queue: Optional[Queue] = None,
    output_queue: Optional[Queue] = None,
):
    """Write successfully classified posts to the queue storage.

    If there are no posts, do nothing.

    If there are posts, add them to the output queue and also remove them
    from the input queue. Removes the posts by deleting the relevant
    batch IDs from the input queue (all posts from the given batch will either
    be successfully labeled or failed to label, and we can delete the batch ID
    from the input queue since the failed posts will be re-inserted into the
    input queue).

    Args:
        inference_type: The type of inference being processed
        posts: List of successfully classified posts to write to cache
        batch_size: Optional batch size for queue operations
        input_queue: Optional input Queue instance for dependency injection (defaults to real queue)
        output_queue: Optional output Queue instance for dependency injection (defaults to real queue)
    """
    if not posts:
        logger.info("No posts to write to cache.")
        return

    successfully_labeled_batch_ids = set(post["batch_id"] for post in posts)

    in_queue = (
        input_queue if input_queue is not None else _default_input_queue(inference_type)
    )
    out_queue = (
        output_queue
        if output_queue is not None
        else _default_output_queue(inference_type)
    )

    logger.info(f"Adding {len(posts)} posts to the output queue.")
    out_queue.batch_add_items_to_queue(
        items=posts,
        batch_size=batch_size,
    )
    logger.info(
        f"Deleting {len(successfully_labeled_batch_ids)} batch IDs from the input queue."
    )
    in_queue.batch_delete_items_by_ids(
        ids=list(successfully_labeled_batch_ids),
    )


# TODO: add unit tests.
def split_labels_into_successful_and_failed_labels(
    labels: Sequence[T],
    successful_label_attribute_name: str = "was_successfully_labeled",
) -> tuple[list[T], list[T]]:
    """Splits labels into successful and failed labels.

    Args:
        labels: List of labels to split into successful and failed labels
        successful_label_attribute_name: Attribute name of the successful label

    Returns:
        Tuple of lists: (successful_labels, failed_labels)
    """
    successful_labels: list[T] = []
    failed_labels: list[T] = []
    for label in labels:
        # add defensiveness in case labels aren't Pydantic models.
        if isinstance(label, dict):
            raise ValueError("Labels must be a sequence of Pydantic models or dicts.")
        if getattr(label, successful_label_attribute_name, False):
            successful_labels.append(label)
        else:
            failed_labels.append(label)
    return successful_labels, failed_labels
