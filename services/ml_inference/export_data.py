"""Exports the results of classifying posts."""

from typing import Literal, Optional

from lib.db.queue import Queue
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__file__)

# Queue name mapping (no initialization at import time)
_INPUT_QUEUE_NAMES = {
    "perspective_api": "input_ml_inference_perspective_api",
    "sociopolitical": "input_ml_inference_sociopolitical",
    "ime": "input_ml_inference_ime",
    "valence_classifier": "input_ml_inference_valence_classifier",
}

_OUTPUT_QUEUE_NAMES = {
    "perspective_api": "output_ml_inference_perspective_api",
    "sociopolitical": "output_ml_inference_sociopolitical",
    "ime": "output_ml_inference_ime",
    "valence_classifier": "output_ml_inference_valence_classifier",
}


# Default implementation factories (lazy loading to avoid import-time initialization)
def _default_input_queue(inference_type: str) -> Queue:
    """Factory for default input queue (lazy-loaded, no import-time creation).

    Args:
        inference_type: The type of inference (perspective_api, sociopolitical, etc.)

    Returns:
        Queue: A Queue instance for the specified inference type
    """
    queue_name = _INPUT_QUEUE_NAMES[inference_type]
    return Queue(queue_name=queue_name, create_new_queue=True)


def _default_output_queue(inference_type: str) -> Queue:
    """Factory for default output queue (lazy-loaded, no import-time creation).

    Args:
        inference_type: The type of inference (perspective_api, sociopolitical, etc.)

    Returns:
        Queue: A Queue instance for the specified inference type
    """
    queue_name = _OUTPUT_QUEUE_NAMES[inference_type]
    return Queue(queue_name=queue_name, create_new_queue=True)


def return_failed_labels_to_input_queue(
    inference_type: Literal[
        "perspective_api",
        "sociopolitical",
        "ime",
        "valence_classifier",
    ],
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

    queue = input_queue or _default_input_queue(inference_type)
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
    inference_type: Literal[
        "perspective_api",
        "sociopolitical",
        "ime",
        "valence_classifier",
    ],
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

    in_queue = input_queue or _default_input_queue(inference_type)
    out_queue = output_queue or _default_output_queue(inference_type)

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
