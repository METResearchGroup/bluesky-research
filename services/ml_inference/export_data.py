"""Exports the results of classifying posts."""

from typing import Literal, Optional

from lib.db.queue import Queue
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger

inference_type_to_input_queue_map = {
    "perspective_api": Queue(
        queue_name="input_ml_inference_perspective_api",
        create_new_queue=True,
    ),
    "sociopolitical": Queue(
        queue_name="input_ml_inference_sociopolitical",
        create_new_queue=True,
    ),
    "ime": Queue(
        queue_name="input_ml_inference_ime",
        create_new_queue=True,
    ),
    "valence_classifier": Queue(
        queue_name="input_ml_inference_valence_classifier",
        create_new_queue=True,
    ),
}

inference_type_to_output_queue_map = {
    "perspective_api": Queue(
        queue_name="output_ml_inference_perspective_api",
        create_new_queue=True,
    ),
    "sociopolitical": Queue(
        queue_name="output_ml_inference_sociopolitical",
        create_new_queue=True,
    ),
    "ime": Queue(
        queue_name="output_ml_inference_ime",
        create_new_queue=True,
    ),
    "valence_classifier": Queue(
        queue_name="output_ml_inference_valence_classifier",
        create_new_queue=True,
    ),
}

logger = get_logger(__file__)


def return_failed_labels_to_input_queue(
    inference_type: Literal[
        "perspective_api",
        "sociopolitical",
        "ime",
        "valence_classifier",
    ],
    failed_label_models: list[dict],
    batch_size: Optional[int] = None,
):
    """Returns failed labels to the input queue.

    If there are no failed labels, do nothing.

    If there are failed labels, add them to the input queue. For the 'reason'
    metadata, pick the first failed label's reason, since they all likely
    failed for the same reason.
    """
    if not failed_label_models:
        return

    input_queue = inference_type_to_input_queue_map[inference_type]
    input_queue.batch_add_items_to_queue(
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
):
    """Write successfully classified posts to the queue storage.

    If there are no posts, do nothing.

    If there are posts, add them to the output queue and also remove them
    from the input queue. Removes the posts by deleting the relevant
    batch IDs from the input queue (all posts from the given batch will either
    be successfully labeled or failed to label, and we can delete the batch ID
    from the input queue since the failed posts will be re-inserted into the
    input queue).
    """
    if not posts:
        return

    successfully_labeled_batch_ids = set(post["batch_id"] for post in posts)

    input_queue = inference_type_to_input_queue_map[inference_type]
    output_queue = inference_type_to_output_queue_map[inference_type]

    logger.info(f"Adding {len(posts)} posts to the output queue.")
    output_queue.batch_add_items_to_queue(
        items=posts,
        batch_size=batch_size,
    )
    logger.info(
        f"Deleting {len(successfully_labeled_batch_ids)} batch IDs from the input queue."
    )
    input_queue.batch_delete_items_by_ids(
        ids=list(successfully_labeled_batch_ids),
    )
