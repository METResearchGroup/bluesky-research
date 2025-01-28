"""Exports the results of classifying posts to an external store."""

from typing import Optional

from lib.db.queue import Queue
from lib.helper import generate_current_datetime_str
from services.ml_inference.models import PerspectiveApiLabelsModel

input_queue = Queue(
    queue_name="input_ml_inference_perspective_api",
    create_new_queue=True,
)
output_queue = Queue(
    queue_name="output_ml_inference_perspective_api",
    create_new_queue=True,
)


def return_failed_labels_to_input_queue(
    failed_label_models: list[PerspectiveApiLabelsModel],
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

    input_queue.batch_add_items_to_queue(
        items=[{"uri": post.uri, "text": post.text} for post in failed_label_models],
        batch_size=batch_size,
        metadata={
            "reason": "failed_label_perspective_api",
            "model_reason": failed_label_models[0].reason,
            "label_timestamp": generate_current_datetime_str(),
        },
    )


def write_posts_to_cache(
    posts: list[PerspectiveApiLabelsModel],
    batch_size: Optional[int] = None,
):
    """Write successfully classified posts to the queue storage.

    If there are no posts, do nothing.
    """
    if not posts:
        return

    output_queue.batch_add_items_to_queue(
        items=[post.dict() for post in posts],
        batch_size=batch_size,
    )
