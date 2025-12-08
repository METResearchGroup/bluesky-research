"""Export preprocessing data."""

from typing import Optional

from lib.db.queue import Queue
from lib.log.logger import get_logger

input_queue_name = "input_preprocess_raw_data"
output_queue_name = "output_preprocess_raw_data"

logger = get_logger(__name__)


def _default_input_queue() -> Queue:
    """Factory for default input queue."""
    return Queue(queue_name=input_queue_name, create_new_queue=True)


def _default_output_queue() -> Queue:
    """Factory for default output queue."""
    return Queue(queue_name=output_queue_name, create_new_queue=True)


def write_posts_to_cache(
    posts: list[dict],
    batch_ids: list[int],
    input_queue: Optional[Queue] = None,  # DI parameter
    output_queue: Optional[Queue] = None,  # DI parameter
) -> None:
    """Writes posts to cache.

    If there are no posts, do nothing.

    If there are posts, add them to the output queue and also remove them
    from the input queue. Removes the posts by deleting the relevant
    batch IDs from the input queue (all posts from the given batch will either
    be successfully labeled or failed to label, and we can delete the batch ID
    from the input queue since the failed posts will be re-inserted into the
    input queue).

    TODO: we still need to implement the logic to push failed records back to
    the input queue, other integrations do this but haven't found the need
    to do it here, plus failures don't have additional costs (e.g., if there
    are failures with LLM failures, it incurs API costs).

    Args:
        posts: List of preprocessed posts to write to cache.
        batch_ids: List of batch IDs for the posts.
        input_queue: Optional input queue instance for dependency injection.
                     If not provided, uses the default input queue.
        output_queue: Optional output queue instance for dependency injection.
                      If not provided, uses the default output queue.
    """
    if not posts:
        logger.info("No preprocessed posts to write to cache. Exiting...")
        return

    input_queue_instance = (
        input_queue if input_queue is not None else _default_input_queue()
    )
    output_queue_instance = (
        output_queue if output_queue is not None else _default_output_queue()
    )

    successfully_preprocessed_batch_ids = set(batch_ids)
    logger.info(f"Adding {len(posts)} posts to the output queue.")
    output_queue_instance.batch_add_items_to_queue(items=posts)
    logger.info(
        f"Deleting {len(successfully_preprocessed_batch_ids)} batch IDs from the input queue."
    )
    input_queue_instance.batch_delete_items_by_ids(
        ids=list(successfully_preprocessed_batch_ids)
    )
