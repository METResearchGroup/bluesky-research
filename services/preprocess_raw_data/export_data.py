"""Export preprocessing data."""

from lib.db.queue import Queue
from lib.log.logger import get_logger

input_queue_name = "input_preprocess_raw_data"
output_queue_name = "output_preprocess_raw_data"

input_queue = Queue(queue_name=input_queue_name)
output_queue = Queue(queue_name=output_queue_name)

logger = get_logger(__name__)


def write_posts_to_cache(posts: list[dict]) -> None:
    """Writes posts to cache."""
    if not posts:
        logger.info("No preprocessed posts to write to cache. Exiting...")
        return
    successfully_preprocessed_batch_ids = set(post["batch_id"] for post in posts)
    logger.info(f"Adding {len(posts)} posts to the output queue.")
    output_queue.batch_add_items_to_queue(items=posts)
    logger.info(
        f"Deleting {len(successfully_preprocessed_batch_ids)} batch IDs from the input queue."
    )
    input_queue.batch_delete_items_by_ids(ids=list(successfully_preprocessed_batch_ids))
