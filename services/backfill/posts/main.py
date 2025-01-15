"""Service for backfilling posts."""

from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.posts.load_data import load_posts_to_backfill

logger = get_logger(__file__)


def backfill_posts(payload: dict):
    posts_to_backfill: dict[str, set[str]] = load_posts_to_backfill(
        payload.get("integration")
    )
    context = {"total": len(posts_to_backfill)}
    logger.info(f"Loaded {len(posts_to_backfill)} posts to backfill.", context=context)
    for integration, post_uris in posts_to_backfill.items():
        queue = Queue(queue_name=integration, create_new_queue=True)
        payloads = [{"uri": uri} for uri in post_uris]
        queue.batch_add_item_to_queue(payloads)


if __name__ == "__main__":
    payload = {"integration": "ml_inference_perspective_api"}
    backfill_posts(payload)
