from lib.db.queue import get_input_queue_for_integration
from lib.log.logger import get_logger
from services.backfill.models import PostToEnqueueModel

logger = get_logger(__file__)


class QueueManagerService:
    """Class to manage all queue-related I/O operations for the backfill
    service."""

    def insert_posts_to_queue(
        self, integration_name: str, posts: list[PostToEnqueueModel]
    ) -> None:
        input_queue = get_input_queue_for_integration(integration_name=integration_name)
        posts_dicts: list[dict] = [post.model_dump() for post in posts]
        input_queue.batch_add_items_to_queue(items=posts_dicts, metadata=None)
        logger.info(
            f"Inserted {len(posts)} posts into queue for integration: {integration_name}"
        )
