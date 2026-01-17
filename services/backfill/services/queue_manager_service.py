from typing import Literal

from lib.db.queue import (
    Queue,
    get_input_queue_for_integration,
    get_output_queue_for_integration,
)
from lib.log.logger import get_logger
from services.backfill.models import PostToEnqueueModel
from services.backfill.exceptions import QueueManagerServiceError

logger = get_logger(__file__)


class QueueManagerService:
    """Class to manage all queue-related I/O operations for the backfill
    service."""

    # NOTE: for the purposes of backfill, we only insert into the input
    # queues, hence why it's hardcoded. We do insert to the output queues,
    # but that's managed by each individual integration service.
    # TODO: consider refactoring this AND the individual integration service
    # implementations to use a single `insert_records_to_queue` function.
    def insert_posts_to_queue(
        self, integration_name: str, posts: list[PostToEnqueueModel]
    ) -> None:
        try:
            input_queue = get_input_queue_for_integration(
                integration_name=integration_name
            )
            posts_dicts: list[dict] = [post.model_dump() for post in posts]
            input_queue.batch_add_items_to_queue(items=posts_dicts, metadata=None)
            logger.info(
                f"Inserted {len(posts)} posts into queue for integration: {integration_name}"
            )
        except Exception as e:
            logger.error(
                f"Error inserting posts to queue for integration: {integration_name}: {e}"
            )
            raise QueueManagerServiceError(
                f"Error inserting posts to queue for integration: {integration_name}: {e}"
            ) from e

    # TODO: for the purposes of backfill, we only need to load posts from
    # the output queues, hence why it's hardcoded. We do load from the input
    # queues, but that's managed by each individual integration service.
    # TODO: consider refactoring this AND the individual integration service
    # implementations to use a single `load_records_from_queue` function.
    def load_records_from_queue(self, integration_name: str) -> list[dict]:
        """Loads records from the queue for the given integration name."""
        try:
            output_queue = get_output_queue_for_integration(
                integration_name=integration_name
            )
            records: list[dict] = output_queue.load_dict_items_from_queue()
            return records
        except Exception as e:
            logger.error(
                f"Error loading records from queue for integration: {integration_name}: {e}"
            )
            raise QueueManagerServiceError(
                f"Error loading records from queue for integration: {integration_name}: {e}"
            ) from e

    def load_queue_item_ids(
        self, integration_name: str, queue_type: Literal["input", "output"]
    ) -> list[int]:
        """Loads queue item IDs from the queue for the given integration name.

        Args:
            integration_name: The name of the integration to load IDs from.
            queue_type: The type of queue to load from ("input" or "output").

        Returns:
            list[int]: List of queue item IDs matching the filters.
        """
        try:
            if queue_type == "input":
                queue: Queue = get_input_queue_for_integration(
                    integration_name=integration_name
                )
            elif queue_type == "output":
                queue = get_output_queue_for_integration(
                    integration_name=integration_name
                )
            else:
                raise QueueManagerServiceError(f"Invalid queue type: {queue_type}")
            ids: list[int] = queue.load_item_ids_from_queue()
            logger.info(
                f"Loaded {len(ids)} queue item IDs from {queue_type} queue for integration: {integration_name}"
            )
            return ids
        except Exception as e:
            logger.error(
                f"Error loading queue item IDs from queue for integration: {integration_name}: {e}"
            )
            raise QueueManagerServiceError(
                f"Error loading queue item IDs from queue for integration: {integration_name}: {e}"
            ) from e

    # NOTE: same caveat as `load_records_from_queue` regarding the hardcoding.
    # TODO: consolidate the _clear_output_queue and _clear_single_queue
    # from the CLI pipeline to use this service instead, and to use delete_records_from_queue
    # instead of _clear_single_queue.
    def delete_records_from_queue(
        self,
        integration_name: str,
        queue_type: Literal["input", "output"],
        queue_ids_to_delete: list[int] | None = None,
    ):
        """Deletes records from the queue for the given integration name.

        Args:
            integration_name: The name of the integration to delete records from.
            queue_ids_to_delete: The IDs of the queue items to delete. If not provided, all records will be deleted.
        """
        try:
            if queue_type == "input":
                queue: Queue = get_input_queue_for_integration(
                    integration_name=integration_name
                )
            elif queue_type == "output":
                queue = get_output_queue_for_integration(
                    integration_name=integration_name
                )
            else:
                raise QueueManagerServiceError(f"Invalid queue type: {queue_type}")
            if queue_ids_to_delete is None:
                deleted_count = queue.clear_queue()
                logger.info(
                    f"Deleted {deleted_count} records from {queue_type} queue for integration: {integration_name}"
                )
            else:
                deleted_count = queue.batch_delete_items_by_ids(ids=queue_ids_to_delete)
                logger.info(
                    f"Deleted {deleted_count} records from {queue_type} queue for integration: {integration_name}"
                )
        except Exception as e:
            logger.error(
                f"Error deleting records from queue for integration: {integration_name}: {e}"
            )
            raise QueueManagerServiceError(
                f"Error deleting records from queue for integration: {integration_name}: {e}"
            ) from e
