from typing import Optional

from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__name__)

queue = Queue(queue_name="backfill_sync", create_new_queue=True)


def write_records_to_cache(records: list[dict], batch_size: Optional[int] = None):
    if not records:
        return

    logger.info(f"Adding {len(records)} records to the backfill sync queue.")
    queue.batch_add_items_to_queue(items=records, batch_size=batch_size)
