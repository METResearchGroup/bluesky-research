from typing import Optional

from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.sync.constants import queue_name

logger = get_logger(__name__)

queue = Queue(queue_name=queue_name, create_new_queue=True)


def write_records_to_cache(records: list[dict], batch_size: Optional[int] = None):
    if not records:
        return

    logger.info(f"Adding {len(records)} records to the backfill sync queue.")
    queue.batch_add_items_to_queue(items=records, batch_size=batch_size)
