from typing import Optional

from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.core.constants import base_queue_name

logger = get_logger(__name__)


def write_record_type_to_cache(
    record_type: str,
    records: list[dict],
    batch_size: Optional[int] = None,
):
    if not records:
        return

    logger.info(
        f"Adding {len(records)} records to the backfill sync queue for record type {record_type}."
    )
    queue_name = f"{base_queue_name}_{record_type}"
    queue = Queue(queue_name=queue_name, create_new_queue=True)
    queue.batch_add_items_to_queue(items=records, batch_size=batch_size)


def write_records_to_cache(
    type_to_record_maps: dict[str, list[dict]],
    batch_size: Optional[int] = None,
):
    """Writes the records to the cache.

    Args:
        type_to_record_maps: A dictionary mapping record types to lists of records.
        batch_size: The batch size to use when writing to the cache.
    """
    if not type_to_record_maps:
        return

    for record_type, records in type_to_record_maps.items():
        write_record_type_to_cache(
            record_type=record_type,
            records=records,
            batch_size=batch_size,
        )
