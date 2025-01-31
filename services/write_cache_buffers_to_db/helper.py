"""
Helper functions for the write_cache_buffers_to_db service.
"""

import pandas as pd

from lib.db.queue import Queue, QueueItem
from lib.db.manage_local_data import export_data_to_local_storage
from lib.log.logger import get_logger

logger = get_logger(__name__)

SERVICES_TO_WRITE = [
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "ml_inference_ime",
]


def transform_queue_items(queue_items: list[QueueItem]) -> pd.DataFrame:
    """Transforms the queue items into a list of dictionaries."""
    # TODO: probably need to add some steps too no?
    queue_item_dicts: list[dict] = [item.model_dump() for item in queue_items]
    df = pd.DataFrame(queue_item_dicts)
    return df


def write_cache_buffer_queue_to_db(service: str):
    """Writes the cache buffer queue to the database.

    Takes the output queue for the given service/integration (which should
    have its latest records) and writes them to the database. Then deletes
    the records from the queue.
    """
    queue = Queue(queue_name=f"output_{service}")

    latest_queue_items: list[QueueItem] = queue.load_items_from_queue(
        limit=None, min_id=None, min_timestamp=None
    )
    latest_queue_item_ids = [item.id for item in latest_queue_items]
    df = transform_queue_items(latest_queue_items)

    logger.info(
        f"Exporting {len(df)} records to local storage for service {service}..."
    )
    export_data_to_local_storage(service=service, df=df, export_format="parquet")
    logger.info(
        f"Finished exporting {len(df)} records to local storage for service {service}..."
    )

    logger.info(
        f"Deleting {len(latest_queue_item_ids)} records from queue for service {service}..."
    )
    queue.batch_delete_items_by_ids(ids=latest_queue_item_ids)
    logger.info(
        f"Finished deleting {len(latest_queue_item_ids)} records from queue for service {service}..."
    )


def write_all_cache_buffer_queues_to_dbs():
    logger.info("Starting to write all cache buffer queues to DBs...")
    for service in SERVICES_TO_WRITE:
        logger.info(f"Migrating cache buffer queue for service {service}...")
        write_cache_buffer_queue_to_db(service=service)
    logger.info("Finished writing all cache buffer queues to DBs.")
