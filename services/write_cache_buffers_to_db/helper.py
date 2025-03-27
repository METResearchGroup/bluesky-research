"""
Helper functions for the write_cache_buffers_to_db service.
"""

import pandas as pd

from lib.db.queue import Queue
from lib.db.manage_local_data import export_data_to_local_storage
from lib.log.logger import get_logger
from services.backfill.sync.constants import base_queue_name, valid_types

logger = get_logger(__name__)

SERVICES_TO_WRITE = [
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "ml_inference_ime",
    "backfill_sync",
]


def write_backfill_sync_queues_to_db(clear_queue: bool = True):
    """Writes the backfill sync queues to the database.

    Args:
        clear_queue (bool): Whether to clear the queue after writing. Defaults to False.

    We iterate through each of the queues and write their records to the
    database.
    """
    for record_type in valid_types:
        queue_name = f"{base_queue_name}_{record_type}"
        queue = Queue(queue_name=queue_name, create_new_queue=True)
        latest_payloads: list[dict] = queue.load_dict_items_from_queue(
            limit=None, min_id=None, min_timestamp=None, status="pending"
        )
        df = pd.DataFrame(latest_payloads)
        latest_queue_item_ids: list[str] = df["batch_id"].tolist()

        logger.info(
            f"Exporting {len(df)} records to local storage for backfill sync record type {record_type}..."
        )
        export_data_to_local_storage(
            service="backfill_sync",
            df=df,
            export_format="parquet",
            custom_args={"record_type": record_type},
        )
        logger.info(
            f"Finished exporting {len(df)} records to local storage for backfill sync record type {record_type}..."
        )

        if clear_queue:
            logger.info(
                f"Deleting {len(latest_queue_item_ids)} records from queue for backfill sync record type {record_type}..."
            )
            queue.batch_delete_items_by_ids(ids=latest_queue_item_ids)
            logger.info(
                f"Finished deleting {len(latest_queue_item_ids)} records from queue for backfill sync record type {record_type}..."
            )


def write_cache_buffer_queue_to_db(service: str, clear_queue: bool = True):
    """Writes the cache buffer queue to the database.

    Takes the output queue for the given service/integration (which should
    have its latest records) and writes them to the database. Then deletes
    the records from the queue.
    """
    if service == "backfill_sync":
        write_backfill_sync_queues_to_db(clear_queue=clear_queue)
        return

    queue = Queue(queue_name=f"output_{service}")

    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None, min_id=None, min_timestamp=None, status="pending"
    )
    df = pd.DataFrame(latest_payloads)
    latest_queue_item_ids: list[str] = df["batch_id"].tolist()

    logger.info(
        f"Exporting {len(df)} records to local storage for service {service}..."
    )
    export_data_to_local_storage(service=service, df=df, export_format="parquet")
    logger.info(
        f"Finished exporting {len(df)} records to local storage for service {service}..."
    )

    if clear_queue:
        logger.info(
            f"Deleting {len(latest_queue_item_ids)} records from queue for service {service}..."
        )
        queue.batch_delete_items_by_ids(ids=latest_queue_item_ids)
        logger.info(
            f"Finished deleting {len(latest_queue_item_ids)} records from queue for service {service}..."
        )


def write_all_cache_buffer_queues_to_dbs(clear_queue: bool = False):
    """Write all cache buffer queues to their corresponding databases.

    Args:
        clear_queue (bool): Whether to clear the queue after writing. Defaults to False.
    """
    logger.info("Starting to write all cache buffer queues to DBs...")
    for service in SERVICES_TO_WRITE:
        logger.info(f"Migrating cache buffer queue for service {service}...")
        write_cache_buffer_queue_to_db(service=service, clear_queue=clear_queue)
    logger.info("Finished writing all cache buffer queues to DBs.")


def clear_cache(service: str):
    """Clears the cache for the given service."""
    queue = Queue(queue_name=f"output_{service}")

    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None, min_id=None, min_timestamp=None, status="pending"
    )
    df = pd.DataFrame(latest_payloads)
    latest_queue_item_ids: list[str] = df["batch_id"].tolist()
    logger.info(
        f"Deleting {len(latest_queue_item_ids)} records from queue for service {service}..."
    )
    queue.batch_delete_items_by_ids(ids=latest_queue_item_ids)
    logger.info(
        f"Finished deleting {len(latest_queue_item_ids)} records from queue for service {service}..."
    )


def clear_all_caches():
    """Clears the cache for all services."""
    for service in SERVICES_TO_WRITE:
        logger.info(f"Clearing cache for service {service}...")
        clear_cache(service=service)
        logger.info(f"Finished clearing cache for service {service}...")
