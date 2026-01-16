import os
from typing import Optional

from lib.log.logger import get_logger
from lib.db.queue import Queue
from services.backfill.pds_backfills.core.constants import (
    input_queue_name,
    base_queue_name,
)
from services.backfill.pds_backfills.core.models import PlcResult

logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))


def _default_input_queue() -> Queue:
    """Factory for default input queue."""
    return Queue(queue_name=input_queue_name, create_new_queue=True)


def get_write_queues(
    pds_endpoint: str,
    skip_if_queue_not_found: bool = False,
):
    """Get the write queues for the backfill.

    Optional to skip if the queues don't exist. We use this when we do queue
    reads, since if a backfill is 100% done, the DIDs will be in DynamoDB
    instead of in the queues, and the queues will be deleted. We don't want
    to create new queues if they don't exist, so we skip in that case."""
    # Extract the hostname from the PDS endpoint URL
    # eg., https://lepista.us-west.host.bsky.network.db -> lepista.us-west.host.bsky.network.db
    pds_hostname = (
        pds_endpoint.replace("https://", "").replace("http://", "").replace("/", "")
    )
    logger.info(f"Fetching queues for PDS hostname: {pds_hostname}")

    output_results_db_path = os.path.join(current_dir, f"results_{pds_hostname}.db")
    output_deadletter_db_path = os.path.join(
        current_dir, f"deadletter_{pds_hostname}.db"
    )

    if skip_if_queue_not_found:
        if os.path.exists(output_results_db_path):
            output_results_queue = Queue(
                queue_name=f"results_{pds_hostname}",
                create_new_queue=True,
                temp_queue=True,
                temp_queue_path=output_results_db_path,
            )
        else:
            output_results_queue = None
        if os.path.exists(output_deadletter_db_path):
            output_deadletter_queue = Queue(
                queue_name=f"deadletter_{pds_hostname}",
                create_new_queue=True,
                temp_queue=True,
                temp_queue_path=output_deadletter_db_path,
            )
        else:
            output_deadletter_queue = None
    else:
        output_results_queue = Queue(
            queue_name=f"results_{pds_hostname}",
            create_new_queue=True,
            temp_queue=True,
            temp_queue_path=output_results_db_path,
        )
        output_deadletter_queue = Queue(
            queue_name=f"deadletter_{pds_hostname}",
            create_new_queue=True,
            temp_queue=True,
            temp_queue_path=output_deadletter_db_path,
        )
    return {
        "output_results_queue": output_results_queue,
        "output_deadletter_queue": output_deadletter_queue,
    }


def load_latest_dids_to_backfill_from_queue(
    input_queue: Optional[Queue] = None,  # DI parameter
) -> list[str]:
    """Load latest DIDs to backfill from the input queue.

    Args:
        input_queue: Optional queue instance for dependency injection.
                     If not provided, uses the default input queue.

    Returns:
        List of DIDs to backfill.
    """
    queue = input_queue if input_queue is not None else _default_input_queue()
    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None,
        status="pending",
    )
    dids_to_backfill: list[str] = [payload["dids"] for payload in latest_payloads]
    return dids_to_backfill


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


def load_existing_plc_results(plc_storage_db_path: str) -> list[dict]:
    """Gets the existing DID to PDS endpoint map from the local SQLite database."""
    did_plc_db = Queue(
        queue_name="did_plc",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=plc_storage_db_path,
    )
    items = did_plc_db.load_dict_items_from_queue()
    plc_results_to_export: list[dict] = []
    for item in items:
        item.pop("batch_id")
        item.pop("batch_metadata")
        plc_result = PlcResult(**item)
        plc_results_to_export.append(plc_result.model_dump())
    return plc_results_to_export


def load_dids_to_query(db_path: str) -> list[str]:
    did_plc_db = Queue(
        queue_name="did_plc",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=db_path,
    )
    items = did_plc_db.load_dict_items_from_queue()
    dids_to_query: list[str] = []
    for batch in items:
        dids_to_query.extend(batch["dids"])
    return dids_to_query
