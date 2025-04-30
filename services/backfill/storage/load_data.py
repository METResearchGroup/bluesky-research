from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.storage.queue_utils import get_write_queues

logger = get_logger(__name__)


def get_dids_from_queues(pds_endpoint: str) -> list[str]:
    """Get previous DIDs from SQLite queues (if they exist).

    If they don't exist, we don't want to create new queues."""
    queues = get_write_queues(pds_endpoint, skip_if_queue_not_found=True)
    output_results_queue: Optional[Queue] = queues["output_results_queue"]
    output_deadletter_queue: Optional[Queue] = queues["output_deadletter_queue"]
    dids = []
    for queue in [output_results_queue, output_deadletter_queue]:
        if queue:
            items = queue.load_dict_items_from_queue()
            queue_dids = [item["did"] for item in items]
            dids.extend(queue_dids)
    return dids


def load_previously_liked_post_uris() -> set[str]:
    """Load the URIs of posts that have been liked by study users."""
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        custom_args={"record_type": "like"},
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        custom_args={"record_type": "like"},
    )
    df = pd.concat([active_df, cache_df])
    liked_post_uris = set(df["uri"])
    logger.info(f"Loaded {len(liked_post_uris)} liked post URIs")
    return liked_post_uris


def get_previously_processed_dids(pds_endpoint: str) -> set[str]:
    """Load previously processed DIDs from both DynamoDB (if the backfill
    is already 100% completed) and from the queues (if the backfill is
    not yet 100% completed, and thus the DIDs haven't been written to
    DynamoDB yet)."""

    # TODO: uncomment this.
    # dynamodb_previously_processed_dids = get_dids_by_pds_endpoint(pds_endpoint)
    # queue_previously_processed_dids = get_dids_from_queues(pds_endpoint)

    dynamodb_previously_processed_dids = set()
    queue_previously_processed_dids = set()

    return set(dynamodb_previously_processed_dids) | set(
        queue_previously_processed_dids
    )
