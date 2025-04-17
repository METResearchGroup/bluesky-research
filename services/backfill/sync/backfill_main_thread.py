"""Threaded application for running PDS backfill sync."""

from concurrent.futures import ThreadPoolExecutor
import os
import random
import sqlite3
import time
import threading

from lib.db.queue import Queue
from lib.helper import create_batches
from lib.log.logger import get_logger
from services.backfill.sync.backfill import get_plc_directory_doc
from services.backfill.sync.backfill_endpoint_thread import (
    run_backfill_for_pds_endpoint,
)
from services.backfill.sync.constants import current_dir
from services.backfill.sync.determine_dids_to_backfill import (
    sqlite_db_path,
)

did_plc_sqlite_db_path = os.path.join(current_dir, "did_plc.sqlite")

# I already optimized SQLite writes as a part of this `Queue` class so I'll
# just continue using this abstraction for non-queue, more generic DB cases.
did_plc_db = Queue(
    queue_name="did_plc",
    create_new_queue=True,
    temp_queue=True,
    temp_queue_path=did_plc_sqlite_db_path,
)


plc_endpoint_to_dids_db_path = os.path.join(current_dir, "plc_endpoint_to_dids.sqlite")
plc_endpoint_to_dids_db = Queue(
    queue_name="plc_endpoint_to_dids",
    create_new_queue=True,
    temp_queue=True,
    temp_queue_path=plc_endpoint_to_dids_db_path,
)

default_plc_requests_per_second = 50
default_plc_backfill_batch_size = 250
logging_minibatch_size = 25
max_plc_threads = 4

logger = get_logger(__name__)


def load_dids_from_local_db() -> list[str]:
    """Loads the DIDs from the local SQLite database."""
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT did FROM dids")
    dids = cursor.fetchall()
    conn.close()
    return [did[0] for did in dids]


def export_did_to_plc_endpoint_map_to_local_db(
    did_to_plc_endpoint_map: dict[str, str],
) -> None:
    """Exports the DID to PLC endpoint map to the local SQLite database."""
    did_plc_db.batch_add_items_to_queue(
        items=[
            {"did": did, "plc_endpoint": plc_endpoint}
            for did, plc_endpoint in did_to_plc_endpoint_map.items()
        ]
    )
    logger.info(
        f"Exported DID to PLC endpoint map to local database at {did_plc_sqlite_db_path}"
    )


def load_did_to_plc_endpoint_map() -> dict[str, str]:
    """Loads the DID to PLC endpoint map from the database."""
    results = did_plc_db.load_dict_items_from_queue()
    did_to_plc_endpoint_map = {row["did"]: row["plc_endpoint"] for row in results}
    return did_to_plc_endpoint_map


def generate_plc_endpoint_to_dids_map(
    did_to_plc_endpoint_map: dict[str, str],
) -> dict[str, list[str]]:
    """Generates the PLC endpoint to DIDs map."""
    plc_endpoint_to_dids_map: dict[str, list[str]] = {}
    for did, plc_endpoint in did_to_plc_endpoint_map.items():
        if plc_endpoint not in plc_endpoint_to_dids_map:
            plc_endpoint_to_dids_map[plc_endpoint] = []
        plc_endpoint_to_dids_map[plc_endpoint].append(did)
    return plc_endpoint_to_dids_map


def export_plc_endpoint_to_dids_map_to_local_db(
    plc_endpoint_to_dids_map: dict[str, list[str]],
) -> None:
    """Exports the PLC endpoint to DIDs map to the local SQLite database."""
    plc_endpoint_to_dids_db.batch_add_items_to_queue(
        items=[
            {"plc_endpoint": plc_endpoint, "dids": ",".join(dids)}
            for plc_endpoint, dids in plc_endpoint_to_dids_map.items()
        ]
    )
    logger.info(
        f"Exported PLC endpoint to DIDs map to local database at {plc_endpoint_to_dids_db_path}"
    )


def load_plc_endpoint_to_dids_map() -> dict[str, list[str]]:
    """Loads the PLC endpoint to DIDs map from the database."""
    results = plc_endpoint_to_dids_db.load_dict_items_from_queue()
    plc_endpoint_to_dids_map = {
        row["plc_endpoint"]: row["dids"].split(",") for row in results
    }
    logger.info(
        f"Loaded PLC endpoint to DIDs map from local database at {plc_endpoint_to_dids_db_path}"
    )
    return plc_endpoint_to_dids_map


def run_rate_limit_jitter(num_requests_per_second: int) -> None:
    """Runs a rate limit with jitter. Aims to let us send the desired # of
    requests per second, with some noise."""
    time.sleep(1 / num_requests_per_second + random.uniform(0, 0.05))


def parallelize_plc_directory_requests(minibatch_dids: list[str]) -> dict[str, str]:
    """Parallelizes requests to the PLC directory."""
    with ThreadPoolExecutor(max_workers=max_plc_threads) as executor:
        futures = [
            executor.submit(get_plc_directory_doc, did) for did in minibatch_dids
        ]
        results = [future.result() for future in futures]
    return results


def single_batch_backfill_missing_did_to_plc_endpoints(
    dids: list[str],
) -> dict[str, str]:
    """Single batch backfills missing DID to PLC endpoint mappings."""
    did_to_plc_endpoint_map: dict[str, str] = {}
    logger.info(f"\tFetching PLC endpoints for {len(dids)} DIDs")

    # parallelize the requests to the PLC directory
    minibatch_dids_batches: list[list[str]] = create_batches(
        batch_list=dids, batch_size=max_plc_threads
    )

    logger.info(
        f"\tFetching PLC endpoints for {len(dids)} DIDs. Splitting into {len(minibatch_dids_batches)} minibatches batches of {max_plc_threads} DIDs each."
    )
    for i, minibatch_dids_batch in enumerate(minibatch_dids_batches):
        if i % logging_minibatch_size == 0:
            logger.info(f"\t\tFetching PLC endpoint for {i}/{len(dids)} DIDs in batch")
        minibatch_plc_directory_docs: list[dict] = parallelize_plc_directory_requests(
            minibatch_dids=minibatch_dids_batch
        )
        service_endpoints = [
            doc["service"][0]["serviceEndpoint"] for doc in minibatch_plc_directory_docs
        ]
        dids_to_service_endpoints = dict(zip(minibatch_dids_batch, service_endpoints))
        did_to_plc_endpoint_map.update(dids_to_service_endpoints)

        # NOTE: running it without the rate limit jitter still seems to be baseline slow,
        # i.e., 25 requests in 18-20 seconds, so seems like there's some delay and latency
        # already affecting this process anyways (unsure if it is my own network latency)
        # or if it is the API's latency.
        run_rate_limit_jitter(num_requests_per_second=default_plc_requests_per_second)
        if i % logging_minibatch_size == 0:
            logger.info(
                f"\t\tCompleted fetching PLC endpoints for {i}/{len(dids)} DIDs in batch"
            )

    logger.info(f"\tCompleted fetching PLC endpoints for {len(dids)} DIDs")
    export_did_to_plc_endpoint_map_to_local_db(did_to_plc_endpoint_map)
    return did_to_plc_endpoint_map


def batch_backfill_missing_did_to_plc_endpoints(dids: list[str]) -> dict[str, str]:
    """Batch backfills missing DID to PLC endpoint mappings."""
    did_to_plc_endpoint_map: dict[str, str] = {}
    dids_batches: list[list[str]] = create_batches(
        batch_list=dids, batch_size=default_plc_backfill_batch_size
    )
    logger.info(
        f"Fetching PLC endpoints for {len(dids_batches)} batches of DIDs ({len(dids)} DIDs total)"
    )
    time_start = time.time()
    for i, dids_batch in enumerate(dids_batches):
        logger.info(f"Fetching PLC endpoints for batch {i+1}/{len(dids_batches)}")
        batch_did_to_plc_endpoint_map = (
            single_batch_backfill_missing_did_to_plc_endpoints(dids=dids_batch)
        )
        did_to_plc_endpoint_map.update(batch_did_to_plc_endpoint_map)
        logger.info(
            f"Completed fetching PLC endpoints for batch {i+1}/{len(dids_batches)}"
        )
    time_end = time.time()
    total_time_seconds = time_end - time_start
    total_time_minutes = total_time_seconds / 60
    logger.info(
        f"Completed fetching and exporting PLC endpoints for {len(dids_batches)} batches of DIDs ({len(dids)} DIDs total)"
    )
    logger.info(
        f"Time for PLC endpoint backfill of {len(dids)} DIDs: {total_time_minutes} minutes"
    )
    return did_to_plc_endpoint_map


def backfill_did_to_plc_endpoint_map(
    dids: list[str],
    current_did_to_plc_endpoint_map: dict[str, str],
) -> dict[str, str]:
    """Backfills the DID to PLC endpoint map to include any missing DIDs."""
    missing_dids = [did for did in dids if did not in current_did_to_plc_endpoint_map]
    if len(missing_dids) == 0:
        logger.info("No missing DIDs, returning current map.")
        return current_did_to_plc_endpoint_map
    logger.info(
        f"Backfilling the PLC endpoints for {len(missing_dids)}/{len(dids)} missing DIDs"
    )
    missing_dids_to_plc_endpoint_map = batch_backfill_missing_did_to_plc_endpoints(
        dids=missing_dids
    )

    return {
        **current_did_to_plc_endpoint_map,
        **missing_dids_to_plc_endpoint_map,
    }


def run_backfills(
    dids: list[str],
    load_existing_endpoints_to_dids_map: bool = False,
    plc_backfill_only: bool = False,
) -> None:
    """Runs backfills for a list of DIDs.

    Args:
        dids: list[str] - The list of DIDs to run backfills for.
        load_existing_endpoints_to_dids_map: bool - Whether to load the existing PLC endpoint to DIDs map from the local database.
        plc_backfill_only: bool - Whether to only run the PLC endpoint backfill, as opposed to doing the PLC
            endpoint backfill and then the PDS endpoint backfill.
    """
    if load_existing_endpoints_to_dids_map:
        plc_endpoint_to_dids_map: dict[str, list[str]] = load_plc_endpoint_to_dids_map()
    else:
        did_to_plc_endpoint_map: dict[str, str] = load_did_to_plc_endpoint_map()
        did_to_plc_endpoint_map: dict[str, str] = backfill_did_to_plc_endpoint_map(
            dids=dids,
            current_did_to_plc_endpoint_map=did_to_plc_endpoint_map,
        )
        plc_endpoint_to_dids_map: dict[str, list[str]] = (
            generate_plc_endpoint_to_dids_map(
                did_to_plc_endpoint_map=did_to_plc_endpoint_map,
            )
        )
        export_plc_endpoint_to_dids_map_to_local_db(plc_endpoint_to_dids_map)

    for plc_endpoint in plc_endpoint_to_dids_map.keys():
        logger.info(
            f"Number of records for PLC endpoint {plc_endpoint}: {len(plc_endpoint_to_dids_map[plc_endpoint])}"
        )

    if plc_backfill_only:
        logger.info("Backfilling only PLC endpoints, skipping PDS endpoint backfill.")
        return

    for plc_endpoint in plc_endpoint_to_dids_map.keys():
        logger.info(
            f"Running backfill for PLC endpoint {plc_endpoint} for {len(plc_endpoint_to_dids_map[plc_endpoint])} DIDs"
        )
        threading.Thread(
            target=run_backfill_for_pds_endpoint,
            kwargs={
                "plc_endpoint": plc_endpoint,
                "dids": plc_endpoint_to_dids_map[plc_endpoint],
            },
        ).start()

    logger.info("Backfills started")


def main():
    dids: list[str] = load_dids_from_local_db()

    # TODO: first start with loading a small # of DIDs (e.g., 2,000).
    # Continue a phased rollout: 2,000 -> 10,000 -> 20,000 -> 50,000 -> 100,000
    dids = dids[:2000]

    run_backfills(
        dids=dids,
        load_existing_endpoints_to_dids_map=False,
        plc_backfill_only=True,
    )


if __name__ == "__main__":
    main()
