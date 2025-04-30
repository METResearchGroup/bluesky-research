"""Manager class for running PDS backfill sync across multiple DIDs and PDS
endpoints."""

from typing import Optional
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from pprint import pprint
import random
import sqlite3
import time

from lib.db.queue import Queue
from lib.helper import create_batches
from lib.log.logger import get_logger
from lib.telemetry.prometheus.server import start_metrics_server
from services.backfill.core.backfill import get_plc_directory_doc
from services.backfill.core.worker import (
    get_previously_processed_dids,
    get_write_queues,
    PDSEndpointWorker,
)
from services.backfill.core.constants import current_dir
from services.backfill.scripts.determine_dids_to_backfill import sqlite_db_path
from services.backfill.storage.write_queue_to_db import write_pds_queue_to_db

# this is backfilled from `determine_dids_to_backfill.py`, which is triggered
# in the handler.py file.
did_plc_sqlite_db_path = os.path.join(current_dir, "did_plc.sqlite")

# I already optimized SQLite writes as a part of this `Queue` class so I'll
# just continue using this abstraction for non-queue, more generic DB cases.
did_plc_db = Queue(
    queue_name="did_plc",
    create_new_queue=True,
    temp_queue=True,
    temp_queue_path=did_plc_sqlite_db_path,
)


pds_endpoint_to_dids_db_path = os.path.join(current_dir, "pds_endpoint_to_dids.sqlite")
pds_endpoint_to_dids_db = Queue(
    queue_name="pds_endpoint_to_dids",
    create_new_queue=True,
    temp_queue=True,
    temp_queue_path=pds_endpoint_to_dids_db_path,
)

default_plc_requests_per_second = 50
default_plc_backfill_batch_size = 250
logging_minibatch_size = 25
# max_plc_threads = 4
max_plc_threads = 64
# max PDS endpoints to sync at once.
# max_pds_endpoints_to_sync = 32
max_pds_endpoints_to_sync = 200  # top 200 PDS endpoints.

# I want to focus on the big PDS endpoints as opposed to custom PDSes, so I'll
# set a minimum # of DIDs that a PDS endpoint must have before it is synced.
min_dids_per_pds_endpoint = 50

logger = get_logger(__name__)

# start Prometheus server for tracking.
start_metrics_server(port=8000)


def load_dids_from_local_db() -> list[str]:
    """Loads the DIDs from the local SQLite database."""
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT did FROM dids")
    dids = cursor.fetchall()
    conn.close()
    return [did[0] for did in dids]


def export_did_to_pds_endpoint_map_to_local_db(
    did_to_pds_endpoint_map: dict[str, str],
) -> None:
    """Exports the DID to PLC endpoint map to the local SQLite database."""
    did_plc_db.batch_add_items_to_queue(
        items=[
            {"did": did, "pds_endpoint": pds_endpoint}
            for did, pds_endpoint in did_to_pds_endpoint_map.items()
        ]
    )
    logger.info(
        f"Exported DID to PLC endpoint map to local database at {did_plc_sqlite_db_path}"
    )


def load_did_to_pds_endpoint_map() -> dict[str, str]:
    """Loads the DID to PLC endpoint map from the database."""
    results = did_plc_db.load_dict_items_from_queue()
    did_to_pds_endpoint_map = {row["did"]: row["pds_endpoint"] for row in results}
    return did_to_pds_endpoint_map


def generate_pds_endpoint_to_dids_map(
    did_to_pds_endpoint_map: dict[str, str],
) -> dict[str, list[str]]:
    """Generates the PLC endpoint to DIDs map."""
    pds_endpoint_to_dids_map: dict[str, list[str]] = {}
    for did, pds_endpoint in did_to_pds_endpoint_map.items():
        if pds_endpoint not in pds_endpoint_to_dids_map:
            pds_endpoint_to_dids_map[pds_endpoint] = []
        pds_endpoint_to_dids_map[pds_endpoint].append(did)
    return pds_endpoint_to_dids_map


def export_pds_endpoint_to_dids_map_to_local_db(
    pds_endpoint_to_dids_map: dict[str, list[str]],
) -> None:
    """Exports the PLC endpoint to DIDs map to the local SQLite database."""
    pds_endpoint_to_dids_db.batch_add_items_to_queue(
        items=[
            {"pds_endpoint": pds_endpoint, "dids": ",".join(dids)}
            for pds_endpoint, dids in pds_endpoint_to_dids_map.items()
        ]
    )
    logger.info(
        f"Exported PLC endpoint to DIDs map to local database at {pds_endpoint_to_dids_db_path}"
    )


def load_pds_endpoint_to_dids_map() -> dict[str, list[str]]:
    """Loads the PLC endpoint to DIDs map from the database."""
    results = pds_endpoint_to_dids_db.load_dict_items_from_queue()
    pds_endpoint_to_dids_map = {
        row["pds_endpoint"]: row["dids"].split(",") for row in results
    }
    logger.info(
        f"Loaded PLC endpoint to DIDs map from local database at {pds_endpoint_to_dids_db_path}"
    )
    return pds_endpoint_to_dids_map


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


def single_batch_backfill_missing_did_to_pds_endpoints(
    dids: list[str],
) -> dict[str, str]:
    """Single batch backfills missing DID to PLC endpoint mappings."""
    did_to_pds_endpoint_map: dict[str, str] = {}
    logger.info(f"\tFetching PLC endpoints for {len(dids)} DIDs")

    # parallelize the requests to the PLC directory
    minibatch_did_size = max_plc_threads
    minibatch_dids_batches: list[list[str]] = create_batches(
        batch_list=dids, batch_size=minibatch_did_size
    )

    logger.info(
        f"\tFetching PLC endpoints for {len(dids)} DIDs. Splitting into {len(minibatch_dids_batches)} minibatches batches of {minibatch_did_size} DIDs each."
    )
    for i, minibatch_dids_batch in enumerate(minibatch_dids_batches):
        if i % logging_minibatch_size == 0:
            logger.info(
                f"\t\tFetching PLC endpoint for {i*minibatch_did_size}/{len(dids)} DIDs in batch"
            )
        minibatch_plc_directory_docs: list[dict] = parallelize_plc_directory_requests(
            minibatch_dids=minibatch_dids_batch
        )
        try:
            service_endpoints = [
                doc["service"][0]["serviceEndpoint"]
                for doc in minibatch_plc_directory_docs
            ]
        except Exception:
            # example failed doc: [{'message': 'DID not registered: did:web:witchy.mom'}]
            service_endpoints = []
            num_invalid_docs = 0
            for doc in minibatch_plc_directory_docs:
                if "service" in doc:
                    service_endpoints.append(doc["service"][0]["serviceEndpoint"])
                else:
                    service_endpoints.append("invalid_doc")
            if num_invalid_docs > 0:
                logger.info(f"Found {num_invalid_docs} invalid docs for batch {i}")
        dids_to_service_endpoints = dict(zip(minibatch_dids_batch, service_endpoints))
        did_to_pds_endpoint_map.update(dids_to_service_endpoints)

        # NOTE: running it without the rate limit jitter still seems to be baseline slow,
        # i.e., 25 requests in 18-20 seconds, so seems like there's some delay and latency
        # already affecting this process anyways (unsure if it is my own network latency)
        # or if it is the API's latency.
        run_rate_limit_jitter(num_requests_per_second=default_plc_requests_per_second)
        if i % logging_minibatch_size == 0:
            logger.info(
                f"\t\tCompleted fetching PLC endpoints for {i*minibatch_did_size}/{len(dids)} DIDs in batch"
            )

    logger.info(f"\tCompleted fetching PLC endpoints for {len(dids)} DIDs")
    export_did_to_pds_endpoint_map_to_local_db(did_to_pds_endpoint_map)
    return did_to_pds_endpoint_map


def batch_backfill_missing_did_to_pds_endpoints(dids: list[str]) -> dict[str, str]:
    """Batch backfills missing DID to PLC endpoint mappings."""
    did_to_pds_endpoint_map: dict[str, str] = {}
    dids_batches: list[list[str]] = create_batches(
        batch_list=dids, batch_size=default_plc_backfill_batch_size
    )
    logger.info(
        f"Fetching PLC endpoints for {len(dids_batches)} batches of DIDs ({len(dids)} DIDs total)"
    )
    time_start = time.time()
    for i, dids_batch in enumerate(dids_batches):
        logger.info(f"Fetching PLC endpoints for batch {i+1}/{len(dids_batches)}")
        batch_did_to_pds_endpoint_map = (
            single_batch_backfill_missing_did_to_pds_endpoints(dids=dids_batch)
        )
        did_to_pds_endpoint_map.update(batch_did_to_pds_endpoint_map)
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
    return did_to_pds_endpoint_map


def backfill_did_to_pds_endpoint_map(
    dids: list[str],
    current_did_to_pds_endpoint_map: dict[str, str],
) -> dict[str, str]:
    """Backfills the DID to PDS endpoint map to include any missing DIDs."""
    missing_dids = [did for did in dids if did not in current_did_to_pds_endpoint_map]
    if len(missing_dids) == 0:
        logger.info("No missing DIDs, returning current map.")
        return current_did_to_pds_endpoint_map
    logger.info(
        f"Backfilling the PDS endpoints for {len(missing_dids)}/{len(dids)} missing DIDs"
    )
    missing_dids_to_pds_endpoint_map = batch_backfill_missing_did_to_pds_endpoints(
        dids=missing_dids
    )

    return {
        **current_did_to_pds_endpoint_map,
        **missing_dids_to_pds_endpoint_map,
    }


def check_if_pds_endpoint_backfill_completed(
    pds_endpoint: str, expected_total: int
) -> bool:
    """Checks if the PDS endpoint backfill is completed."""
    previously_processed_dids: set[str] = get_previously_processed_dids(
        pds_endpoint=pds_endpoint
    )
    if previously_processed_dids:
        logger.info(
            f"(PDS endpoint: {pds_endpoint}): {len(previously_processed_dids)} DIDs backfilled. Expected {expected_total} total DIDs."
        )
    return len(previously_processed_dids) == expected_total


def calculate_completed_pds_endpoint_backfills(
    pds_endpoint_to_dids_map: dict[str, list[str]],
) -> list[str]:
    """Iterate through the PDS endpoints and check to see which ones
    are already completely backfilled, and send a list of these."""
    completed_pds_endpoint_backfills = []
    for pds_endpoint, dids in pds_endpoint_to_dids_map.items():
        if check_if_pds_endpoint_backfill_completed(
            pds_endpoint=pds_endpoint, expected_total=len(dids)
        ):
            completed_pds_endpoint_backfills.append(pds_endpoint)
    return completed_pds_endpoint_backfills


async def run_pds_backfills(
    dids: list[str],
    pds_endpoint_to_dids_map: dict[str, list[str]],
) -> None:
    """Runs backfills for a list of DIDs.

    Spins up one instance of `PDSEndpointWorker` for each PDS endpoint, which
    manages all HTTP requests, DB writes, and rate limiting for that PDS endpoint.

    HTTP requests are managed by semaphores, DB writes are offloaded to a
    background task, and CPU-intensive work is offloaded to a thread pool
    shared across all worker instances of `PDSEndpointWorker`.
    """
    # TODO: figure out optimal # of threads to use. Just used default from
    # ChatGPT.

    max_threads = 6
    max_workers_per_thread = 4

    # TODO: this shouldn't be necessary? Unsure why. I suppose it's because I
    # create a separate instance of `PDSEndpointWorker` for each PDS endpoint,
    # I'm not sure how this is managed across threads. Maybe not enough threads
    # to go around?
    pds_endpoints_to_sync = list(pds_endpoint_to_dids_map.keys())[:max_threads]
    pds_endpoint_to_dids_map = {
        pds_endpoint: pds_endpoint_to_dids_map[pds_endpoint]
        for pds_endpoint in pds_endpoints_to_sync
    }
    logger.info(
        f"Syncing {len(pds_endpoints_to_sync)} PDS endpoints: {pds_endpoints_to_sync}"
    )

    semaphore = asyncio.Semaphore(max_threads)

    async def run_single_pds_backfill(pds_endpoint: str, dids: list[str]) -> None:
        cpu_pool = ThreadPoolExecutor(max_workers=max_workers_per_thread)
        async with aiohttp.ClientSession() as session:
            logger.info(
                f"Starting PDS backfill for {pds_endpoint} with {len(dids)} DIDs."
            )
            worker = PDSEndpointWorker(
                pds_endpoint=pds_endpoint,
                dids=dids,
                session=session,
                cpu_pool=cpu_pool,
            )
            await worker.start()
            logger.info(f"Worker started for {pds_endpoint}, waiting for completion...")
            await worker.wait_done()
            await worker.shutdown()
            logger.info(f"Completed PDS backfill for {pds_endpoint}.")
        cpu_pool.shutdown(wait=True)

    async def limited_backfill(pds_endpoint: str, dids: list[str]) -> None:
        async with semaphore:
            await run_single_pds_backfill(pds_endpoint, dids)

    # Create a list of coroutines instead of ThreadPoolExecutor futures
    tasks = [
        limited_backfill(pds_endpoint, endpoint_dids)
        for pds_endpoint, endpoint_dids in pds_endpoint_to_dids_map.items()
    ]

    # Run them concurrently using asyncio
    await asyncio.gather(*tasks)

    logger.info("All PDS backfills completed.")


def run_backfills(
    dids: list[str],
    load_existing_endpoints_to_dids_map: bool = False,
    plc_backfill_only: bool = False,
    skip_completed_pds_endpoints: bool = False,
    max_pds_endpoints_to_sync: int = max_pds_endpoints_to_sync,
) -> None:
    """Runs backfills for a list of DIDs.

    Args:
        dids: list[str] - The list of DIDs to run backfills for.
        load_existing_endpoints_to_dids_map: bool - Whether to load the existing PLC endpoint to DIDs map from the local database.
        plc_backfill_only: bool - Whether to only run the PLC endpoint backfill, as opposed to doing the PLC
            endpoint backfill and then the PDS endpoint backfill.
        skip_completed_pds_endpoints: bool - Whether to skip completed PDS endpoint backfills.
        max_pds_endpoints_to_sync: int - The maximum number of PDS endpoints to sync at once.
    """
    if load_existing_endpoints_to_dids_map:
        pds_endpoint_to_dids_map: dict[str, list[str]] = load_pds_endpoint_to_dids_map()
    else:
        did_to_pds_endpoint_map: dict[str, str] = load_did_to_pds_endpoint_map()
        did_to_pds_endpoint_map: dict[str, str] = backfill_did_to_pds_endpoint_map(
            dids=dids,
            current_did_to_pds_endpoint_map=did_to_pds_endpoint_map,
        )
        pds_endpoint_to_dids_map: dict[str, list[str]] = (
            generate_pds_endpoint_to_dids_map(
                did_to_pds_endpoint_map=did_to_pds_endpoint_map,
            )
        )
        export_pds_endpoint_to_dids_map_to_local_db(pds_endpoint_to_dids_map)

    logger.info("Sorted PDS endpoints by number of DIDs (descending order)")

    # dedupe DIDs in each PDS endpoint, if necessary.
    pds_endpoint_to_dids_map = {
        pds_endpoint: list(set(dids))
        for pds_endpoint, dids in pds_endpoint_to_dids_map.items()
    }

    pds_endpoint_to_did_count = {
        endpoint: len(dids) for endpoint, dids in pds_endpoint_to_dids_map.items()
    }

    # Sort the endpoints by number of DIDs in descending order
    sorted_endpoints = sorted(
        pds_endpoint_to_did_count.items(), key=lambda item: item[1], reverse=True
    )

    # Log the top endpoints
    logger.info("Top PDS endpoints by number of DIDs:")
    for endpoint, count in sorted_endpoints[:20]:  # Show top 20
        logger.info(f"  {endpoint}: {count} DIDs")

    # Export the PDS endpoint counts.
    # pds_endpoint_to_did_count_path = os.path.join(
    #     current_dir, "pds_endpoint_to_did_count.json"
    # )
    # with open(pds_endpoint_to_did_count_path, "w") as f:
    #     json.dump(pds_endpoint_to_did_count, f, indent=2)

    # logger.info(
    #     f"Exported PDS endpoint to DID count map to {pds_endpoint_to_did_count_path}"
    # )

    if plc_backfill_only:
        logger.info("Backfilling only PLC endpoints, skipping PDS endpoint backfill.")
        return

    logger.info(
        f"Total number of possible PDS endpoints to sync: {len(pds_endpoint_to_dids_map)}"
    )

    # filter out PDS endpoints that have less than `min_dids_per_pds_endpoint` DIDs.
    pds_endpoint_to_dids_map = {
        endpoint: dids
        for endpoint, dids in pds_endpoint_to_dids_map.items()
        if len(dids) >= min_dids_per_pds_endpoint
    }

    completed_pds_endpoint_backfills = calculate_completed_pds_endpoint_backfills(
        pds_endpoint_to_dids_map=pds_endpoint_to_dids_map
    )

    # TODO: can just do this functionality with `write_queue_to_db.py`?
    # check if any of the PDS endpoints are completed but still have queues
    # that need to be flushed.
    for pds_endpoint in completed_pds_endpoint_backfills:
        queues = get_write_queues(pds_endpoint, skip_if_queue_not_found=True)
        output_results_queue: Optional[Queue] = queues["output_results_queue"]
        output_deadletter_queue: Optional[Queue] = queues["output_deadletter_queue"]

        if output_results_queue or output_deadletter_queue:
            logger.info(
                f"PDS endpoint {pds_endpoint} is completed but still has queues that need to be flushed. Flushing them..."
            )
            loop = asyncio.get_event_loop()
            loop.run_until_complete(write_pds_queue_to_db(pds_endpoint=pds_endpoint))
            output_results_queue.delete_queue()
            output_deadletter_queue.delete_queue()
            logger.info(f"Flushed queues for PDS endpoint {pds_endpoint}.")

    if skip_completed_pds_endpoints:
        pds_endpoints_to_skip = ["invalid_doc"]
        pds_endpoints_to_skip.extend(completed_pds_endpoint_backfills)
        logger.info(
            f"Skipping {len(pds_endpoints_to_skip)} PDS endpoints since their backfills are already completed: {pds_endpoints_to_skip}"
        )
        for pds_endpoint in pds_endpoints_to_skip:
            if pds_endpoint in pds_endpoint_to_dids_map:
                pds_endpoint_to_dids_map.pop(pds_endpoint)
        if len(pds_endpoint_to_dids_map) == 0:
            logger.info("No PDS endpoints to sync, exiting.")
            return

    if max_pds_endpoints_to_sync is not None:
        # sort PDS endpoints by number of DIDs in descending order.
        # We recalculate and exclude the ones that have already been backfilled.
        sorted_endpoints = sorted(
            pds_endpoint_to_did_count.items(), key=lambda item: item[1], reverse=True
        )
        # We only want to sync the top N PDS endpoints.
        sorted_endpoints = sorted_endpoints[:max_pds_endpoints_to_sync]
        sorted_endpoints = [endpoint for endpoint, _ in sorted_endpoints]
        sorted_dict = {key: pds_endpoint_to_did_count[key] for key in sorted_endpoints}
        logger.info("Sorted PDS endpoints by number of DIDs (descending order):")
        pprint(sorted_dict)
        pds_endpoint_to_dids_map = {
            endpoint: dids
            for endpoint, dids in pds_endpoint_to_dids_map.items()
            if endpoint in sorted_endpoints
        }
        logger.info(f"Only sorting the top {max_pds_endpoints_to_sync} PDS endpoints.")

    logger.info("Triggering async PDS backfills...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        run_pds_backfills(
            dids=dids,
            pds_endpoint_to_dids_map=pds_endpoint_to_dids_map,
        )
    )
    logger.info("Async PDS backfills completed.")
    loop = asyncio.get_event_loop()


def main():
    dids: list[str] = load_dids_from_local_db()

    # TODO: first start with loading a small # of DIDs (e.g., 2,000).
    # Continue a phased rollout: 2,000 -> 10,000 -> 20,000 -> 50,000 -> 100,000
    # dids = dids[:2000]
    # dids = dids[2000:4000]
    # dids = dids[4000:20000]
    # dids = dids[20000:50000]
    # dids = dids[50000:80000]
    # dids = dids[:50000]

    # Set a 3-hour time limit
    # def exit_after_timeout():
    #     logger.info("3-hour time limit reached. Exiting process...")
    #     sys.exit(0)

    # hours = 1
    # seconds = hours * 60 * 60
    # timer = threading.Timer(seconds, exit_after_timeout)
    # timer.daemon = True
    # timer.start()

    # try:
    #     run_backfills(
    #         dids=dids,
    #         load_existing_endpoints_to_dids_map=True,
    #         plc_backfill_only=False,
    #         skip_completed_pds_endpoints=True,
    #         max_pds_endpoints_to_sync=max_pds_endpoints_to_sync,
    #     )
    # finally:
    #     # Cancel the timer if backfill completes before timeout
    #     timer.cancel()
    run_backfills(
        dids=dids,
        load_existing_endpoints_to_dids_map=True,
        plc_backfill_only=False,
        skip_completed_pds_endpoints=True,
        max_pds_endpoints_to_sync=max_pds_endpoints_to_sync,
    )


if __name__ == "__main__":
    main()
