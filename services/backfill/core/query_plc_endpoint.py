"""Given a list of DIDs, query the PLC endpoint and store the results."""

from concurrent.futures import ThreadPoolExecutor
import os
import random
import time

from services.backfill.config.schema import BackfillConfigSchema
from lib.constants import project_home_directory
from lib.db.queue import Queue
from lib.helper import create_batches
from lib.log.logger import get_logger
from services.backfill.core.backfill import get_plc_directory_doc
from services.backfill.core.models import PlcResult
from services.backfill.storage.utils.main import (
    load_existing_plc_results,
    load_dids_to_query,
)

logger = get_logger(__name__)

default_plc_requests_per_second = 50
default_plc_backfill_batch_size = 250
max_plc_threads = 64
logging_minibatch_size = 25


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


def single_batch_backfill_missing_plc_docs(
    dids: list[str],
    did_plc_db: Queue,
) -> dict[str, str]:
    """Single batch backfills missing DID to PLC endpoint mappings."""
    did_to_plc_result_map: dict[str, PlcResult] = {}
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
        plc_results: list[PlcResult] = []
        try:
            for plc_directory_doc in minibatch_plc_directory_docs:
                did = plc_directory_doc["id"]
                pds_endpoint = plc_directory_doc["service"][0]["serviceEndpoint"]
                pds_owner = (
                    "bluesky" if "bsky.network" in pds_endpoint else "not_bluesky"
                )
                handle = plc_directory_doc["alsoKnownAs"][0]
                plc_result = PlcResult(
                    did=did,
                    pds_service_endpoint=pds_endpoint,
                    pds_owner=pds_owner,
                    handle=handle,
                )
                plc_results.append(plc_result)
        except Exception as e:
            # example failed doc: [{'message': 'DID not registered: did:web:witchy.mom'}]
            logger.error(f"Error processing PLC directory doc: {e}")
        batch_did_to_plc_result_map: dict[str, PlcResult] = {
            plc_result.did: plc_result for plc_result in plc_results
        }
        did_to_plc_result_map.update(batch_did_to_plc_result_map)
        # NOTE: running it without the rate limit jitter still seems to be baseline slow,
        # i.e., 25 requests in 18-20 seconds, so seems like there's some delay and latency
        # already affecting this process anyways (unsure if it is my own network latency)
        # or if it is the API's latency.
        run_rate_limit_jitter(num_requests_per_second=default_plc_requests_per_second)
        if i % logging_minibatch_size == 0:
            logger.info(
                f"\t\tCompleted fetching PLC endpoints for {i*minibatch_did_size}/{len(dids)} DIDs in batch"
            )

    # TODO: continue removing `did_to_pds_endpoint_map` and replacing with
    # `plc_results_to_export`.
    plc_results_to_export: list[PlcResult] = list(did_to_plc_result_map.values())
    plc_results_to_export: list[dict] = [
        plc_result.model_dump() for plc_result in plc_results_to_export
    ]

    processed_dids = [plc_result["did"] for plc_result in plc_results_to_export]

    logger.info(f"\tCompleted fetching PLC endpoints for {len(dids)} DIDs")
    export_plc_results_to_local_db(
        plc_results_to_export=plc_results_to_export,
        did_plc_db=did_plc_db,
    )
    return processed_dids


def batch_backfill_missing_plc_docs(
    dids: list[str],
    did_plc_db: Queue,
) -> list[str]:
    """Batch backfills missing PLC docs."""
    dids_batches: list[list[str]] = create_batches(
        batch_list=dids, batch_size=default_plc_backfill_batch_size
    )
    logger.info(
        f"Fetching PLC endpoints for {len(dids_batches)} batches of DIDs ({len(dids)} DIDs total)"
    )
    time_start = time.time()
    for i, dids_batch in enumerate(dids_batches):
        logger.info(f"Fetching PLC endpoints for batch {i+1}/{len(dids_batches)}")
        single_batch_backfill_missing_plc_docs(
            dids=dids_batch,
            did_plc_db=did_plc_db,
        )
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


def export_plc_results_to_local_db(
    plc_results_to_export: list[dict], did_plc_db: Queue
) -> None:
    """Exports the DID to PLC endpoint map to the local SQLite database."""
    did_plc_db.batch_add_items_to_queue(items=plc_results_to_export)
    logger.info(
        f"Exported DID to PLC endpoint map to local database at {did_plc_db.db_path}"
    )


def filter_dids_to_query(
    dids: list[str], existing_queried_dids: list[str]
) -> list[str]:
    """Loads the DIDs to query from the local SQLite database."""
    missing_dids = [did for did in dids if did not in existing_queried_dids]
    return missing_dids


def query_plc_endpoint(config: BackfillConfigSchema) -> None:
    """Query the PLC endpoint and store the results.

    Steps:
    1. Load the DIDs to query from the local SQLite database.
    2. Load the existing DID to PLC endpoint map from the local SQLite database.
    3. Filter the DIDs to query to only include missing DIDs.
    4. Query the PLC endpoint for the missing DIDs.
    5. Export the DID to PLC endpoint map to the local SQLite database.
    """
    full_plc_storage_fp = os.path.join(
        project_home_directory,
        config.plc_storage.path,
    )
    full_did_storage_fp = os.path.join(
        project_home_directory,
        config.source.path,
    )
    logger.info(f"Loading DIDs from {full_did_storage_fp}")
    logger.info(f"Loading existing DID to PLC endpoint map from {full_plc_storage_fp}")

    dids: list[str] = load_dids_to_query(
        type=config.source.type, path=full_did_storage_fp
    )
    existing_plc_results: list[dict] = load_existing_plc_results(
        type=config.plc_storage.type, path=full_plc_storage_fp
    )
    existing_queried_dids = [plc_result["did"] for plc_result in existing_plc_results]
    logger.info(f"Filtering DIDs to query from {len(dids)} total DIDs")
    logger.info(
        f"Filtering DIDs to query from {len(existing_queried_dids)} existing DIDs"
    )
    dids_to_query: list[str] = filter_dids_to_query(
        dids=dids,
        existing_queried_dids=existing_queried_dids,
    )
    if len(dids_to_query) == 0 and len(existing_queried_dids) > 0:
        logger.info("No more DIDs to do a PLC backfill for.")
        return
    did_plc_db = Queue(
        queue_name="did_plc",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=full_plc_storage_fp,
    )
    batch_backfill_missing_plc_docs(dids=dids_to_query, did_plc_db=did_plc_db)
    logger.info("Completed PLC backfill for DIDs.")
