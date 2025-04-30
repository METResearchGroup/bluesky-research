"""Given a list of DIDs, query the PLC endpoint and store the results."""

from concurrent.futures import ThreadPoolExecutor
import random
import time

from api.backfill_router.config.schema import BackfillConfigSchema
from lib.db.queue import Queue
from lib.helper import create_batches
from lib.log.logger import get_logger
from services.backfill.core.backfill import get_plc_directory_doc
from services.backfill.storage.utils.main import (
    load_existing_did_to_pds_endpoint_map,
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


def export_did_to_pds_endpoint_map_to_local_db(
    did_to_pds_endpoint_map: dict[str, str],
    did_plc_db: Queue,
) -> None:
    """Exports the DID to PLC endpoint map to the local SQLite database."""
    did_plc_db.batch_add_items_to_queue(
        items=[
            {"did": did, "pds_endpoint": pds_endpoint}
            for did, pds_endpoint in did_to_pds_endpoint_map.items()
        ]
    )
    logger.info(
        f"Exported DID to PLC endpoint map to local database at {did_plc_db.db_path}"
    )


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


def filter_dids_to_query(
    dids: list[str], existing_did_to_pds_endpoint_map: dict[str, str]
) -> list[str]:
    """Loads the DIDs to query from the local SQLite database."""
    existing_dids = list(existing_did_to_pds_endpoint_map.keys())
    missing_dids = [did for did in dids if did not in existing_dids]
    return missing_dids


def query_plc_endpoint(dids: list[str], config: BackfillConfigSchema) -> None:
    """Query the PLC endpoint and store the results.

    Steps:
    1. Load the DIDs to query from the local SQLite database.
    2. Load the existing DID to PLC endpoint map from the local SQLite database.
    3. Filter the DIDs to query to only include missing DIDs.
    4. Query the PLC endpoint for the missing DIDs.
    5. Export the DID to PLC endpoint map to the local SQLite database.
    """

    dids: list[str] = load_dids_to_query(
        type=config.source.type,
        path=config.source.path,
    )
    existing_did_to_pds_endpoint_map: dict[str, str] = (
        load_existing_did_to_pds_endpoint_map(
            type=config.plc_storage.type,
            path=config.plc_storage.path,
        )
    )
    dids_to_query: list[str] = filter_dids_to_query(
        dids=dids,
        existing_did_to_pds_endpoint_map=existing_did_to_pds_endpoint_map,
    )
    did_plc_db = Queue(
        queue_name="did_plc",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=config.plc_storage.path,
    )
    did_to_pds_endpoint_map = batch_backfill_missing_did_to_pds_endpoints(
        dids=dids_to_query
    )
    export_did_to_pds_endpoint_map_to_local_db(
        did_to_pds_endpoint_map=did_to_pds_endpoint_map, did_plc_db=did_plc_db
    )
