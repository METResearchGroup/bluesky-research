"""Backfill a single PLC endpoint in a separate thread."""

from datetime import datetime, timezone
import os
import queue
import time

from lib.constants import timestamp_format
from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.sync.backfill import send_request_to_pds
from services.backfill.sync.constants import current_dir

input_queue = queue.Queue()
temp_results_queue = queue.Queue()
temp_deadletter_queue = queue.Queue()

GLOBAL_RATE_LIMIT = (
    3000  # rate limit, I think, is 3000/5 minutes, we can put our own cap.
)
MANUAL_RATE_LIMIT = 0.9 * GLOBAL_RATE_LIMIT  # we can put our own cap.

start_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)

logger = get_logger(__name__)


def filter_previously_processed_dids(
    pds_endpoint: str,
    dids: list[str],
    results_db: Queue,
    deadletter_db: Queue,
) -> list[str]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    query_results = results_db.load_dict_items_from_queue()
    query_deadletter = deadletter_db.load_dict_items_from_queue()
    query_result_dids = [result["did"] for result in query_results]
    query_deadletter_dids = [deadletter["did"] for deadletter in query_deadletter]
    previously_processed_dids = set(query_result_dids) | set(query_deadletter_dids)
    filtered_dids = [did for did in dids if did not in previously_processed_dids]
    total_original_dids = len(dids)
    total_remaining_dids = len(filtered_dids)
    total_dids_filtered_out = total_original_dids - total_remaining_dids
    if total_dids_filtered_out > 0:
        logger.info(
            f"(PDS endpoint: {pds_endpoint}):  {total_remaining_dids} remaining after filtering out {total_dids_filtered_out}/{total_original_dids} DIDs"
        )
    else:
        logger.info(
            f"(PDS endpoint: {pds_endpoint}): No DIDs filtered out. {total_remaining_dids}/{total_original_dids} DIDs remaining."
        )
    return filtered_dids


def flush_queues(
    pds_endpoint: str,
    results_db: Queue,
    deadletter_db: Queue,
) -> int:
    """Flushes both the results and deadletter queues to permanent storage."""
    current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
    current_results = []
    current_deadletter = []

    while not temp_results_queue.empty():
        current_results.append(results_db.get())

    while not temp_deadletter_queue.empty():
        current_deadletter.append(deadletter_db.get())

    total_current_results = len(current_results)
    total_current_deadletters = len(current_deadletter)
    results_db.batch_add_items_to_queue(
        items=current_results,
        metadata={"timestamp": current_timestamp},
    )
    deadletter_db.batch_add_items_to_queue(
        items=current_deadletter,
        metadata={"timestamp": current_timestamp},
    )
    total_records_flushed = total_current_results + total_current_deadletters
    logger.info(
        f"""
            (PDS endpoint {pds_endpoint}): Finished flushing
            {total_current_results} results and {total_current_deadletters}
            deadletters to permanent storage.
            Total records flushed: {total_records_flushed}
        """
    )
    return total_records_flushed


def add_dids_to_queue(dids: list[str]):
    """Add DIDs to the queue."""
    for did in dids:
        input_queue.put(did)


def enforce_manual_rate_limit():
    """Enforces the manual rate limit and returns the updated new request count."""
    time_start = time.time()
    total_minutes_sleep = 5
    total_seconds_sleep = total_minutes_sleep * 60
    logger.info(f"Rate limit hit. Sleeping for {total_seconds_sleep} seconds.")
    current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
    logger.info(f"Current timestamp: {current_timestamp}")
    time.sleep(total_seconds_sleep)
    time_end = time.time()
    logger.info(f"Time taken to sleep: {time_end - time_start} seconds")
    logger.info(
        f"New timestamp: {datetime.now(timezone.utc).strftime(timestamp_format)}"
    )


def log_progress(
    pds_endpoint: str,
    total_dids: int,
    total_filtered_dids: int,
    total_records_flushed: int,
    total_requests_made: int,
):
    """Logs the following:
    1. Total DIDs for this PDS endpoint.
    2. Total DIDs processed in both results and deadletter queues.
    3. Total DIDs to be processed (at the start of the run).
    4. Total DIDs processed so far in this run.
    5. Total requests made so far in this run.
    """
    logger.info(f"=== Progress for PDS endpoint {pds_endpoint} ===")
    logger.info(f"\tTotal DIDs for endpoint: {total_dids}")
    logger.info(f"\tTotal DIDs previously processed {total_dids - total_filtered_dids}")
    logger.info(f"\tTotal DIDs to be processed in this session: {total_filtered_dids}")
    logger.info(f"\tTotal DIDs processed in this session: {total_records_flushed}")
    logger.info(f"\tTotal requests made in this session: {total_requests_made}")
    logger.info(f"=== End of progress for PDS endpoint {pds_endpoint} ===")


def get_write_queues(pds_endpoint: str):
    # Extract the hostname from the PDS endpoint URL
    # eg., https://lepista.us-west.host.bsky.network.db -> lepista.us-west.host.bsky.network.db
    pds_hostname = (
        pds_endpoint.replace("https://", "").replace("http://", "").replace("/", "")
    )
    logger.info(f"Instantiating queues for PDS hostname: {pds_hostname}")

    output_results_db_path = os.path.join(current_dir, f"results_{pds_hostname}.db")
    output_deadletter_db_path = os.path.join(
        current_dir, f"deadletter_{pds_hostname}.db"
    )

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


def run_backfill_for_pds_endpoint(pds_endpoint: str, dids: list[str]):
    """Runs backfill for a single PDS endpoint."""

    write_queues = get_write_queues(pds_endpoint=pds_endpoint)
    output_results_queue: Queue = write_queues["output_results_queue"]
    output_deadletter_queue: Queue = write_queues["output_deadletter_queue"]

    filtered_dids: list[str] = filter_previously_processed_dids(
        pds_endpoint=pds_endpoint,
        dids=dids,
        results_db=output_results_queue,
        deadletter_db=output_deadletter_queue,
    )

    add_dids_to_queue(dids=filtered_dids)
    logger.info(f"Added {len(filtered_dids)} DIDs to input queue.")
    total_processed_dids_counter = 0
    request_count = 0
    global_total_records_flushed = 0
    total_requests_made = 0
    while not input_queue.empty():
        did = input_queue.get()
        if total_processed_dids_counter % 100 == 0:
            logger.info(
                f"(PDS endpoint: {pds_endpoint}): Processing DID {total_processed_dids_counter} of {len(filtered_dids)}"
            )
        total_processed_dids_counter += 1
        if pds_endpoint == "invalid_doc":
            logger.info(
                "Processing the DIDs that had invalid PLC docs. These will all go to deadletter queues."
            )
            temp_deadletter_queue.put({"did": did, "content": ""})
        else:
            response = send_request_to_pds(did=did, pds_endpoint=pds_endpoint)
            total_requests_made += 1
            if response.status_code == 200:
                temp_results_queue.put({"did": did, "content": response.content})
            else:
                if response.status_code == 429:
                    logger.info("Rate limit hit for PDS. Flushing queue and sleeping.")
                    time_start = time.time()
                    total_records_flushed = flush_queues(
                        pds_endpoint=pds_endpoint,
                        results_db=output_results_queue,
                        deadletter_db=output_deadletter_queue,
                    )
                    global_total_records_flushed += total_records_flushed
                    time_end = time.time()
                    logger.info(
                        f"Time taken to flush queue: {time_end - time_start} seconds"
                    )
                    reset_time = response.headers["ratelimit-reset"]
                    reset_timestamp = datetime.fromtimestamp(
                        reset_time, timezone.utc
                    ).strftime(timestamp_format)
                    current_time = int(time.time())
                    wait_time = reset_time - current_time
                    logger.info(f"Reset timestamp: {reset_timestamp}")
                    logger.info(f"Seconds to wait: {wait_time}")
                    utc_time_str = time.strftime(
                        timestamp_format, time.gmtime(reset_time)
                    )
                    logger.info(f"Reset will occur at: {utc_time_str} UTC")
                    time.sleep(wait_time)
                    logger.info("Waking up from sleep.")
                else:
                    # logger.error(
                    #     f"Error getting CAR file for user {did}: {response.status_code}\t{response.headers}"
                    # )
                    # logger.info(f"response.headers: {response.headers}")
                    # logger.info(f"response.content: {response.content}")
                    # logger.info("Adding user to deadletter queue.")
                    temp_deadletter_queue.put({"did": did, "content": ""})
            request_count += 1
            if request_count >= MANUAL_RATE_LIMIT:
                logger.info(
                    f"(PDS endpoint: {pds_endpoint}): Flushing queues and enforcing manual rate limit of {MANUAL_RATE_LIMIT} requests/5 minutes..."
                )
                total_records_flushed = flush_queues(
                    pds_endpoint=pds_endpoint,
                    results_db=output_results_queue,
                    deadletter_db=output_deadletter_queue,
                )
                global_total_records_flushed += total_records_flushed
                enforce_manual_rate_limit()
                request_count = 0
                log_progress(
                    pds_endpoint=pds_endpoint,
                    total_dids=len(dids),
                    total_filtered_dids=len(filtered_dids),
                    total_records_flushed=global_total_records_flushed,
                    total_requests_made=total_requests_made,
                )

    flush_queues(
        pds_endpoint=pds_endpoint,
        results_db=output_results_queue,
        deadletter_db=output_deadletter_queue,
    )
    logger.info(
        f"(PDS endpoint: {pds_endpoint}): Finished processing {len(filtered_dids)} DIDs."
    )
