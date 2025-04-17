"""Backfill a single PLC endpoint in a separate thread."""

from datetime import datetime, timezone
import queue
import sqlite3
import time

from lib.constants import timestamp_format
from lib.log.logger import get_logger
from services.backfill.sync.backfill import send_request_to_pds

input_queue = queue.Queue()
results_queue = queue.Queue()
deadletter_queue = queue.Queue()

GLOBAL_RATE_LIMIT = (
    3000  # rate limit, I think, is 3000/5 minutes, we can put our own cap.
)
MANUAL_RATE_LIMIT = 0.9 * GLOBAL_RATE_LIMIT  # we can put our own cap.

start_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)

logger = get_logger(__name__)


def create_results_table(results_db: sqlite3.Connection):
    results_db.execute("""
        CREATE TABLE IF NOT EXISTS results (
            did TEXT,
            content TEXT,
            timestamp TEXT
        )
    """)


def create_deadletter_table(deadletter_db: sqlite3.Connection):
    deadletter_db.execute("""
        CREATE TABLE IF NOT EXISTS deadletter (
            did TEXT,
            content TEXT,
            timestamp TEXT
        )
    """)


def filter_previously_processed_dids(
    dids: list[str],
    results_db: sqlite3.Connection,
    deadletter_db: sqlite3.Connection,
) -> list[str]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    query_results = results_db.execute("SELECT did FROM results")
    query_deadletter = deadletter_db.execute("SELECT did FROM deadletter")
    previously_processed_dids = set(query_results) | set(query_deadletter)
    filtered_dids = [did for did in dids if did not in previously_processed_dids]
    total_original_dids = len(dids)
    total_remaining_dids = len(filtered_dids)
    total_dids_filtered_out = total_original_dids - total_remaining_dids
    logger.info(
        f"{total_remaining_dids} remaining after filtering out {total_dids_filtered_out}/{total_original_dids} DIDs"
    )
    return filtered_dids


def flush_queues(
    results_db: sqlite3.Connection,
    deadletter_db: sqlite3.Connection,
):
    """Flushes both the results and deadletter queues to permanent storage."""
    current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
    while not results_queue.empty():
        result: dict = results_queue.get()
        result["timestamp"] = current_timestamp
        results_db.execute(
            """
            INSERT INTO results (did, content, timestamp)
            VALUES (?, ?, ?)
        """,
            (result["did"], result["content"], result["timestamp"]),
        )

    while not deadletter_queue.empty():
        deadletter_item: dict = deadletter_queue.get()
        deadletter_item["timestamp"] = current_timestamp
        deadletter_db.execute(
            "INSERT INTO deadletter (did, content, timestamp) VALUES (?, ?, ?)",
            (
                deadletter_item["did"],
                deadletter_item["content"],
                deadletter_item["timestamp"],
            ),
        )

    results_db.close()
    deadletter_db.close()


def add_dids_to_queue(dids: list[str]):
    """Add DIDs to the queue."""
    for did in dids:
        input_queue.put(did)


def enforce_manual_rate_limit(request_count: int) -> int:
    """Enforces the manual rate limit and returns the updated new request count."""
    if request_count <= MANUAL_RATE_LIMIT:
        return request_count
    else:
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
        return 0


def run_backfill_for_pds_endpoint(pds_endpoint: str, dids: list[str]):
    """Runs backfill for a single PDS endpoint."""
    results_db_name = f"results_{pds_endpoint}.db"
    deadletter_db_name = f"deadletter_{pds_endpoint}.db"

    results_db = sqlite3.connect(results_db_name)
    deadletter_db = sqlite3.connect(deadletter_db_name)

    create_results_table(results_db=results_db)
    create_deadletter_table(deadletter_db=deadletter_db)

    filtered_dids: list[str] = filter_previously_processed_dids(
        dids=dids,
        results_db=results_db,
        deadletter_db=deadletter_db,
    )

    add_dids_to_queue(dids=filtered_dids)
    logger.info(f"Added {len(filtered_dids)} DIDs to input queue.")
    total_processed_dids_counter = 0
    request_count = 0
    while not input_queue.empty():
        did = input_queue.get()
        if total_processed_dids_counter % 100 == 0:
            logger.info(f"Processing DID {did} of {len(filtered_dids)}")
        total_processed_dids_counter += 1
        response = send_request_to_pds(did=did, pds_endpoint=pds_endpoint)
        if response.status_code == 200:
            results_queue.put({"did": did, "content": response.content})
        else:
            if response.status_code == 429:
                logger.info("Rate limit hit for PDS. Flushing queue and sleeping.")
                time_start = time.time()
                flush_queues(
                    results_db=results_db,
                    deadletter_db=deadletter_db,
                )
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
                utc_time_str = time.strftime(timestamp_format, time.gmtime(reset_time))
                logger.info(f"Reset will occur at: {utc_time_str} UTC")
                time.sleep(wait_time)
                logger.info("Waking up from sleep.")
            else:
                logger.error(
                    f"Error getting CAR file for user {did}: {response.status_code}"
                )
                logger.info(f"response.headers: {response.headers}")
                logger.info(f"response.content: {response.content}")
                logger.info("Adding user to deadletter queue.")
                deadletter_queue.put({"did": did, "content": ""})
        request_count += 1
        request_count = enforce_manual_rate_limit(request_count=request_count)
        if request_count == 0:
            logger.info("Starting with a clean slate of requests.")

    flush_queues(
        results_db=results_db,
        deadletter_db=deadletter_db,
    )
    logger.info(f"Finished processing {len(filtered_dids)} DIDs.")
