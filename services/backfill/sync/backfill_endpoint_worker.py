"""Backfill a single PLC endpoint in a separate thread."""

import aiohttp
import asyncio
import contextlib
from datetime import datetime, timezone
import os
import time

from lib.constants import timestamp_format
from lib.db.queue import Queue
from lib.db.rate_limiter import AsyncTokenBucket
from lib.log.logger import get_logger
from services.backfill.sync.backfill import (
    get_records_from_pds_bytes,
    async_send_request_to_pds,
)
from services.backfill.sync.constants import current_dir


default_qps = 10  # assume 10 QPS max = 600/minute = 3000/5 minutes

GLOBAL_RATE_LIMIT = (
    3000  # rate limit, I think, is 3000/5 minutes, we can put our own cap.
)
MANUAL_RATE_LIMIT = 0.9 * GLOBAL_RATE_LIMIT  # we can put our own cap.

start_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)

logger = get_logger(__name__)

default_worker_threads = 5


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


class PDSEndpointWorker:
    """Wrapper class for managing the backfills for a single PDS endpoint."""

    def __init__(
        self,
        pds_endpoint: str,
        dids: list[str],
        session: aiohttp.ClientSession,
        cpu_pool,
        qps: int = default_qps,
        batch_size: int = 500,
    ):
        self.pds_endpoint = pds_endpoint
        self.dids = dids
        self.qps = qps
        self.semaphore = asyncio.Semaphore(qps)

        # token bucket, for managing rate limits.
        self.token_reset_minutes = 5  # 5 minutes, as per the rate limit spec.
        self.token_reset_seconds = self.token_reset_minutes * 60
        self.max_tokens = (
            MANUAL_RATE_LIMIT  # set conservatively to avoid max rate limit.
        )
        self.token_bucket = AsyncTokenBucket(
            max_tokens=self.max_tokens, window_seconds=self.token_reset_seconds
        )

        # permanent storage queues.
        self.write_queues = get_write_queues(pds_endpoint=pds_endpoint)
        self.output_results_queue: Queue = self.write_queues["output_results_queue"]
        self.output_deadletter_queue: Queue = self.write_queues[
            "output_deadletter_queue"
        ]
        # temporary queues.
        self.temp_results_queue = asyncio.Queue()
        self.temp_deadletter_queue = asyncio.Queue()
        self.temp_work_queue = asyncio.Queue()

        self.dids: list[str] = filter_previously_processed_dids(
            pds_endpoint=pds_endpoint,
            dids=dids,
            results_db=self.output_results_queue,
            deadletter_db=self.output_deadletter_queue,
        )
        for did in self.dids:
            self.temp_work_queue.put(did)

        self.session = session
        self.cpu_pool = cpu_pool
        self._batch_size = batch_size
        self._writer_task: asyncio.Task | None = None

    async def _process_did(self, did: str, session: aiohttp.ClientSession):
        """Processes a DID and adds the results to the temporary queues.

        Args:
            did: str, the DID to process.
            session: aiohttp.ClientSession, the session to use to send the request.

        Runs the requests to the PDS endpoint using asyncio. Number of
        concurrent requests is managed by the semaphore. CPU-intensive work
        is offloaded to the thread pool.
        """
        await self.token_bucket._acquire()
        async with self.semaphore:
            content = None
            try:
                response = await async_send_request_to_pds(
                    did=did, pds_endpoint=self.pds_endpoint, session=session
                )
                if response.status == 200:
                    content: bytes = await response.read()
                elif response.status == 429:
                    # Rate limited, put back in queue
                    await self.temp_work_queue.put(did)
                else:
                    # Error case
                    await self.temp_deadletter_queue.put({"did": did, "content": ""})
            except Exception as e:
                logger.error(f"Error processing DID {did}: {e}")
                await self.temp_deadletter_queue.put({"did": did, "content": ""})

        if content:
            # offload CPU portion to thread pool.
            loop = asyncio.get_running_loop()
            records: list[dict] = await loop.run_in_executor(
                self.cpu_pool, get_records_from_pds_bytes, content
            )
            await self.temp_results_queue.put({"did": did, "content": records})

    async def start(self):
        """Starts the backfill for a single PDS endpoint."""
        # background DB writer.
        self._writer_task = asyncio.create_task(self._writer_task())

        # spawn producers for DIDs.
        # TODO: should get DIDs from work queue, so that we can
        # always spawn tasks as needed while the work queue is not empty.
        # NOTE: is this true? Unsure.
        producer_tasks = [
            asyncio.create_task(self._process_did(did, self.session))
            for did in self.dids
        ]
        self._producer_group = asyncio.gather(*producer_tasks)

    async def wait_done(self):
        """Waits for the backfill to finish."""
        # wait for all producers to finish.
        await self._producer_group

        # wait until writer consumes everything in queues.
        await self.temp_results_queue.join()
        await self.temp_deadletter_queue.join()

    async def shutdown(self):
        """Gracefully shuts down the backfill for a single PDS endpoint."""
        if self._writer_task:
            self._writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._writer_task

    async def _writer(self) -> None:
        """Background task to flush temp queues to permanent storage in batches."""
        result_buffer: list[dict] = []
        deadletter_buffer: list[dict] = []
        global_total_records_flushed = 0
        while True:
            result: dict = await self.temp_results_queue.get()
            deadletter: dict = await self.temp_deadletter_queue.get()
            result_buffer.append(result)
            deadletter_buffer.append(deadletter)

            total_result_buffer_size = len(result_buffer)
            total_deadletter_buffer_size = len(deadletter_buffer)
            total_buffer_size = total_result_buffer_size + total_deadletter_buffer_size

            if total_buffer_size >= self._batch_size:
                await self._async_flush_buffers(
                    result_buffer=result_buffer, deadletter_buffer=deadletter_buffer
                )
                global_total_records_flushed += total_buffer_size
                result_buffer = []
                deadletter_buffer = []
                log_progress(
                    pds_endpoint=self.pds_endpoint,
                    total_dids=len(self.dids),
                    total_filtered_dids=len(self.dids) - len(self.temp_work_queue),
                    total_records_flushed=global_total_records_flushed,
                )

    async def _async_flush_buffers(
        self,
        result_buffer: list[dict],
        deadletter_buffer: list[dict],
    ) -> None:
        """Helper to flush buffers to permanent storage."""
        total_results_buffer = len(result_buffer)
        total_deadletter_buffer = len(deadletter_buffer)
        total_buffer = total_results_buffer + total_deadletter_buffer
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Flushing {total_results_buffer} results and {total_deadletter_buffer} deadletters to permanent storage. Total: {total_buffer}."
        )
        current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
        await self.output_results_queue.async_batch_add_items_to_queue(
            items=result_buffer, metadata={"timestamp": current_timestamp}
        )
        await self.output_deadletter_queue.async_batch_add_items_to_queue(
            items=deadletter_buffer, metadata={"timestamp": current_timestamp}
        )
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Finished flushing {total_results_buffer} results and {total_deadletter_buffer} deadletters to permanent storage. Total: {total_buffer}."
        )
