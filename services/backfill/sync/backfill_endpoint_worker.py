"""Worker class for managing the backfill for a single PDS endpoint."""

import aiohttp
import asyncio
import concurrent.futures
import contextlib
from datetime import datetime, timezone
import os
import queue
import requests
import time
import collections

import numpy as np

from lib.constants import timestamp_format
from lib.db.queue import Queue
from lib.db.rate_limiter import AsyncTokenBucket
from lib.helper import create_batches
from lib.log.logger import get_logger
from lib.telemetry.prometheus.service import pds_backfill_metrics as pdm
from services.backfill.sync.backfill import (
    get_records_from_pds_bytes,
    async_send_request_to_pds,
    send_request_to_pds,
    validate_is_valid_bsky_type,
)
from services.backfill.sync.constants import current_dir


# default_qps = 10  # assume 10 QPS max = 600/minute = 3000/5 minutes
default_qps = 2  # 2 QPS max = 120/minute = 600/5 minutes
# default_qps = 1 # simplest implementation.

GLOBAL_RATE_LIMIT = (
    3000  # rate limit, I think, is 3000/5 minutes, we can put our own cap.
)
MANUAL_RATE_LIMIT = (
    0.9 * GLOBAL_RATE_LIMIT
)  # we can put conservatively to avoid max rate limit.

start_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)

logger = get_logger(__name__)

default_write_batch_size = 100


def filter_previous_did_endpoint_tuples(
    did_endpoint_tuples: list[tuple[str, str]],
    results_db: Queue,
    deadletter_db: Queue,
) -> list[tuple[str, str]]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    query_results = results_db.load_dict_items_from_queue()
    query_deadletter = deadletter_db.load_dict_items_from_queue()
    query_result_dids = [result["did"] for result in query_results]
    query_deadletter_dids = [deadletter["did"] for deadletter in query_deadletter]
    previously_processed_dids = set(query_result_dids) | set(query_deadletter_dids)
    filtered_did_endpoint_tuples = [
        did_endpoint_tuple
        for did_endpoint_tuple in did_endpoint_tuples
        if did_endpoint_tuple[0] not in previously_processed_dids
    ]
    return filtered_did_endpoint_tuples


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


def log_progress(
    pds_endpoint: str,
    total_dids: int,
    total_filtered_dids: int,
    total_records_flushed: int,
    total_requests_made: int = 0,
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


def get_single_write_queues():
    output_results_db_path = os.path.join(current_dir, "results_all.db")
    output_deadletter_db_path = os.path.join(current_dir, "deadletter_all.db")
    output_results_queue = Queue(
        queue_name="results_all",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=output_results_db_path,
    )
    output_deadletter_queue = Queue(
        queue_name="deadletter_all",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=output_deadletter_db_path,
    )
    return {
        "output_results_queue": output_results_queue,
        "output_deadletter_queue": output_deadletter_queue,
    }


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
        batch_size: int = default_write_batch_size,
    ):
        self.pds_endpoint = pds_endpoint
        self.dids = dids
        self.qps = qps
        self.semaphore = asyncio.Semaphore(qps)

        # Counters for metrics
        self.total_requests = 0
        self.total_successes = 0

        self.max_retries = 3  # number of times to retry a failed DID before adding to deadletter queue.

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

        self.session = session
        self.cpu_pool = cpu_pool
        self._batch_size = batch_size
        self._writer_task: asyncio.Task | None = None

        # Initialize queue size metrics
        pdm.BACKFILL_QUEUE_SIZES.labels(
            endpoint=self.pds_endpoint, queue_type="work"
        ).set(0)
        pdm.BACKFILL_QUEUE_SIZES.labels(
            endpoint=self.pds_endpoint, queue_type="results"
        ).set(0)
        pdm.BACKFILL_QUEUE_SIZES.labels(
            endpoint=self.pds_endpoint, queue_type="deadletter"
        ).set(0)

        # add backoff factor to avoid overloading the PDS endpoint
        # with burst requests.
        self.backoff_factor = 1.0  # 1 second backoff.

        # Track a rolling window of response times
        self.response_times = collections.deque(maxlen=20)

    async def _init_worker_queue(self):
        for did in self.dids:
            await self.temp_work_queue.put(did)
        # Update queue size metric
        queue_size = self.temp_work_queue.qsize()
        pdm.BACKFILL_QUEUE_SIZES.labels(
            endpoint=self.pds_endpoint, queue_type="work"
        ).set(queue_size)
        pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=self.pds_endpoint).set(queue_size)

    async def _process_did(
        self, did: str, session: aiohttp.ClientSession, max_retries: int = None
    ):
        """Processes a DID and adds the results to the temporary queues.

        Args:
            did: str, the DID to process.
            session: aiohttp.ClientSession, the session to use to send the request.

        Runs the requests to the PDS endpoint using asyncio. Number of
        concurrent requests is managed by the semaphore. CPU-intensive work
        is offloaded to the thread pool.
        """
        if not max_retries:
            max_retries = self.max_retries
        retry_count = 0

        self.total_requests += 1

        while retry_count < max_retries:
            try:
                content = None
                pdm.BACKFILL_REQUESTS.labels(endpoint=self.pds_endpoint).inc()
                pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).inc()
                await self.token_bucket._acquire()
                pdm.BACKFILL_TOKENS_LEFT.labels(endpoint=self.pds_endpoint).set(
                    self.token_bucket._tokens
                )

                start = time.perf_counter()
                async with self.semaphore:
                    response = await async_send_request_to_pds(
                        did=did, pds_endpoint=self.pds_endpoint, session=session
                    )
                    if response.status == 200:
                        content: bytes = await response.read()
                    elif response.status == 429:
                        # Rate limited, put back in queue
                        await asyncio.sleep(1 * (2**retry_count))  # Exponential backoff
                        await self.temp_work_queue.put(did)
                        pdm.BACKFILL_QUEUE_SIZES.labels(
                            endpoint=self.pds_endpoint, queue_type="work"
                        ).set(self.temp_work_queue.qsize())
                        return
                    else:
                        # Error case
                        await self.temp_deadletter_queue.put(
                            {"did": did, "content": ""}
                        )
                        pdm.BACKFILL_DID_STATUS.labels(
                            endpoint=self.pds_endpoint, status="http_error"
                        ).inc()
                        pdm.BACKFILL_QUEUE_SIZES.labels(
                            endpoint=self.pds_endpoint, queue_type="deadletter"
                        ).set(self.temp_deadletter_queue.qsize())
                        return

                if content:
                    # offload CPU portion to thread pool.
                    # keeping outside of semaphore to avoid blocking the semaphore
                    # with the extra CPU operations.
                    loop = asyncio.get_running_loop()
                    processing_start = time.perf_counter()
                    records: list[dict] = await loop.run_in_executor(
                        self.cpu_pool, get_records_from_pds_bytes, content
                    )
                    records: list[dict] = await loop.run_in_executor(
                        self.cpu_pool,
                        lambda x: [
                            record
                            for record in x
                            if validate_is_valid_bsky_type(record)
                        ],
                        records,
                    )
                    processing_time = time.perf_counter() - processing_start
                    pdm.BACKFILL_PROCESSING_SECONDS.labels(
                        endpoint=self.pds_endpoint, operation_type="parse_records"
                    ).observe(processing_time)
                    await self.temp_results_queue.put({"did": did, "content": records})

                    # Update DID status and success metrics
                    pdm.BACKFILL_DID_STATUS.labels(
                        endpoint=self.pds_endpoint, status="success"
                    ).inc()
                    self.total_successes += 1

                    # Update success ratio
                    if self.total_requests > 0:
                        success_ratio = self.total_successes / self.total_requests
                        pdm.BACKFILL_SUCCESS_RATIO.labels(
                            endpoint=self.pds_endpoint
                        ).set(success_ratio)

                    # Update queue size metrics
                    current_results_queue_size = self.temp_results_queue.qsize()
                    pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=self.pds_endpoint).set(
                        current_results_queue_size
                    )
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="results"
                    ).set(current_results_queue_size)

                    # After successful fast responses:
                    if processing_time < 1.0:
                        self.backoff_factor = max(
                            self.backoff_factor * 0.8, 1.0
                        )  # Decrease backoff

                    # Track a rolling window of response times
                    self.response_times.append(processing_time)
                    avg_response_time = sum(self.response_times) / len(
                        self.response_times
                    )

                    # Dynamically adjust concurrency based on average response time
                    if avg_response_time > 3.0:  # If responses are getting slow
                        # Reduce effective concurrency temporarily
                        await asyncio.sleep(avg_response_time / 2)

                    return

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Track network errors specifically
                error_type = (
                    "timeout" if isinstance(e, asyncio.TimeoutError) else "connection"
                )
                pdm.BACKFILL_NETWORK_ERRORS.labels(
                    endpoint=self.pds_endpoint, error_type=error_type
                ).inc()

                retry_count += 1
                # Increment retry counter
                pdm.BACKFILL_RETRIES.labels(endpoint=self.pds_endpoint).inc()

                if retry_count < max_retries:
                    # log and retry with backoff
                    logger.warning(
                        f"(PDS endpoint: {self.pds_endpoint}): Connection error for DID {did}, retry {retry_count}: {e}"
                    )
                    await asyncio.sleep(1 * (2**retry_count))  # Exponential backoff
                else:
                    # max retries already reached, move to deadletter.
                    logger.error(
                        f"(PDS endpoint: {self.pds_endpoint}): Failed processing DID {did} after {max_retries} retries: {e}"
                    )
                    await self.temp_deadletter_queue.put({"did": did, "content": ""})
                    pdm.BACKFILL_DID_STATUS.labels(
                        endpoint=self.pds_endpoint, status="network_error"
                    ).inc()
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="deadletter"
                    ).set(self.temp_deadletter_queue.qsize())
            except Exception as e:
                pdm.BACKFILL_ERRORS.labels(endpoint=self.pds_endpoint).inc()
                # Other unexpected errors
                logger.error(
                    f"(PDS endpoint: {self.pds_endpoint}): Unexpected error processing DID {did}: {e}"
                )
                await self.temp_deadletter_queue.put({"did": did, "content": ""})
                pdm.BACKFILL_DID_STATUS.labels(
                    endpoint=self.pds_endpoint, status="unexpected_error"
                ).inc()
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="deadletter"
                ).set(self.temp_deadletter_queue.qsize())
            finally:
                pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).dec()

                # Track both the summary and histogram for latency
                request_latency = time.perf_counter() - start
                pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
                    request_latency
                )
                pdm.BACKFILL_LATENCY_HISTOGRAM.labels(
                    endpoint=self.pds_endpoint
                ).observe(request_latency)

    async def start(self):
        """Starts the backfill for a single PDS endpoint."""
        # initialize the worker queue.
        await self._init_worker_queue()

        # start the background DB writer.
        self._writer_task = asyncio.create_task(self._writer())

        # create worker tasks. These worker tasks will pull DIDs from the
        # worker queue and process them. Fixed relative to desired QPS.
        num_workers = min(self.qps * 2, len(self.dids))
        producer_tasks = [
            asyncio.create_task(self._worker_loop()) for _ in range(num_workers)
        ]
        self._producer_group = asyncio.gather(*producer_tasks)

    async def _worker_loop(self):
        """Worker loop that processes DIDs from the queue until it's empty.

        Allows us to manage DIDs by pulling from the temp worker queues and
        processing them instead of defining a fixed number of tasks.
        """
        while True:
            # get DID from the queue.
            try:
                did = await asyncio.wait_for(self.temp_work_queue.get(), timeout=1.0)
                # Update work queue size metric
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="work"
                ).set(self.temp_work_queue.qsize())
            except asyncio.TimeoutError:
                # if queue is empty, check if work is done.
                if self.temp_work_queue.empty():
                    break
                continue

            # process DID
            try:
                await self._process_did(did=did, session=self.session)
            finally:
                # mark task as done even if it failed.
                self.temp_work_queue.task_done()

    async def wait_done(self):
        """Waits for the backfill to finish."""
        # wait for the work queue to be empty
        await self.temp_work_queue.join()

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
            while not self.temp_results_queue.empty():
                result: dict = await self.temp_results_queue.get()
                result_buffer.append(result)
                self.temp_results_queue.task_done()
                # Update queue size metric
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="results"
                ).set(self.temp_results_queue.qsize())

            while not self.temp_deadletter_queue.empty():
                deadletter: dict = await self.temp_deadletter_queue.get()
                deadletter_buffer.append(deadletter)
                self.temp_deadletter_queue.task_done()
                # Update queue size metric
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="deadletter"
                ).set(self.temp_deadletter_queue.qsize())

            total_result_buffer_size = len(result_buffer)
            total_deadletter_buffer_size = len(deadletter_buffer)
            total_buffer_size = total_result_buffer_size + total_deadletter_buffer_size

            if total_buffer_size >= self._batch_size:
                flush_start = time.perf_counter()
                await self._async_flush_buffers(
                    result_buffer=result_buffer, deadletter_buffer=deadletter_buffer
                )
                flush_time = time.perf_counter() - flush_start
                pdm.BACKFILL_DB_FLUSH_SECONDS.labels(
                    endpoint=self.pds_endpoint
                ).observe(flush_time)
                global_total_records_flushed += total_buffer_size
                result_buffer = []
                deadletter_buffer = []
                log_progress(
                    pds_endpoint=self.pds_endpoint,
                    total_dids=len(self.dids),
                    total_filtered_dids=len(self.dids) - len(self.temp_work_queue),
                    total_records_flushed=global_total_records_flushed,
                    total_requests_made=self.total_requests,
                )

            # Avoid tight loop if both queues are empty
            if self.temp_results_queue.empty() and self.temp_deadletter_queue.empty():
                await asyncio.sleep(0.1)

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
            f"(PDS endpoint: {self.pds_endpoint}): Flushing {total_buffer} results: {total_results_buffer} records and {total_deadletter_buffer} deadletters to permanent storage."
        )
        current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)

        flush_start = time.perf_counter()

        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Starting flush to results queue..."
        )
        try:
            await self.output_results_queue.async_batch_add_items_to_queue(
                items=result_buffer, metadata={"timestamp": current_timestamp}
            )
        except Exception as e:
            logger.error(f"SQLite operation failed: {e}", exc_info=True)
            raise
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Finished flushing results queue."
        )
        total_deadletter_buffer = len(deadletter_buffer)
        if total_deadletter_buffer > 0:
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Flushing {total_deadletter_buffer} deadletters to permanent storage."
            )
            await self.output_deadletter_queue.async_batch_add_items_to_queue(
                items=deadletter_buffer, metadata={"timestamp": current_timestamp}
            )
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Finished flushing deadletter queue."
            )
        else:
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): No deadletters to flush."
            )

        flush_time = time.perf_counter() - flush_start
        pdm.BACKFILL_PROCESSING_SECONDS.labels(
            endpoint=self.pds_endpoint, operation_type="db_flush"
        ).observe(flush_time)

        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Finished flushing {total_buffer} results: {total_results_buffer} records and {total_deadletter_buffer} deadletters to permanent storage."
        )


class TokenBucket:
    """Synchronous implementation of a token bucket rate limiter."""

    def __init__(self, max_tokens: int, window_seconds: int):
        """Initialize the token bucket.

        Args:
            max_tokens: int, the maximum number of tokens in the bucket.
            window_seconds: int, the time window in seconds over which the tokens are refilled.
        """
        self._max_tokens = max_tokens
        self._window_seconds = window_seconds
        self._tokens = max_tokens
        self._token_rate = self._max_tokens / self._window_seconds
        self._last_refill = time.perf_counter()

    def _acquire(self) -> None:
        """Acquires a token from the bucket.

        Blocks until a token is available. Refills tokens based on elapsed time.
        """
        while True:
            now = time.perf_counter()
            time_since_last_refill = now - self._last_refill
            self._tokens = min(
                self._tokens + (time_since_last_refill * self._token_rate),
                self._max_tokens,
            )
            self._last_refill = now

            if self._tokens >= 1:
                self._tokens -= 1
                return
            else:
                # Sleep for a short time before checking again
                time.sleep(0.05)


# class PDSEndpointWorkerSynchronous:
#     """Synchronous version of PDSEndpointWorker."""

#     def __init__(self, did_to_pds_endpoint_map: dict[str, str]):
#         self.did_to_pds_endpoint_map = did_to_pds_endpoint_map

#         self.did_pds_tuples: list[tuple[str, str]] = [
#             (did, pds_endpoint)
#             for did, pds_endpoint
#             in did_to_pds_endpoint_map.items()
#         ]

#         # Counters for metrics
#         self.total_requests = 0
#         self.total_successes = 0

#         self.max_retries = 3  # number of times to retry a failed DID before adding to deadletter queue.

#         # token bucket, for managing rate limits.
#         self.token_reset_minutes = 5  # 5 minutes, as per the rate limit spec.
#         self.token_reset_seconds = self.token_reset_minutes * 60
#         self.max_tokens = (
#             MANUAL_RATE_LIMIT  # set conservatively to avoid max rate limit.
#         )
#         self.token_bucket = TokenBucket(
#             max_tokens=self.max_tokens, window_seconds=self.token_reset_seconds
#         )

#         # permanent storage queues.
#         self.write_queues = get_single_write_queues()
#         self.output_results_queue: Queue = self.write_queues["output_results_queue"]
#         self.output_deadletter_queue: Queue = self.write_queues[
#             "output_deadletter_queue"
#         ]

#         # temporary queues.
#         self.temp_results_queue = queue.Queue()
#         self.temp_deadletter_queue = queue.Queue()
#         self.temp_work_queue = queue.Queue()

#         # TODO: change this and add `filter_previously_processed_dids`
#         # to eventually avoid duplication.
#         max_records = 1000
#         # max_records = 100000
#         original_len_did_pds_tuples = len(self.did_pds_tuples)
#         self.did_pds_tuples = filter_previous_did_endpoint_tuples(
#             did_endpoint_tuples=self.did_pds_tuples,
#             results_db=self.output_results_queue,
#             deadletter_db=self.output_deadletter_queue,
#         )
#         filtered_len_did_pds_tuples = len(self.did_pds_tuples)
#         diff_len_did_pds_tuples = original_len_did_pds_tuples - filtered_len_did_pds_tuples
#         logger.info(
#             f"Filtered {diff_len_did_pds_tuples} DIDs from {original_len_did_pds_tuples} original DIDs."
#         )
#         self.did_pds_tuples = self.did_pds_tuples[:max_records]
#         logger.info(
#             f"Truncated DIDs from {original_len_did_pds_tuples} original DIDs to {len(self.did_pds_tuples)} DIDs with cap of {max_records}."
#         )

#         self.total_did_pds_tuples = len(self.did_pds_tuples)

#         # self.dids = filter_previously_processed_dids(
#         #     pds_endpoint=self.pds_endpoint,
#         #     dids=self.dids,
#         #     results_db=self.output_results_queue,
#         #     deadletter_db=self.output_deadletter_queue,
#         # )

#         # add backoff factor to avoid overloading the PDS endpoint
#         # with burst requests.
#         self.backoff_factor = 1.0  # 1 second backoff.

#         # Track a rolling window of response times
#         self.response_times = collections.deque(maxlen=20)

#     def _process_did(self, did: str, pds_endpoint: str):
#         retry_count = 0
#         max_retries = self.max_retries
#         self.total_requests += 1
#         start = time.perf_counter()
#         while retry_count < max_retries:
#             try:
#                 content = None
#                 pdm.BACKFILL_REQUESTS.labels(endpoint=pds_endpoint).inc()
#                 pdm.BACKFILL_INFLIGHT.labels(endpoint=pds_endpoint).inc()
#                 self.token_bucket._acquire()
#                 pdm.BACKFILL_TOKENS_LEFT.labels(endpoint=pds_endpoint).set(
#                     self.token_bucket._tokens
#                 )
#                 response = send_request_to_pds(did=did, pds_endpoint=pds_endpoint)
#                 content = None
#                 if response.status_code == 200:
#                     content: bytes = response.content
#                     self.temp_results_queue.put({"did": did, "content": content})
#                 elif response.status_code == 429:
#                     # Rate limited, put back in queue
#                     time.sleep(1 * (2**retry_count))  # Exponential backoff
#                     self.temp_work_queue.put(did)
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=pds_endpoint, queue_type="work"
#                     ).set(self.temp_work_queue.qsize())
#                     return
#                 else:
#                     # Error case
#                     self.temp_deadletter_queue.put({"did": did, "content": ""})
#                     pdm.BACKFILL_DID_STATUS.labels(
#                         endpoint=pds_endpoint, status="http_error"
#                     ).inc()
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=pds_endpoint, queue_type="deadletter"
#                     ).set(self.temp_deadletter_queue.qsize())
#                     return

#                 # if content:
#                 #     # offload CPU portion to thread pool.
#                 #     # keeping outside of semaphore to avoid blocking the semaphore
#                 #     # with the extra CPU operations.
#                 #     processing_start = time.perf_counter()
#                 #     records = get_records_from_pds_bytes(content)
#                 #     records = [
#                 #         record
#                 #         for record in records
#                 #         if validate_is_valid_bsky_type(record)
#                 #     ]
#                 #     processing_time = time.perf_counter() - processing_start
#                 #     pdm.BACKFILL_PROCESSING_SECONDS.labels(
#                 #         endpoint=pds_endpoint, operation_type="parse_records"
#                 #     ).observe(processing_time)
#                 #     self.temp_results_queue.put({"did": did, "content": records})

#                 #     # Update DID status and success metrics
#                 #     pdm.BACKFILL_DID_STATUS.labels(
#                 #         endpoint=pds_endpoint, status="success"
#                 #     ).inc()
#                 #     self.total_successes += 1

#                 #     # Update success ratio
#                 #     if self.total_requests > 0:
#                 #         success_ratio = self.total_successes / self.total_requests
#                 #         pdm.BACKFILL_SUCCESS_RATIO.labels(
#                 #             endpoint=pds_endpoint
#                 #         ).set(success_ratio)

#                 #     # Update queue size metrics
#                 #     current_results_queue_size = self.temp_results_queue.qsize()
#                 #     pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=pds_endpoint).set(
#                 #         current_results_queue_size
#                 #     )
#                 #     pdm.BACKFILL_QUEUE_SIZES.labels(
#                 #         endpoint=pds_endpoint, queue_type="results"
#                 #     ).set(current_results_queue_size)

#                 #     # After successful fast responses:
#                 #     if processing_time < 1.0:
#                 #         self.backoff_factor = max(
#                 #             self.backoff_factor * 0.8, 1.0
#                 #         )  # Decrease backoff

#                 #     # Track a rolling window of response times
#                 #     self.response_times.append(processing_time)
#                 #     avg_response_time = sum(self.response_times) / len(
#                 #         self.response_times
#                 #     )

#                 #     # Dynamically adjust concurrency based on average response time
#                 #     if avg_response_time > 3.0:  # If responses are getting slow
#                 #         # Reduce effective concurrency temporarily
#                 #         time.sleep(avg_response_time / 2)

#                 #     return

#             except (requests.RequestException, requests.Timeout) as e:
#                 # Track network errors specifically
#                 error_type = (
#                     "timeout" if isinstance(e, requests.Timeout) else "connection"
#                 )
#                 pdm.BACKFILL_NETWORK_ERRORS.labels(
#                     endpoint=pds_endpoint, error_type=error_type
#                 ).inc()

#                 retry_count += 1
#                 # Increment retry counter
#                 pdm.BACKFILL_RETRIES.labels(endpoint=pds_endpoint).inc()

#                 if retry_count < max_retries:
#                     # log and retry with backoff
#                     logger.warning(
#                         f"(PDS endpoint: {pds_endpoint}): Connection error for DID {did}, retry {retry_count}: {e}"
#                     )
#                     time.sleep(1 * (2**retry_count))  # Exponential backoff
#                 else:
#                     # max retries already reached, move to deadletter.
#                     logger.error(
#                         f"(PDS endpoint: {pds_endpoint}): Failed processing DID {did} after {max_retries} retries: {e}"
#                     )
#                     self.temp_deadletter_queue.put({"did": did, "content": ""})
#                     pdm.BACKFILL_DID_STATUS.labels(
#                         endpoint=pds_endpoint, status="network_error"
#                     ).inc()
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=pds_endpoint, queue_type="deadletter"
#                     ).set(self.temp_deadletter_queue.qsize())
#             except Exception as e:
#                 pdm.BACKFILL_ERRORS.labels(endpoint=pds_endpoint).inc()
#                 # Other unexpected errors
#                 logger.error(
#                     f"(PDS endpoint: {pds_endpoint}): Unexpected error processing DID {did}: {e}"
#                 )
#                 self.temp_deadletter_queue.put({"did": did, "content": ""})
#                 pdm.BACKFILL_DID_STATUS.labels(
#                     endpoint=pds_endpoint, status="unexpected_error"
#                 ).inc()
#                 pdm.BACKFILL_QUEUE_SIZES.labels(
#                     endpoint=pds_endpoint, queue_type="deadletter"
#                 ).set(self.temp_deadletter_queue.qsize())
#             finally:
#                 pdm.BACKFILL_INFLIGHT.labels(endpoint=pds_endpoint).dec()

#                 # Track both the summary and histogram for latency
#                 request_latency = time.perf_counter() - start
#                 pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=pds_endpoint).observe(
#                     request_latency
#                 )
#                 pdm.BACKFILL_LATENCY_HISTOGRAM.labels(
#                     endpoint=pds_endpoint
#                 ).observe(request_latency)

#     def flush_to_db(self):
#         results = []
#         deadletters = []
#         current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
#         while not self.temp_results_queue.empty():
#             # results.append(self.temp_results_queue.get())
#             temp_result = self.temp_results_queue.get()
#             records = get_records_from_pds_bytes(temp_result["content"])
#             records = [
#                 record
#                 for record in records
#                 if validate_is_valid_bsky_type(record)
#             ]
#             results.extend(records)
#         while not self.temp_deadletter_queue.empty():
#             deadletters.append(self.temp_deadletter_queue.get())
#         logger.info(
#             f"Flushing {len(results)} results and {len(deadletters)} deadletters to DB..."
#         )
#         if len(results) > 0:
#             self.output_results_queue.batch_add_items_to_queue(
#                 items=results, metadata={"timestamp": current_timestamp}
#             )
#         if len(deadletters) > 0:
#             self.output_deadletter_queue.batch_add_items_to_queue(
#                 items=deadletters, metadata={"timestamp": current_timestamp}
#             )

#     def run(self):
#         for did, pds_endpoint in self.did_pds_tuples:
#             self.temp_work_queue.put((did, pds_endpoint))
#         logger.info(f"Processing {len(self.did_pds_tuples)} DIDs...")
#         current_idx = 0
#         # log_batch_interval = 50
#         log_batch_interval = 1
#         total_dids = len(self.did_pds_tuples)

#         total_threads = 8 # TODO: prob increase?
#         latest_batch = []

#         total_dids_processed = 0
#         total_batches = self.total_did_pds_tuples // total_threads

#         while not self.temp_work_queue.empty():
#             latest_batch = []
#             # Get a batch of DIDs to process
#             for _ in range(total_threads):
#                 if not self.temp_work_queue.empty():
#                     latest_batch.append(self.temp_work_queue.get())
#                     total_dids_processed += 1
#                 else:
#                     break

#             # if current_idx % log_batch_interval == 0:
#             #     logger.info(f"Processing DID {current_idx}/{total_dids}...")

#             if current_idx % total_batches == 0:
#                 logger.info(
#                     f"Processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
#                 )

#             # Process DIDs in parallel using ThreadPoolExecutor
#             with concurrent.futures.ThreadPoolExecutor(
#                 max_workers=total_threads
#             ) as executor:
#                 futures = [
#                     executor.submit(self._process_did, did, pds_endpoint)
#                     for did, pds_endpoint in latest_batch
#                 ]
#                 # Wait for all threads to complete
#                 concurrent.futures.wait(futures)

#             if (
#                 total_dids_processed > 0
#                 and total_dids_processed % default_write_batch_size == 0
#             ):
#                 logger.info(
#                     f"Total DIDs processed so far: {total_dids_processed}. Flushing to DB."
#                 )
#                 self.flush_to_db()
#                 logger.info(
#                     f"Total DIDs processed so far: {total_dids_processed}. Flushed to DB."
#                 )

#             # if current_idx % log_batch_interval == 0:
#             #     logger.info(f"Completed processing {current_idx}/{total_dids} DIDs...")
#             if current_idx % total_batches == 0:
#                 logger.info(
#                     f"Completed processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
#                 )
#             current_idx += 1

#         logger.info(f"Finished processing all DIDs.")


class PDSEndpointWorkerSynchronous:
    """Synchronous version of PDSEndpointWorker."""

    def __init__(self, pds_endpoint: str, dids: list[str]):
        self.pds_endpoint = pds_endpoint
        self.dids = dids

        # Counters for metrics
        self.total_requests = 0
        self.total_successes = 0

        self.max_retries = 3  # number of times to retry a failed DID before adding to deadletter queue.

        # token bucket, for managing rate limits.
        self.token_reset_minutes = 5  # 5 minutes, as per the rate limit spec.
        self.token_reset_seconds = self.token_reset_minutes * 60
        self.max_tokens = (
            MANUAL_RATE_LIMIT  # set conservatively to avoid max rate limit.
        )
        self.token_bucket = TokenBucket(
            max_tokens=self.max_tokens, window_seconds=self.token_reset_seconds
        )

        # permanent storage queues.
        self.write_queues = get_write_queues(pds_endpoint=pds_endpoint)
        self.output_results_queue: Queue = self.write_queues["output_results_queue"]
        self.output_deadletter_queue: Queue = self.write_queues[
            "output_deadletter_queue"
        ]

        # temporary queues.
        self.temp_results_queue = queue.Queue()
        self.temp_deadletter_queue = queue.Queue()
        self.temp_work_queue = queue.Queue()

        self.dids = filter_previously_processed_dids(
            pds_endpoint=self.pds_endpoint,
            dids=self.dids,
            results_db=self.output_results_queue,
            deadletter_db=self.output_deadletter_queue,
        )

        # add backoff factor to avoid overloading the PDS endpoint
        # with burst requests.
        self.backoff_factor = 1.0  # 1 second backoff.

        # Track a rolling window of response times
        self.response_times = collections.deque(maxlen=20)

    def _process_did(self, did: str):
        retry_count = 0
        max_retries = self.max_retries
        self.total_requests += 1
        start = time.perf_counter()
        while retry_count < max_retries:
            try:
                content = None
                pdm.BACKFILL_REQUESTS.labels(endpoint=self.pds_endpoint).inc()
                pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).inc()
                self.token_bucket._acquire()
                pdm.BACKFILL_TOKENS_LEFT.labels(endpoint=self.pds_endpoint).set(
                    self.token_bucket._tokens
                )
                request_start = time.perf_counter()
                response = send_request_to_pds(did=did, pds_endpoint=self.pds_endpoint)
                request_latency = time.perf_counter() - request_start
                logger.info(f"Request latency: {request_latency} seconds")
                pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
                    request_latency
                )
                content = None
                if response.status_code == 200:
                    content: bytes = response.content
                    self.temp_results_queue.put({"did": did, "content": content})
                elif response.status_code == 429:
                    # Rate limited, put back in queue
                    time.sleep(1 * (2**retry_count))  # Exponential backoff
                    self.temp_work_queue.put(did)
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="work"
                    ).set(self.temp_work_queue.qsize())
                    return
                else:
                    # Error case
                    self.temp_deadletter_queue.put({"did": did, "content": ""})
                    pdm.BACKFILL_DID_STATUS.labels(
                        endpoint=self.pds_endpoint, status="http_error"
                    ).inc()
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="deadletter"
                    ).set(self.temp_deadletter_queue.qsize())
                    return

                # if content:
                #     # offload CPU portion to thread pool.
                #     # keeping outside of semaphore to avoid blocking the semaphore
                #     # with the extra CPU operations.
                #     processing_start = time.perf_counter()
                #     records = get_records_from_pds_bytes(content)
                #     records = [
                #         record
                #         for record in records
                #         if validate_is_valid_bsky_type(record)
                #     ]
                #     processing_time = time.perf_counter() - processing_start
                #     pdm.BACKFILL_PROCESSING_SECONDS.labels(
                #         endpoint=pds_endpoint, operation_type="parse_records"
                #     ).observe(processing_time)
                #     self.temp_results_queue.put({"did": did, "content": records})

                #     # Update DID status and success metrics
                #     pdm.BACKFILL_DID_STATUS.labels(
                #         endpoint=pds_endpoint, status="success"
                #     ).inc()
                #     self.total_successes += 1

                #     # Update success ratio
                #     if self.total_requests > 0:
                #         success_ratio = self.total_successes / self.total_requests
                #         pdm.BACKFILL_SUCCESS_RATIO.labels(
                #             endpoint=pds_endpoint
                #         ).set(success_ratio)

                #     # Update queue size metrics
                #     current_results_queue_size = self.temp_results_queue.qsize()
                #     pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=pds_endpoint).set(
                #         current_results_queue_size
                #     )
                #     pdm.BACKFILL_QUEUE_SIZES.labels(
                #         endpoint=pds_endpoint, queue_type="results"
                #     ).set(current_results_queue_size)

                #     # After successful fast responses:
                #     if processing_time < 1.0:
                #         self.backoff_factor = max(
                #             self.backoff_factor * 0.8, 1.0
                #         )  # Decrease backoff

                #     # Track a rolling window of response times
                #     self.response_times.append(processing_time)
                #     avg_response_time = sum(self.response_times) / len(
                #         self.response_times
                #     )

                #     # Dynamically adjust concurrency based on average response time
                #     if avg_response_time > 3.0:  # If responses are getting slow
                #         # Reduce effective concurrency temporarily
                #         time.sleep(avg_response_time / 2)

                #     return

            except (requests.RequestException, requests.Timeout) as e:
                # Track network errors specifically
                error_type = (
                    "timeout" if isinstance(e, requests.Timeout) else "connection"
                )
                pdm.BACKFILL_NETWORK_ERRORS.labels(
                    endpoint=self.pds_endpoint, error_type=error_type
                ).inc()

                retry_count += 1
                # Increment retry counter
                pdm.BACKFILL_RETRIES.labels(endpoint=self.pds_endpoint).inc()

                if retry_count < max_retries:
                    # log and retry with backoff
                    logger.warning(
                        f"(PDS endpoint: {self.pds_endpoint}): Connection error for DID {did}, retry {retry_count}: {e}"
                    )
                    time.sleep(1 * (2**retry_count))  # Exponential backoff
                else:
                    # max retries already reached, move to deadletter.
                    logger.error(
                        f"(PDS endpoint: {self.pds_endpoint}): Failed processing DID {did} after {max_retries} retries: {e}"
                    )
                    self.temp_deadletter_queue.put({"did": did, "content": ""})
                    pdm.BACKFILL_DID_STATUS.labels(
                        endpoint=self.pds_endpoint, status="network_error"
                    ).inc()
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="deadletter"
                    ).set(self.temp_deadletter_queue.qsize())
            except Exception as e:
                pdm.BACKFILL_ERRORS.labels(endpoint=self.pds_endpoint).inc()
                # Other unexpected errors
                logger.error(
                    f"(PDS endpoint: {self.pds_endpoint}): Unexpected error processing DID {did}: {e}"
                )
                self.temp_deadletter_queue.put({"did": did, "content": ""})
                pdm.BACKFILL_DID_STATUS.labels(
                    endpoint=self.pds_endpoint, status="unexpected_error"
                ).inc()
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="deadletter"
                ).set(self.temp_deadletter_queue.qsize())
            finally:
                pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).dec()

                # Track both the summary and histogram for latency
                request_latency = time.perf_counter() - start
                pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
                    request_latency
                )
                pdm.BACKFILL_LATENCY_HISTOGRAM.labels(
                    endpoint=self.pds_endpoint
                ).observe(request_latency)

    def flush_to_db(self):
        results = []
        deadletters = []
        current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
        while not self.temp_results_queue.empty():
            # results.append(self.temp_results_queue.get())
            temp_result = self.temp_results_queue.get()
            records = get_records_from_pds_bytes(temp_result["content"])
            records = [
                record for record in records if validate_is_valid_bsky_type(record)
            ]
            results.extend(records)
        while not self.temp_deadletter_queue.empty():
            deadletters.append(self.temp_deadletter_queue.get())
        logger.info(
            f"Flushing {len(results)} results and {len(deadletters)} deadletters to DB..."
        )
        if len(results) > 0:
            self.output_results_queue.batch_add_items_to_queue(
                items=results, metadata={"timestamp": current_timestamp}
            )
        if len(deadletters) > 0:
            self.output_deadletter_queue.batch_add_items_to_queue(
                items=deadletters, metadata={"timestamp": current_timestamp}
            )

    def run(self):
        for did in self.dids:
            self.temp_work_queue.put(did)
        logger.info(f"Processing {len(self.dids)} DIDs for {self.pds_endpoint}...")
        current_idx = 0
        log_batch_interval = 50
        total_dids = len(self.dids)

        total_threads = 8  # TODO: prob increase?
        latest_batch = []

        total_dids_processed = 0

        while not self.temp_work_queue.empty():
            latest_batch = []
            # Get a batch of DIDs to process
            for _ in range(total_threads):
                if not self.temp_work_queue.empty():
                    latest_batch.append(self.temp_work_queue.get())
                    total_dids_processed += 1
                else:
                    break

            if current_idx % log_batch_interval == 0:
                logger.info(f"Processing DID {current_idx}/{total_dids}...")

            # if current_idx % total_batches == 0:
            #     logger.info(
            #         f"Processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
            #     )

            # Process DIDs in parallel using ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=total_threads
            ) as executor:
                futures = [
                    executor.submit(self._process_did, did) for did in latest_batch
                ]
                # Wait for all threads to complete
                concurrent.futures.wait(futures)

            if (
                total_dids_processed > 0
                and total_dids_processed % default_write_batch_size == 0
            ):
                logger.info(
                    f"Total DIDs processed so far: {total_dids_processed}. Flushing to DB."
                )
                self.flush_to_db()
                logger.info(
                    f"Total DIDs processed so far: {total_dids_processed}. Flushed to DB."
                )

            if current_idx % log_batch_interval == 0:
                logger.info(f"Completed processing {current_idx}/{total_dids} DIDs...")
            # if current_idx % total_batches == 0:
            #     logger.info(
            #         f"Completed processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
            #     )
            current_idx += 1

        logger.info("Finished processing all DIDs.")


class PDSEndpointWorkerAsynchronous:
    """Async implementation of PDSEndpointWorker."""

    def __init__(self, pds_endpoint: str, dids: list[str]):
        self.pds_endpoint = pds_endpoint
        self.dids = dids
        self.token_bucket = AsyncTokenBucket(
            max_tokens=MANUAL_RATE_LIMIT, window_seconds=300
        )
        self.batch_size = 20
        self.did_batches = create_batches(
            batch_list=self.dids, batch_size=self.batch_size
        )

        self.concurrency = 10

        # for testing purposes.
        # self.max_batches = 5
        # self.max_threads = 8

        # this leads to ~200, but stalls after ~110 DIDs.
        self.max_threads = 5
        self.max_batches = 2 * self.max_threads

        # ~240. Let's see if it stalls.
        # self.max_threads = 3
        # self.max_batches = 4 * self.max_threads

        self.did_batches = self.did_batches[: self.max_batches]

        # TODO: copy the DIDs.
        # breakpoint()

        logger.info(f"Total batches: {len(self.did_batches)}")

        # queues:
        self.temp_results_queue = queue.Queue()
        self.temp_deadletter_queue = queue.Queue()
        self.temp_work_queue = queue.Queue()

        # put batches into queue, so each item in the queue is a batch of DIDs.
        for batch in self.did_batches:
            self.temp_work_queue.put(batch)

        # NOTE: split each batch across multiple threads?
        self.in_flight = 0
        self.max_in_flight = 0
        self.request_times = []
        self.completed_requests = 0

    async def _process_did(
        self,
        did: str,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ) -> requests.Response:
        # pdm.BACKFILL_REQUESTS.labels(endpoint=self.pds_endpoint).inc()
        # pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).inc()
        did = "did:plc:4rlh46czb2ix4azam3cfyzys"
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        logger.info(f"Starting request for {did}. In-flight: {self.in_flight}")

        await self.token_bucket._acquire()

        # make the request and time.
        request_start = time.perf_counter()
        async with semaphore:
            response = await async_send_request_to_pds(
                did=did, pds_endpoint=self.pds_endpoint, session=session
            )
            _ = await response.read()
            self.completed_requests += 1
        request_latency = time.perf_counter() - request_start
        logger.info(f"Request latency: {request_latency} seconds")
        # pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
        #     request_latency
        # )
        self.request_times.append(request_latency)
        headers = response.headers
        rate_limit_remaining = headers.get("ratelimit-remaining")
        logger.info(f"Rate limit remaining: {rate_limit_remaining}")
        return response

    async def run(self):
        times = []
        conn = aiohttp.TCPConnector(
            limit=self.max_threads, limit_per_host=self.max_threads
        )
        # timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_connect=5, sock_read=15)
        # async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        start_time = time.perf_counter()
        async with aiohttp.ClientSession(connector=conn) as session:
            idx = 0
            while idx < len(self.did_batches):
                # while not self.temp_work_queue.empty():
                semaphore = asyncio.Semaphore(self.concurrency)
                time_start = time.perf_counter()
                # Create tasks for each DID in the batch to process them concurrently
                tasks = []
                # latest_batch = self.temp_work_queue.get()
                latest_batch = self.did_batches[idx]
                idx += 1

                async def limited_process(did):
                    async with semaphore:
                        return await self._process_did(
                            did=did, session=session, semaphore=semaphore
                        )

                tasks = [limited_process(did) for did in latest_batch]

                # for did in latest_batch:
                #     task = asyncio.create_task(limited_process(did))
                #     tasks.append(task)

                # Wait for all tasks to complete
                responses = await asyncio.gather(*tasks)
                logger.info(
                    f"Total results: {len(responses)} ({len(latest_batch)} DIDs across {len(responses)} tasks)"
                )
                time_end = time.perf_counter()
                diff = time_end - time_start
                times.append(diff)
                # to simulate DB flush.
                # logger.info(f"Sleeping for 30 seconds to simulate DB flush...")
                # await asyncio.sleep(30)
                # logger.info(f"Done sleeping. Continuing...")
                # logger.info(f"Sleeping for 15 seconds to simulate DB flush...")
                # await asyncio.sleep(15)
                # logger.info(f"Done sleeping. Continuing...")

            # for batch in self.did_batches:
            #     start = time.perf_counter()
            #     res: list[requests.Response] = await self._process_batch(batch=batch, session=session)
            #     logger.info(f"Total results: {len(res)}")
            #     times.append(time.perf_counter() - start)
        elapsed = time.perf_counter() - start_time
        final_qps = self.completed_requests / elapsed
        logger.info("=== TEST RESULTS ===")
        logger.info(f"Total requests: {self.completed_requests}")
        logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        logger.info(f"Average QPS: {final_qps:.2f}")
        logger.info(
            f"Average time to send/receive requests: {np.mean(self.request_times):.3f}s"
        )
        logger.info(f"Max in-flight requests: {self.max_in_flight}")
        logger.info(f"Average time to process each batch: {sum(times)/len(times):.3f}s")
        # logger.info(f"Average time per threaded batch (so {self.max_threads} threads, each with 1 batch): {sum(times)/len(times):.3f}s")

        # TODO: add flush db later.


async def dummy_aiosqlite_write():
    """Testing aiosqlite writes."""
    queue_fp = os.path.join(current_dir, "test_queue.db")
    queue = Queue(
        queue_name="test_queue",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=queue_fp,
    )
    items = [{"test": "test"} for _ in range(100)]
    await queue.async_batch_add_items_to_queue(items=items)
    total_items_in_queue = queue.get_queue_length()
    logger.info(f"Total items in queue: {total_items_in_queue}")


async def diagnose_qps_issue():
    """Test to diagnose slow QPS problem. We're only seeing max 1 QPS per
    endpoint, so trying to understand why that could be the case."""
    # Test 1: Measure token bucket replenishment rate
    token_bucket = AsyncTokenBucket(max_tokens=MANUAL_RATE_LIMIT, window_seconds=300)
    start_time = time.perf_counter()
    acquired_tokens = 0
    for _ in range(100):
        await token_bucket._acquire()
        acquired_tokens += 1
        if acquired_tokens % 10 == 0:
            elapsed = time.perf_counter() - start_time
            logger.info(
                f"Acquired {acquired_tokens} tokens in {elapsed:.2f}s = {acquired_tokens/elapsed:.2f} tokens/sec"
            )

    # Test 2: Measure HTTP request time without processing
    async with aiohttp.ClientSession() as session:
        pds_endpoint = "https://puffball.us-east.host.bsky.network"
        did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
        times = []
        for _ in range(10):
            start = time.perf_counter()
            await async_send_request_to_pds(
                did=did,
                pds_endpoint=pds_endpoint,
                session=session,
            )
            times.append(time.perf_counter() - start)
        logger.info(f"Average HTTP request time: {sum(times)/len(times):.3f}s")


async def run_backfill_for_dids(pds_endpoint: str, dids: list[str]):
    # TEST_DIDS = ["did:plc:4rlh46czb2ix4azam3cfyzys"] * 50
    # TEST_PDS_ENDPOINT = "https://morel.us-east.host.bsky.network"
    TEST_DIDS = dids
    TEST_PDS_ENDPOINT = pds_endpoint
    logger.info(f"Running backfill for {len(TEST_DIDS)} DIDs on {TEST_PDS_ENDPOINT}...")

    CONCURRENCY = 10
    TOTAL_REQUESTS = 50

    # Stats tracking
    start_time = time.time()
    completed_requests = 0
    in_flight = 0
    max_in_flight = 0
    request_times = []

    # Create token bucket similar to your worker
    token_bucket = AsyncTokenBucket(max_tokens=3000 * 0.9, window_seconds=300)

    # Track in-flight requests
    in_flight_gauge = asyncio.Semaphore(CONCURRENCY)

    async def process_did(did, session):
        nonlocal completed_requests, in_flight, max_in_flight

        try:
            # Track in-flight requests
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            logger.info(f"Starting request for {did}. In-flight: {in_flight}")

            # Acquire token and time
            token_start = time.perf_counter()
            await token_bucket._acquire()
            token_time = time.perf_counter() - token_start

            # Make the request and time
            request_start = time.perf_counter()
            async with in_flight_gauge:
                response = await async_send_request_to_pds(
                    did=did, pds_endpoint=TEST_PDS_ENDPOINT, session=session
                )
                _ = await response.read()  # Read the content
            request_time = time.perf_counter() - request_start

            # Record stats
            request_times.append(request_time)
            completed_requests += 1

            # Log detailed timing
            total_time = time.perf_counter() - token_start
            logger.info(
                f"Completed {did}: Token wait={token_time:.3f}s, Request={request_time:.3f}s, Total={total_time:.3f}s"
            )

            # Calculate and log current QPS
            elapsed = time.time() - start_time
            current_qps = completed_requests / elapsed
            logger.info(
                f"Current stats: {completed_requests}/{TOTAL_REQUESTS} completed, QPS={current_qps:.2f}"
            )

        finally:
            in_flight -= 1

    # Create a session pool
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY)
    # timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_connect=5, sock_read=15)
    # async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
    async with aiohttp.ClientSession(connector=conn) as session:
        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def limited_process(did):
            async with semaphore:  # Only allow CONCURRENCY concurrent requests
                await process_did(did, session)

        # Process DIDs with enforced concurrency limit
        tasks = [limited_process(did) for did in TEST_DIDS[:TOTAL_REQUESTS]]
        await asyncio.gather(*tasks)

    # Log final stats
    elapsed = time.time() - start_time
    final_qps = completed_requests / elapsed
    avg_request_time = sum(request_times) / len(request_times) if request_times else 0

    logger.info("=== TEST RESULTS ===")
    logger.info(f"Total requests: {completed_requests}")
    logger.info(f"Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"Average QPS: {final_qps:.2f}")
    logger.info(f"Average request time: {avg_request_time:.3f} seconds")
    logger.info(f"Max in-flight requests: {max_in_flight}")

    # Analyze token bucket behavior
    logger.info(f"Token bucket initial capacity: {token_bucket._max_tokens}")
    logger.info(f"Token bucket final tokens: {token_bucket._tokens}")


async def multithreaded_run_backfill_for_dids(pds_endpoint: str, dids: list[str]):
    """Run backfill for DIDs in parallel using multiple threads.

    This function processes DIDs in parallel using a combination of asyncio for I/O operations
    and a thread pool for CPU-bound tasks. It uses 32 threads with a semaphore limit of 50
    to control concurrency.

    Args:
        pds_endpoint: The PDS endpoint URL to send requests to
        dids: List of DIDs to process
    """
    TEST_DIDS = dids
    TEST_PDS_ENDPOINT = pds_endpoint
    logger.info(
        f"Running multithreaded backfill for {len(TEST_DIDS)} DIDs on {TEST_PDS_ENDPOINT}..."
    )

    # max_dids = 100
    max_dids = 500
    TEST_DIDS = TEST_DIDS[:max_dids]

    # Configuration
    THREAD_COUNT = 32
    CONCURRENCY = 50
    TOTAL_REQUESTS = len(TEST_DIDS)

    # Stats tracking
    start_time = time.time()
    completed_requests = 0
    in_flight = 0
    max_in_flight = 0
    request_times = []

    # Create token bucket for rate limiting
    token_bucket = AsyncTokenBucket(max_tokens=3000 * 0.9, window_seconds=300)

    # Create thread pool
    thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_COUNT)

    async def process_did(did, session):
        nonlocal completed_requests, in_flight, max_in_flight

        try:
            # Track in-flight requests
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            logger.info(f"Starting request for {did}. In-flight: {in_flight}")

            # Acquire token and time
            token_start = time.perf_counter()
            await token_bucket._acquire()

            # Make the request and time
            request_start = time.perf_counter()

            # Use thread pool for CPU-bound operations
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                thread_pool,
                lambda: asyncio.run_coroutine_threadsafe(
                    async_send_request_to_pds(
                        did=did, pds_endpoint=TEST_PDS_ENDPOINT, session=session
                    ),
                    loop,
                ).result(),
            )
            rate_limit_remaining = response.headers.get("ratelimit-remaining")
            # logger.info(f"Rate limit remaining: {rate_limit_remaining}")
            _ = await response.read()  # Read the content
            request_time = time.perf_counter() - request_start

            # Record stats
            request_times.append(request_time)
            completed_requests += 1

            # Log detailed timing
            total_time = time.perf_counter() - token_start
            # Calculate and log current QPS
            elapsed = time.time() - start_time
            current_qps = completed_requests / elapsed
            logger.info(
                f"Completed {did}: Rate limit remaining={rate_limit_remaining}, Request={request_time:.3f}s, Total={total_time:.3f}s\tCurrent stats: {completed_requests}/{TOTAL_REQUESTS} completed, QPS={current_qps:.2f}"
            )

        finally:
            in_flight -= 1

    # Create a session pool
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY)

    async with aiohttp.ClientSession(connector=conn) as session:
        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def limited_process(did):
            async with semaphore:  # Only allow CONCURRENCY concurrent requests
                await process_did(did, session)

        # Process DIDs with enforced concurrency limit
        tasks = [limited_process(did) for did in TEST_DIDS]
        await asyncio.gather(*tasks)

    # Shutdown thread pool
    thread_pool.shutdown(wait=True)

    # Log final stats
    elapsed = time.time() - start_time
    final_qps = completed_requests / elapsed
    avg_request_time = sum(request_times) / len(request_times) if request_times else 0

    logger.info("=== MULTITHREADED TEST RESULTS ===")
    logger.info(f"Total requests: {completed_requests}")
    logger.info(f"Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"Average QPS: {final_qps:.2f}")
    logger.info(f"Average request time: {avg_request_time:.3f} seconds")
    logger.info(f"Max in-flight requests: {max_in_flight}")
    logger.info(f"Thread count: {THREAD_COUNT}")
    logger.info(f"Concurrency limit: {CONCURRENCY}")

    # Analyze token bucket behavior
    logger.info(f"Token bucket initial capacity: {token_bucket._max_tokens}")
    logger.info(f"Token bucket final tokens: {token_bucket._tokens}")


async def parallel_request_test():
    """Test HTTP request parallelism to diagnose QPS issues.

    I am seeing only 1 QPS per endpoint, so trying to understand why that
    could be the case. Since there are 10 tasks per endpoint, I expect 10
    QPS.
    """

    # TEST_DIDS = ["did:plc:w5mjarupsl6ihdrzwgnzdh4y"] * 50  # 50 DIDs to process
    # TEST_PDS_ENDPOINT = "https://puffball.us-east.host.bsky.network"

    # this combination works slowly during my sync...
    TEST_DIDS = ["did:plc:4rlh46czb2ix4azam3cfyzys"] * 50
    TEST_PDS_ENDPOINT = "https://morel.us-east.host.bsky.network"

    CONCURRENCY = 10
    TOTAL_REQUESTS = 50

    # Stats tracking
    start_time = time.time()
    completed_requests = 0
    in_flight = 0
    max_in_flight = 0
    request_times = []

    # Create token bucket similar to your worker
    token_bucket = AsyncTokenBucket(max_tokens=3000 * 0.9, window_seconds=300)

    # Track in-flight requests
    in_flight_gauge = asyncio.Semaphore(CONCURRENCY)

    async def process_did(did, session):
        nonlocal completed_requests, in_flight, max_in_flight

        try:
            # Track in-flight requests
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            logger.info(f"Starting request for {did}. In-flight: {in_flight}")

            # Acquire token and time
            token_start = time.perf_counter()
            await token_bucket._acquire()
            token_time = time.perf_counter() - token_start

            # Make the request and time
            request_start = time.perf_counter()
            async with in_flight_gauge:
                response = await async_send_request_to_pds(
                    did=did, pds_endpoint=TEST_PDS_ENDPOINT, session=session
                )
                _ = await response.read()  # Read the content
            request_time = time.perf_counter() - request_start

            # Record stats
            request_times.append(request_time)
            completed_requests += 1

            # Log detailed timing
            total_time = time.perf_counter() - token_start
            logger.info(
                f"Completed {did}: Token wait={token_time:.3f}s, Request={request_time:.3f}s, Total={total_time:.3f}s"
            )

            # Calculate and log current QPS
            elapsed = time.time() - start_time
            current_qps = completed_requests / elapsed
            logger.info(
                f"Current stats: {completed_requests}/{TOTAL_REQUESTS} completed, QPS={current_qps:.2f}"
            )

        finally:
            in_flight -= 1

    # Create a session pool
    conn = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_connect=5, sock_read=15)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def limited_process(did):
            async with semaphore:  # Only allow CONCURRENCY concurrent requests
                await process_did(did, session)

        # Process DIDs with enforced concurrency limit
        tasks = [limited_process(did) for did in TEST_DIDS[:TOTAL_REQUESTS]]
        await asyncio.gather(*tasks)

    # Log final stats
    elapsed = time.time() - start_time
    final_qps = completed_requests / elapsed
    avg_request_time = sum(request_times) / len(request_times) if request_times else 0

    logger.info("=== TEST RESULTS ===")
    logger.info(f"Total requests: {completed_requests}")
    logger.info(f"Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"Average QPS: {final_qps:.2f}")
    logger.info(f"Average request time: {avg_request_time:.3f} seconds")
    logger.info(f"Max in-flight requests: {max_in_flight}")

    # Analyze token bucket behavior
    logger.info(f"Token bucket initial capacity: {token_bucket._max_tokens}")
    logger.info(f"Token bucket final tokens: {token_bucket._tokens}")


def _test():
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(dummy_aiosqlite_write())
    loop.run_until_complete(diagnose_qps_issue())
    loop.run_until_complete(parallel_request_test())


if __name__ == "__main__":
    _test()
