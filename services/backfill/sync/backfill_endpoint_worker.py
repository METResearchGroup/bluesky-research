"""Worker class for managing the backfill for a single PDS endpoint."""

from typing import Optional
import aiohttp
import asyncio
import concurrent.futures
import contextlib
from datetime import datetime, timezone
import json
import os
import time
import collections


from lib.constants import timestamp_format
from lib.db.queue import Queue
from lib.db.rate_limiter import AsyncTokenBucket
from lib.log.logger import get_logger
from lib.telemetry.prometheus.service import pds_backfill_metrics as pdm
from services.backfill.sync.atp_agent import AtpAgent
from services.backfill.sync.backfill import (
    get_records_from_pds_bytes,
    async_send_request_to_pds,
    filter_only_valid_bsky_posts,
)
from services.backfill.sync.constants import current_dir


default_qps = 10  # assume 10 QPS max = 600/minute = 3000/5 minutes
# default_qps = 2  # 2 QPS max = 120/minute = 600/5 minutes
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
default_pds_endpoint = "https://bsky.social"


def get_previously_processed_dids(
    results_db: Queue,
    deadletter_db: Queue,
) -> set[str]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    results_metadata = results_db.load_dict_items_metadata_from_queue()
    deadletter_metadata = deadletter_db.load_dict_items_metadata_from_queue()
    query_result_dids: set[str] = set()
    query_deadletter_dids: set[str] = set()
    total_results_metadata = len(results_metadata)
    total_deadletter_metadata = len(deadletter_metadata)

    if len(results_metadata) > 0 or len(deadletter_metadata) > 0:
        logger.info(
            f"Loaded {total_results_metadata} results metadata and {total_deadletter_metadata} deadletter metadata."
        )

    for metadata in results_metadata:
        did_json = json.loads(metadata)["dids"]
        query_result_dids.update(did_json)

    for metadata in deadletter_metadata:
        did_json = json.loads(metadata)["dids"]
        query_deadletter_dids.update(did_json)

    previously_processed_dids = query_result_dids | query_deadletter_dids
    return previously_processed_dids


def filter_previously_processed_dids(
    pds_endpoint: str,
    dids: list[str],
    results_db: Queue,
    deadletter_db: Queue,
) -> list[str]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    previously_processed_dids = get_previously_processed_dids(
        results_db=results_db,
        deadletter_db=deadletter_db,
    )

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
    total_queued_dids: int,
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
    logger.info(
        f"\tTotal DIDS remaining in queue: {total_queued_dids}/{total_filtered_dids}"
    )
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
    """Wrapper class for managing the backfills for a single PDS endpoint.

    By default, uses 'https://bsky.social' as the PDS endpoint. Still unsure
    why, but pinging the individual PDS endpoints is much slower than just
    requesting 'https://bsky.social', which itself routes to the PDS endpoints
    anyways (https://docs.bsky.app/docs/advanced-guides/entryway).

    This is unlike what is described in the docs for how to backfill the
    network: https://docs.bsky.app/docs/advanced-guides/backfill
    """

    def __init__(
        self,
        dids: list[str],
        session: aiohttp.ClientSession,
        cpu_pool,
        pds_endpoint: Optional[str] = default_pds_endpoint,
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

        self.original_total_dids = len(dids)
        self.dids: list[str] = filter_previously_processed_dids(
            pds_endpoint=pds_endpoint,
            dids=dids,
            results_db=self.output_results_queue,
            deadletter_db=self.output_deadletter_queue,
        )
        self.total_dids_post_filter = len(self.dids)
        self.dids_filtered_out = self.original_total_dids - self.total_dids_post_filter

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
            await self.temp_work_queue.put({"did": did})
        # Update queue size metric
        queue_size = self.temp_work_queue.qsize()
        pdm.BACKFILL_QUEUE_SIZES.labels(
            endpoint=self.pds_endpoint, queue_type="work"
        ).set(queue_size)
        pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=self.pds_endpoint).set(queue_size)

    async def _process_did(
        self,
        did: str,
        session: aiohttp.ClientSession,
        retry_count: int,
        max_retries: int = None,
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

        if retry_count > max_retries:
            logger.info(
                f"Failed to process DID {did} after {max_retries} retries. Adding to deadletter queue."
            )
            await self.temp_deadletter_queue.put({"did": did, "content": ""})
            pdm.BACKFILL_QUEUE_SIZES.labels(
                endpoint=self.pds_endpoint, queue_type="deadletter"
            ).set(self.temp_deadletter_queue.qsize())
            return
        else:
            self.total_requests += 1
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
                    atp_agent = AtpAgent(service=self.pds_endpoint)
                    response = await atp_agent.async_get_repo(did=did, session=session)
                    pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).dec()
                    # Track both the summary and histogram for latency
                    request_latency = time.perf_counter() - start
                    pdm.BACKFILL_LATENCY_SECONDS.labels(
                        endpoint=self.pds_endpoint
                    ).observe(request_latency)
                    pdm.BACKFILL_LATENCY_HISTOGRAM.labels(
                        endpoint=self.pds_endpoint
                    ).observe(request_latency)
                    tokens_remaining: int = int(
                        response.headers.get("ratelimit-remaining")
                    )
                    if tokens_remaining < self.token_bucket.get_tokens():
                        token_buffer_size = 50  # have some conservative bucket buffer.
                        logger.info(
                            f"(PDS endpoint: {self.pds_endpoint}): Tokens remaining from headers {tokens_remaining} < bucket tokens {self.token_bucket.get_tokens()}, setting bucket tokens to {tokens_remaining}."
                        )
                        self.token_bucket.set_tokens(
                            tokens_remaining - token_buffer_size
                        )
                    if response.status == 200:
                        content: bytes = await response.read()
                    elif response.status == 429:
                        # Rate limited, put back in queue
                        logger.info(
                            f"(PDS endpoint: {self.pds_endpoint}): Rate limited, putting DID {did} back in queue."
                        )
                        await asyncio.sleep(1 * (2**retry_count))  # Exponential backoff
                        retry_count += 1
                        await self.temp_work_queue.put(
                            {"did": did, "retry_count": retry_count}
                        )
                        pdm.BACKFILL_QUEUE_SIZES.labels(
                            endpoint=self.pds_endpoint, queue_type="work"
                        ).set(self.temp_work_queue.qsize())
                        logger.info(
                            f"Sleep time finished for {did}, continuing backfill."
                        )
                        return
                    elif response.status == 400 and response.reason == "Bad Request":
                        # API returning 400 means the account is deleted/suspended.
                        # The PLC doc might exist, but if you look at Bluesky, their
                        # account won't exist.
                        logger.error(
                            f"(PDS endpoint: {self.pds_endpoint}): Account {did} is deleted/suspended. Adding to deadletter queue."
                        )
                        await self.temp_deadletter_queue.put(
                            {"did": did, "content": ""}
                        )
                        pdm.BACKFILL_DID_STATUS.labels(
                            endpoint=self.pds_endpoint, status="account_deleted"
                        ).inc()
                        pdm.BACKFILL_QUEUE_SIZES.labels(
                            endpoint=self.pds_endpoint, queue_type="deadletter"
                        ).set(self.temp_deadletter_queue.qsize())
                        return
                    else:
                        # For unexpected status codes, we don't retry.
                        logger.error(
                            f"(PDS endpoint: {self.pds_endpoint}): Unexpected status code ({response.status}) and reason ({response.reason}) for DID {did}"
                        )
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
                            if filter_only_valid_bsky_posts(record)
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
                # Track network errors specifically. For network errors,
                # we want to retry.
                error_type = (
                    "timeout" if isinstance(e, asyncio.TimeoutError) else "connection"
                )
                pdm.BACKFILL_NETWORK_ERRORS.labels(
                    endpoint=self.pds_endpoint, error_type=error_type
                ).inc()
                logger.warning(
                    f"(PDS endpoint: {self.pds_endpoint}): Connection error for DID {did}, retry {retry_count}: {e}"
                )
                await asyncio.sleep(1 * (2**retry_count))  # Exponential backoff

                retry_count += 1
                # Increment retry counter
                pdm.BACKFILL_RETRIES.labels(endpoint=self.pds_endpoint).inc()
                await self.temp_work_queue.put({"did": did, "retry_count": retry_count})
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Putting DID {did} back in queue. Current retry count: {retry_count}/{max_retries}."
                )

            except Exception as e:
                # for unexpected errors, we don't re-try them. These shouldn't
                # happen, and if we do, we want to monitor these and NOT retry.
                pdm.BACKFILL_ERRORS.labels(endpoint=self.pds_endpoint).inc()
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
                return

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
                did_item = await asyncio.wait_for(
                    self.temp_work_queue.get(), timeout=1.0
                )
                did = did_item["did"]
                # should only have a retry_count if it's been re-inserted into the queue.
                retry_count = did_item.get("retry_count", 0)
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
                await self._process_did(
                    did=did,
                    session=self.session,
                    retry_count=retry_count,
                )
            finally:
                # mark task as done even if it failed.
                self.temp_work_queue.task_done()

    async def wait_done(self):
        """Waits for the backfill to finish."""
        # wait for the work queue to be empty
        await self.temp_work_queue.join()

        # wait for all producers to finish.
        await self._producer_group

        # Signal to the writer task to flush any remaining items
        # by adding a special flag item to the results queue
        await self.temp_results_queue.put({"final_flush": True})

        # wait until writer consumes everything in queues.
        await self.temp_results_queue.join()
        await self.temp_deadletter_queue.join()
        await asyncio.sleep(2.0)  # give pending DB operations time to complete.

    async def shutdown(self):
        """Gracefully shuts down the backfill for a single PDS endpoint."""
        if self._writer_task:
            self._writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._writer_task
        await self.output_results_queue.close()
        await self.output_deadletter_queue.close()

    async def _writer(self, batch_size: Optional[int] = None) -> None:
        """Background task to flush temp queues to permanent storage in batches."""
        result_buffer: list[dict] = []
        deadletter_buffer: list[dict] = []
        global_total_records_flushed = 0
        if not batch_size:
            batch_size = self._batch_size
        try:
            while True:
                final_flush_signal = False

                # Process results queue
                while not self.temp_results_queue.empty():
                    result: dict = await self.temp_results_queue.get()

                    # Check for the special flag item
                    if result.get("final_flush"):
                        final_flush_signal = True
                        self.temp_results_queue.task_done()
                        continue

                    # Normal processing
                    result_buffer.append(result)
                    self.temp_results_queue.task_done()
                    # Update queue size metric
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="results"
                    ).set(self.temp_results_queue.qsize())

                # Process deadletter queue
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
                total_buffer_size = (
                    total_result_buffer_size + total_deadletter_buffer_size
                )

                # If we received final flush signal, flush remaining buffers regardless of size
                if final_flush_signal:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Received final flush signal. Flushing remaining buffers: {total_buffer_size} records ({total_result_buffer_size} results and {total_deadletter_buffer_size} deadletters)."
                    )
                    if result_buffer or deadletter_buffer:
                        await self._async_flush_buffers(
                            result_buffer=result_buffer,
                            deadletter_buffer=deadletter_buffer,
                        )
                        logger.info(
                            f"(PDS endpoint: {self.pds_endpoint}): Final flush completed successfully."
                        )
                    return  # Exit the writer task

                # Regular batch flush check
                if total_buffer_size >= batch_size:
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
                        total_dids=self.original_total_dids,
                        total_filtered_dids=self.total_dids_post_filter,
                        total_queued_dids=self.temp_work_queue.qsize(),
                        total_records_flushed=global_total_records_flushed,
                        total_requests_made=self.total_requests,
                    )

                # Avoid tight loop if both queues are empty
                if (
                    self.temp_results_queue.empty()
                    and self.temp_deadletter_queue.empty()
                ):
                    await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in writer: {e}", exc_info=True)
            raise
        finally:
            logger.info(f"Writer task for {self.pds_endpoint} finished.")

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
                items=result_buffer,
                metadata={
                    "timestamp": current_timestamp,
                    "total_records": len(result_buffer),
                    "dids": [item["did"] for item in result_buffer],
                },
            )
        except Exception as e:
            logger.error(
                f"SQLite operation failed for results flush: {e}", exc_info=True
            )
            raise
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Finished flushing results queue."
        )
        total_deadletter_buffer = len(deadletter_buffer)
        if total_deadletter_buffer > 0:
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Flushing {total_deadletter_buffer} deadletters to permanent storage."
            )
            try:
                await self.output_deadletter_queue.async_batch_add_items_to_queue(
                    items=deadletter_buffer,
                    metadata={
                        "timestamp": current_timestamp,
                        "total_records": len(deadletter_buffer),
                        "dids": [item["did"] for item in deadletter_buffer],
                    },
                )
            except Exception as e:
                logger.error(
                    f"SQLite operation failed for deadletter flush: {e}", exc_info=True
                )
                raise
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Finished flushing deadletter queue."
            )
        else:
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): No deadletters to flush."
            )

        try:
            flush_time = time.perf_counter() - flush_start
            pdm.BACKFILL_PROCESSING_SECONDS.labels(
                endpoint=self.pds_endpoint, operation_type="db_flush"
            ).observe(flush_time)

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Finished flushing {total_buffer} results: {total_results_buffer} records and {total_deadletter_buffer} deadletters to permanent storage in {flush_time:.2f}s."
            )
        except Exception as e:
            logger.error(f"Error in _async_flush_buffers: {e}", exc_info=True)
            raise


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
