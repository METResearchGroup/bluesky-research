"""Worker class for managing the backfill for a single PDS endpoint."""

from typing import Optional

import pandas as pd
import aiohttp
import asyncio
import concurrent.futures
import contextlib
from datetime import datetime, timezone
import json
import os
import tempfile
import time
import collections


from lib.constants import timestamp_format
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.queue import Queue
from lib.db.rate_limiter import AsyncTokenBucket
from lib.helper import generate_current_datetime_str, create_batches
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.prometheus.service import pds_backfill_metrics as pdm
from services.backfill.sync.atp_agent import AtpAgent
from services.backfill.sync.constants import service_name
from services.backfill.sync.backfill import (
    get_records_from_pds_bytes,
    async_send_request_to_pds,
    filter_only_valid_bsky_posts,
    validate_time_range_record,
    transform_backfilled_record,
    identify_record_type,
    postprocess_backfilled_record,
)
from services.backfill.sync.constants import current_dir
from services.backfill.sync.dynamodb_utils import get_dids_by_pds_endpoint
from services.backfill.sync.models import UserBackfillMetadata
from services.backfill.sync.session_metadata import write_backfill_metadata_to_db


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


def get_previously_processed_dids(pds_endpoint: str) -> set[str]:
    """Load previous results from DynamoDB and filter out DIDs that have already been processed."""
    previously_processed_dids = get_dids_by_pds_endpoint(pds_endpoint)
    return previously_processed_dids


def filter_previously_processed_dids(pds_endpoint: str, dids: list[str]) -> list[str]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    previously_processed_dids = get_previously_processed_dids(pds_endpoint)

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
            pds_endpoint=pds_endpoint, dids=dids
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

    @staticmethod
    def _filter_records(records: list[dict]) -> list[dict]:
        """Filter for only posts/reposts in the date range of interest."""
        return [
            record
            for record in records
            if filter_only_valid_bsky_posts(record)
            and validate_time_range_record(record)
        ]

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
                    raw_records: list[dict] = await loop.run_in_executor(
                        self.cpu_pool, get_records_from_pds_bytes, content
                    )
                    records: list[dict] = await loop.run_in_executor(
                        self.cpu_pool,
                        PDSEndpointWorker._filter_records,
                        raw_records,
                    )
                    processing_time = time.perf_counter() - processing_start
                    pdm.BACKFILL_PROCESSING_SECONDS.labels(
                        endpoint=self.pds_endpoint, operation_type="parse_records"
                    ).observe(processing_time)

                    # NOTE: possible to have 0 records (e.g., if the records
                    # are filtered out as per our criteria above).
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

        logger.info(
            f"(PDS endpoint {self.pds_endpoint}): Completed syncs and SQLite writes. Now persisting to DB and exporting metadata."
        )
        user_to_total_per_record_type_map = await self.persist_to_db()
        self.write_backfill_metadata_to_db(
            user_to_total_per_record_type_map=user_to_total_per_record_type_map
        )
        logger.info(
            f"(PDS endpoint {self.pds_endpoint}): Completed persisting to DB and exporting metadata."
        )
        logger.info(f"(PDS endpoint {self.pds_endpoint}): Deleting queues...")
        self.output_results_queue.delete_queue()
        self.output_deadletter_queue.delete_queue()
        logger.info(f"(PDS endpoint {self.pds_endpoint}): Queues deleted.")

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
                    # Format: {'did': '<DID>', 'content': [<List of records>]}.
                    # Unless it's final flush signal, in which case the
                    # format is {'final_flush': True}.
                    record: dict = await self.temp_results_queue.get()

                    # Check for the special flag item
                    if record.get("final_flush"):
                        final_flush_signal = True
                        self.temp_results_queue.task_done()
                        continue
                    result_buffer.append(record)
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

    def _estimate_parquet_write_size(
        self,
        record_type: str,
        sample_df: pd.DataFrame,
        sample_size: int,
        total_rows: int,
    ) -> float:
        """Writes a sample of the items to a temp file and returns the estimated size, in MB."""
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            sample_df.to_parquet(tmp.name)
            parquet_sample_size = os.path.getsize(tmp.name)
            parquet_sample_size_mb = parquet_sample_size / (1024 * 1024)
            parquet_total_size_mb = parquet_sample_size_mb * (total_rows / sample_size)
            logger.info(f"""
                Estimated {record_type} parquet size: {parquet_total_size_mb:.2f} MB
                (based on {sample_size} sample of {total_rows} rows, where the sample was {parquet_sample_size_mb:.2f} MB)
            """)
            return parquet_total_size_mb

    async def persist_to_db(self) -> dict:
        """Writes records to permanent .parquet storage.

        Returns counts of users per record type, in the form:
        {
            "<did>": {
                "post": 10,
                "repost": 5,
                "reply": 2,
            }
        }
        """

        logger.info("Persisting SQLite queue records to permanent storage...")

        items: list[dict] = self.output_results_queue.load_dict_items_from_queue()
        total_items: int = len(items)
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Persisting {total_items} items to DB..."
        )

        def process_batch(
            batch: list[dict],
            idx: int,
        ) -> tuple[list[dict], list[dict], list[dict], dict]:
            """Process a batch of items and return categorized records and user counts."""
            batch_posts = []
            batch_replies = []
            batch_reposts = []
            batch_user_counts = {}

            for item in batch:
                records = item["content"]
                user_did = item["did"]

                # Filter records
                records = PDSEndpointWorker._filter_records(records=records)

                for record in records:
                    record_type: str = record.get(
                        "record_type", identify_record_type(record)
                    )

                    # Transform and postprocess
                    record = transform_backfilled_record(
                        record=record,
                        record_type=record_type,
                        start_timestamp="2024-09-01-00:00:00",
                        end_timestamp="2024-12-02-00:00:00",
                    )
                    record = postprocess_backfilled_record(record)

                    # Initialize user counts if needed
                    if user_did not in batch_user_counts:
                        batch_user_counts[user_did] = {
                            "post": 0,
                            "repost": 0,
                            "reply": 0,
                        }

                    # Categorize by record type
                    if record_type == "post":
                        batch_posts.append(record)
                        batch_user_counts[user_did]["post"] += 1
                    elif record_type == "repost":
                        batch_reposts.append(record)
                        batch_user_counts[user_did]["repost"] += 1
                    elif record_type == "reply":
                        batch_replies.append(record)
                        batch_user_counts[user_did]["reply"] += 1
                    else:
                        raise ValueError(
                            f"Unknown record type for item being written to DB: {record_type}"
                        )

            total_batch_posts = len(batch_posts)
            total_batch_replies = len(batch_replies)
            total_batch_reposts = len(batch_reposts)
            logger.info(
                f"Batch {idx} has {total_batch_posts} posts, {total_batch_replies} replies, and {total_batch_reposts} reposts."
            )

            return (batch_posts, batch_replies, batch_reposts, batch_user_counts)

        try:
            num_threads = (
                4  # no need to have too many, this shouldn't be an intense computation.
            )
            batch_size = len(items) // num_threads
            batches = create_batches(items, batch_size)
            num_batches = len(batches)
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Processing {num_batches} batches of {batch_size} items each (total {total_items} items)..."
            )

            loop = asyncio.get_event_loop()
            futures = []
            for i, batch in enumerate(batches):
                future = loop.run_in_executor(self.cpu_pool, process_batch, batch, i)
                futures.append(future)

            results = await asyncio.gather(*futures)

            # Combine results
            posts, replies, reposts = [], [], []
            user_to_total_per_record_type_map = {}

            for batch_posts, batch_replies, batch_reposts, batch_user_counts in results:
                posts.extend(batch_posts)
                replies.extend(batch_replies)
                reposts.extend(batch_reposts)

                # Merge user counts
                for user_did, counts in batch_user_counts.items():
                    if user_did not in user_to_total_per_record_type_map:
                        user_to_total_per_record_type_map[user_did] = {
                            "post": 0,
                            "repost": 0,
                            "reply": 0,
                        }

                    user_to_total_per_record_type_map[user_did]["post"] += counts[
                        "post"
                    ]
                    user_to_total_per_record_type_map[user_did]["repost"] += counts[
                        "repost"
                    ]
                    user_to_total_per_record_type_map[user_did]["reply"] += counts[
                        "reply"
                    ]

            logger.info(
                f"Parallel processing complete: {len(posts)} posts, {len(reposts)} reposts, {len(replies)} replies"
            )

            # Create DataFrames
            posts_df = pd.DataFrame(posts)
            reposts_df = pd.DataFrame(reposts)
            replies_df = pd.DataFrame(replies)

            total_posts = len(posts_df)
            total_reposts = len(reposts_df)
            total_replies = len(replies_df)

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Persisting {total_posts} posts, {total_reposts} reposts, and {total_replies} replies to permanent storage..."
            )

            # get baseline measures of write size.
            sample_factor = 0.01  # 1% of total rows.

            if total_posts > 0:
                posts_sample_size = int(total_posts * sample_factor)
                if posts_sample_size == 0:
                    posts_sample_size = 1
                posts_sample_df = posts_df.sample(n=posts_sample_size)
                posts_parquet_size = self._estimate_parquet_write_size(
                    record_type="post",
                    sample_df=posts_sample_df,
                    sample_size=posts_sample_size,
                    total_rows=total_posts,
                )
            else:
                posts_parquet_size = 0

            if total_reposts > 0:
                reposts_sample_size = int(total_reposts * sample_factor)
                if reposts_sample_size == 0:
                    reposts_sample_size = 1
                reposts_sample_df = reposts_df.sample(n=reposts_sample_size)
                reposts_parquet_size = self._estimate_parquet_write_size(
                    record_type="repost",
                    sample_df=reposts_sample_df,
                    sample_size=reposts_sample_size,
                    total_rows=total_reposts,
                )
            else:
                reposts_parquet_size = 0

            if total_replies > 0:
                replies_sample_size = int(total_replies * sample_factor)
                if replies_sample_size == 0:
                    replies_sample_size = 1
                replies_sample_df = replies_df.sample(n=replies_sample_size)
                replies_parquet_size = self._estimate_parquet_write_size(
                    record_type="reply",
                    sample_df=replies_sample_df,
                    sample_size=replies_sample_size,
                    total_rows=total_replies,
                )
            else:
                replies_parquet_size = 0

            logger.info(f"""
                Estimated parquet sizes:
                - posts: {posts_parquet_size:.2f} MB
                - reposts: {reposts_parquet_size:.2f} MB
                - replies: {replies_parquet_size:.2f} MB
            """)

            service = "raw_sync"

            if total_posts > 0:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Exporting posts to local storage..."
                )
                export_data_to_local_storage(
                    service=service,
                    df=posts_df,
                    custom_args={"record_type": "post"},
                )
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Exported posts to local storage."
                )
            else:
                logger.info(f"(PDS endpoint: {self.pds_endpoint}): No posts to export.")

            if total_reposts > 0:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Exporting reposts to local storage..."
                )
                export_data_to_local_storage(
                    service=service,
                    df=reposts_df,
                    custom_args={"record_type": "repost"},
                )
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Exported reposts to local storage."
                )
            else:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): No reposts to export."
                )

            if total_replies > 0:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Exporting replies to local storage..."
                )
                export_data_to_local_storage(
                    service=service,
                    df=replies_df,
                    custom_args={"record_type": "reply"},
                )
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Exported replies to local storage."
                )
            else:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): No replies to export."
                )

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Finished persisting {total_posts} posts, {total_reposts} reposts, and {total_replies} replies to permanent storage."
            )

            return user_to_total_per_record_type_map

        except Exception as e:
            logger.error(f"Error persisting to DB: {e}", exc_info=True)
            raise

    def write_backfill_metadata_to_db(
        self,
        user_to_total_per_record_type_map: dict[str, dict[str, int]],
    ) -> None:
        """Writes backfill metadata to the database."""
        total_records = 0
        total_users = 0
        for user, total_per_record_type in user_to_total_per_record_type_map.items():
            for count in total_per_record_type.values():
                total_records += count
            total_users += 1

        session_metadata_body = {
            "pds_endpoint": self.pds_endpoint,
            "total_records": total_records,
            "total_users": total_users,
        }
        session_metadata = {
            "service": service_name,
            "timestamp": generate_current_datetime_str(),
            "status_code": 200,
            "body": json.dumps(session_metadata_body),
            "metadata_table_name": f"{service_name}_metadata",
            "metadata": "",
        }
        transformed_session_metadata = RunExecutionMetadata(**session_metadata)
        user_backfill_metadata = []

        logger.info(
            f"Total user records to export as metadata: {len(user_to_total_per_record_type_map)}"
        )
        # load records from deadletter queue and add their counts to the metadata.
        deadletter_items: list[dict] = (
            self.output_deadletter_queue.load_dict_items_from_queue()
        )
        logger.info(
            f"Loaded {len(deadletter_items)} deadletter items from queue for {self.pds_endpoint}..."
        )
        for item in deadletter_items:
            did = item["did"]
            if did not in user_to_total_per_record_type_map:
                user_to_total_per_record_type_map[did] = {
                    "post": 0,
                    "repost": 0,
                    "reply": 0,
                }
        logger.info(
            f"Total user records to export as metadata (after adding deadletter items): {len(user_to_total_per_record_type_map)}"
        )

        timestamp = generate_current_datetime_str()
        for user, total_per_record_type in user_to_total_per_record_type_map.items():
            user_backfill_metadata.append(
                UserBackfillMetadata(
                    did=user,
                    bluesky_handle="",  # TODO: I wish I got this from the PLC doc. This'll be available there. Will need to revisit.
                    types=",".join(total_per_record_type.keys()),
                    total_records=sum(total_per_record_type.values()),
                    total_records_by_type=json.dumps(total_per_record_type),
                    pds_service_endpoint=self.pds_endpoint,
                    timestamp=timestamp,
                )
            )

        logger.info(
            f"Writing {len(user_backfill_metadata)} user backfill metadata to DB..."
        )

        write_backfill_metadata_to_db(
            session_backfill_metadata=transformed_session_metadata,
            user_backfill_metadata=user_backfill_metadata,
        )

        logger.info(
            f"(PDS endpoint {self.pds_endpoint}): Completed writing backfill metadata to DB."
        )

    def delete_queues(self):
        """Deletes the queues for the given PDS endpoint."""
        logger.info(f"(PDS endpoint {self.pds_endpoint}): Deleting queues...")
        self.output_results_queue.delete_queue()
        self.output_deadletter_queue.delete_queue()
        logger.info(f"(PDS endpoint {self.pds_endpoint}): Queues deleted.")


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
