"""Worker class for managing the backfill for a single PDS endpoint."""

import aiohttp
import asyncio
import collections
from concurrent.futures import ThreadPoolExecutor
import contextlib
from datetime import datetime, timezone
import json
import os
import random
import tempfile
import time
import traceback
from typing import Optional

import pandas as pd

from services.backfill.config.schema import BackfillConfigSchema
from lib.constants import timestamp_format
from lib.datetime_utils import convert_bsky_dt_to_pipeline_dt
from lib.db.manage_local_data import (
    export_data_to_local_storage,
)
from lib.db.queue import Queue
from lib.db.rate_limiter import AsyncTokenBucket
from lib.helper import create_batches
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.prometheus.service import pds_backfill_metrics as pdm
from services.backfill.core.atp_agent import AtpAgent
from services.backfill.core.constants import (
    default_pds_endpoint,
    default_qps,
    default_write_batch_size,
    MANUAL_RATE_LIMIT,
    service_name,
)
from services.backfill.core.models import UserBackfillMetadata
from services.backfill.core.transform import (
    postprocess_backfilled_record,
    transform_backfilled_record,
)
from services.backfill.core.validate import (
    identify_record_type,
    filter_only_valid_bsky_records,
    validate_time_range_record,
)
from services.backfill.storage.load_data import (
    get_previously_processed_dids,
)
from services.backfill.storage.utils.main import load_uris_to_filter
from services.backfill.storage.utils.queue_utils import get_write_queues
from services.backfill.storage.session_metadata import write_backfill_metadata_to_db

start_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)

logger = get_logger(__name__)


def filter_previously_processed_dids(
    pds_endpoint: str,
    dids: list[str],
    min_timestamp: str,
) -> list[str]:
    """Load previous results from queues and filter out DIDs that have already been processed."""
    previously_processed_dids = get_previously_processed_dids(
        pds_endpoint=pds_endpoint,
        min_timestamp=min_timestamp,
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
    total_dids_to_process: int,
    total_remaining_dids: int,
    total_records_flushed: int,
    total_worker_tasks_running: int,
    total_requests_made: int = 0,
):
    """Logs the following:
    1. Total DIDs for this PDS endpoint.
    2. Total DIDs processed in both results and deadletter queues.
    3. Total DIDs to be processed (at the start of the run).
    4. Total DIDs processed so far in this run.
    5. Total requests made so far in this run.
    """
    logger.info(
        f"=== Progress for PDS endpoint {pds_endpoint} ===\n"
        f"\tTotal DIDs for endpoint: {total_dids}\n"
        f"\tTotal DIDs previously processed {total_dids - total_dids_to_process}\n"
        f"\tTotal DIDs to be processed in this session: {total_dids_to_process}\n"
        f"\tTotal DIDS remaining in queue: {total_remaining_dids}/{total_dids_to_process}\n"
        f"\tTotal DIDs processed in this session: {total_records_flushed}\n"
        f"\tNumber of worker tasks running: {total_worker_tasks_running}\n"
        f"\tTotal requests made in this session: {total_requests_made}\n"
        f"=== End of progress for PDS endpoint {pds_endpoint} ==="
    )


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
        config: BackfillConfigSchema,
        pds_endpoint: Optional[str] = default_pds_endpoint,
        qps: int = default_qps,
        batch_size: int = default_write_batch_size,
    ):
        self.pds_endpoint = pds_endpoint
        self.dids = dids
        self.qps = qps
        self.semaphore = asyncio.Semaphore(qps)
        self.config = config

        # Counters for metrics
        self.total_requests = 0
        self.total_successes = 0

        self.max_retries = 3  # number of times to retry a failed DID before adding to deadletter queue.

        # token bucket, for managing rate limits.
        self.token_reset_minutes = 5  # 5 minutes, as per the rate limit spec.
        self.token_reset_seconds = self.token_reset_minutes * 60
        self.max_tokens = MANUAL_RATE_LIMIT
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
            min_timestamp=self.config.sync_storage.min_timestamp,
        )
        self.total_dids_to_process = len(self.dids)
        self.dids_filtered_out = self.original_total_dids - self.total_dids_to_process

        self.session = session
        self.cpu_pool = cpu_pool
        self._batch_size = batch_size
        self._writer_task: asyncio.Task | None = None
        self._db_task: asyncio.Task | None = None

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

        # min record timestamp - remove any records before this timestamp.
        self.min_record_timestamp = self.config.time_range.start_date

        # filters. Config used for determining how to filter synced
        # records, if applicable (e.g., filtering by URI).
        self.filters = self.config.filters
        self.uris_to_filter = load_uris_to_filter(
            type=self.filters.source.type,
            path=self.filters.source.path,
        )
        if not self.uris_to_filter:
            logger.warning(
                "No URIs to filter out. May be intentional, should check (this excludes previously processed URIs and only includes those pre-emptively chosen to be filtered out (e.g., only 'relevant' URIs are included))."
            )
            self.uris_to_filter = set()
        else:
            logger.info(f"Loaded {len(self.uris_to_filter)} URIs to filter out.")

        # track rate limit resets at a global level, so that all tasks can reference
        # the same reset time.
        self.rate_limit_reset_time_unix = None
        self.rate_limit_reset_time_ts = None

    async def _init_worker_queue(self):
        for did in self.dids:
            await self.temp_work_queue.put({"did": did})
        # Update queue size metric
        queue_size = self.temp_work_queue.qsize()
        pdm.BACKFILL_QUEUE_SIZES.labels(
            endpoint=self.pds_endpoint, queue_type="work"
        ).set(queue_size)
        pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=self.pds_endpoint).set(queue_size)

    def _filter_only_relevant_uri(self, record: dict) -> bool:
        """Filters for only URIs that are relevant to the sync."""
        return record["uri"] in self.uris_to_filter

    def _filter_records(self, records: list[dict]) -> list[dict]:
        """Filter for only posts/reposts in the date range of interest."""
        filtered_records = [
            record
            for record in records
            if filter_only_valid_bsky_records(
                record=record,
                types_to_sync=self.config.record_types,
            )
            and validate_time_range_record(
                record=record,
                start_timestamp=self.config.time_range.start_date,
                end_timestamp=self.config.time_range.end_date,
            )
            and self._filter_only_relevant_uri(record=record)
        ]
        return filtered_records

    async def _fetch_paginated_records(
        self,
        did: str,
        record_type: str,
        session: aiohttp.ClientSession,
        retry_count: int,
    ) -> Optional[list[dict]]:
        """Fetches all paginated records for a single record type.

        Args:
            record_type: str, the type of record to fetch (post, repost, etc.)
            did: str, the DID to fetch records for.
            session: aiohttp.ClientSession, the session to use to send the request.
            retry_count: int, number of retries attempted so far

        Returns:
            list[dict]: All records for this type, or None if error. Empty
            if there are no records.
        """
        all_records = []
        cursor = None
        total_paginated_requests = 1

        while True:
            try:
                # Rate limit handling
                pdm.BACKFILL_REQUESTS.labels(endpoint=self.pds_endpoint).inc()
                pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).inc()
                rand_noise = random.uniform(0.0, 0.5)
                await asyncio.sleep(
                    rand_noise
                )  # add some noise to avoid all workers fetching tokens at the same time.
                await self.token_bucket._acquire()
                pdm.BACKFILL_TOKENS_LEFT.labels(endpoint=self.pds_endpoint).set(
                    self.token_bucket._tokens
                )

                start = time.perf_counter()

                # Make paginated request
                atp_agent = AtpAgent(service=self.pds_endpoint)
                response = await atp_agent.async_list_records(
                    repo=did, collection=record_type, session=session, cursor=cursor
                )

                self.total_requests += 1
                pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).dec()

                # Track latency
                request_latency = time.perf_counter() - start
                pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
                    request_latency
                )

                # Handle rate limit tokens
                tokens_remaining: int = int(
                    response.headers.get("ratelimit-remaining", 0)
                )
                if tokens_remaining < self.token_bucket.get_tokens():
                    token_buffer_size = 50
                    new_tokens = tokens_remaining - token_buffer_size
                    logger.info(f"""
                        (PDS endpoint: {self.pds_endpoint}):
                            Tokens remaining (according to endpoint header): {tokens_remaining},
                            current tokens: {self.token_bucket.get_tokens()},
                            token buffer size: {token_buffer_size}
                            Setting tokens to {new_tokens}
                        """)
                    self.token_bucket.set_tokens(new_tokens)

                # Process response
                if response.status == 200:
                    response_dict = await response.json()
                    records: list[dict] = response_dict.get("records", [])
                    all_records.extend(records)

                    # no records synced.
                    if len(records) == 0:
                        break

                    # check if the last record in the response is before
                    # the min record timestamp. If so, we don't want to send
                    # any more requests.
                    earliest_record_timestamp = convert_bsky_dt_to_pipeline_dt(
                        records[-1]["value"]["createdAt"]
                    )
                    if earliest_record_timestamp < self.min_record_timestamp:
                        # logger.info(
                        #     f"""
                        #     (PDS endpoint: {self.pds_endpoint}):
                        #         Earliest record timestamp: {earliest_record_timestamp}
                        #         Min record timestamp: {self.min_record_timestamp}
                        #         Total paginated requests made for DID {did}, record type {record_type}: {total_paginated_requests}
                        #     """
                        # )
                        break

                    # Check if more pages
                    cursor = response_dict.get("cursor")
                    if not cursor:  # No more pages
                        # logger.info(
                        #     f"""
                        #     (PDS endpoint: {self.pds_endpoint}):
                        #         No more cursor found.
                        #         Total paginated requests made for DID {did},
                        #         record type {record_type}: {total_paginated_requests}
                        #     """
                        # )
                        break

                    total_paginated_requests += 1

                elif response.status == 429:
                    if self.rate_limit_reset_time_unix is None:
                        self.rate_limit_reset_time_unix = int(
                            response.headers["ratelimit-reset"]
                        )
                    else:
                        if (
                            int(response.headers["ratelimit-reset"])
                            > self.rate_limit_reset_time_unix
                        ):
                            self.rate_limit_reset_time_unix = int(
                                response.headers["ratelimit-reset"]
                            )
                    await asyncio.sleep(0.5)  # have a little buffer.
                    retry_count += 1
                    await self.temp_work_queue.put(
                        {"did": did, "retry_count": retry_count}
                    )
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="work"
                    ).set(self.temp_work_queue.qsize())
                    self.rate_limit_reset_time_ts = datetime.fromtimestamp(
                        self.rate_limit_reset_time_unix, tz=timezone.utc
                    ).strftime(timestamp_format)
                    logger.info(
                        f"""
                        (PDS endpoint: {self.pds_endpoint}): Rate limited, putting back in queue to retry. {did}.
                            Set new reset time: {self.rate_limit_reset_time_ts} UTC.
                        """
                    )
                    return None
                elif response.status == 400 and response.reason == "Bad Request":
                    # API returning 400 means the account is deleted/suspended.
                    # The PLC doc might exist, but if you look at Bluesky, their
                    # account won't exist.
                    logger.error(
                        f"(PDS endpoint: {self.pds_endpoint}): Account {did} is deleted/suspended. Adding to deadletter queue."
                    )
                    await self.temp_deadletter_queue.put({"did": did, "content": ""})
                    pdm.BACKFILL_DID_STATUS.labels(
                        endpoint=self.pds_endpoint, status="account_deleted"
                    ).inc()
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="deadletter"
                    ).set(self.temp_deadletter_queue.qsize())
                    return None
                else:
                    # Other errors - log and stop pagination
                    logger.error(
                        f"(PDS endpoint: {self.pds_endpoint}): Error fetching {record_type} records for DID {did}. "
                        f"Status: {response.status}, Reason: {response.reason}"
                    )
                    await self.temp_deadletter_queue.put({"did": did, "content": ""})
                    pdm.BACKFILL_DID_STATUS.labels(
                        endpoint=self.pds_endpoint, status="http_error"
                    ).inc()
                    pdm.BACKFILL_QUEUE_SIZES.labels(
                        endpoint=self.pds_endpoint, queue_type="deadletter"
                    ).set(self.temp_deadletter_queue.qsize())
                    return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Network errors - log and stop pagination
                error_type = (
                    "timeout" if isinstance(e, asyncio.TimeoutError) else "connection"
                )
                pdm.BACKFILL_NETWORK_ERRORS.labels(
                    endpoint=self.pds_endpoint, error_type=error_type
                ).inc()
                logger.warning(
                    f"(PDS endpoint: {self.pds_endpoint}): Connection error fetching {record_type} records for DID {did}: {e}"
                )
                break

        return all_records

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
            retry_count: int, number of retries attempted so far
            max_retries: int, optional maximum retries before giving up

        Runs the requests to the PDS endpoint using asyncio. Number of
        concurrent requests is managed by the semaphore. CPU-intensive work
        is offloaded to the thread pool.
        """

        if not max_retries:
            max_retries = self.max_retries

        if retry_count > max_retries:
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Failed to process DID {did} after {max_retries} retries. Adding to deadletter queue."
            )
            await self.temp_deadletter_queue.put({"did": did, "content": ""})
            pdm.BACKFILL_QUEUE_SIZES.labels(
                endpoint=self.pds_endpoint, queue_type="deadletter"
            ).set(self.temp_deadletter_queue.qsize())
            return

        try:
            all_records = []
            # Fetch paginated records for each type
            for record_type in self.config.record_types:
                async with self.semaphore:
                    current_unix_ts = time.time()
                    if (
                        self.rate_limit_reset_time_unix
                        and current_unix_ts < self.rate_limit_reset_time_unix
                    ):
                        seconds_sleep = (
                            self.rate_limit_reset_time_unix - current_unix_ts
                        )
                        logger.info(
                            f"(PDS endpoint: {self.pds_endpoint}): Rate limit will reset at ts={self.rate_limit_reset_time_ts}, sleeping for {seconds_sleep} seconds."
                        )
                        await asyncio.sleep(seconds_sleep)
                        # add a staggered sleep to avoid all workers hitting the
                        # rate limit at the same time.
                        rand_sleep = random.uniform(0.0, 2.0)
                        await asyncio.sleep(rand_sleep)
                    records: Optional[list[dict]] = await self._fetch_paginated_records(
                        did=did,
                        record_type=record_type,
                        session=session,
                        retry_count=retry_count,
                    )
                    if records is None:
                        logger.info(
                            f"(PDS endpoint: {self.pds_endpoint}): Error fetching records for {did} {record_type}. Putting DID back in queue to retry."
                        )
                        return
                    all_records.extend(records)

                # sleep to force waiting for the rate limit to reset,
                # since the other ways for avoiding the rate limit aren't working, bruh...
                sleep_seconds = 30
                await asyncio.sleep(sleep_seconds)

            if all_records:
                # Process collected records
                loop = asyncio.get_running_loop()
                processing_start = time.perf_counter()

                filtered_records: list[dict] = await loop.run_in_executor(
                    self.cpu_pool,
                    self._filter_records,
                    all_records,
                )

                processing_time = time.perf_counter() - processing_start
                pdm.BACKFILL_PROCESSING_SECONDS.labels(
                    endpoint=self.pds_endpoint, operation_type="parse_records"
                ).observe(processing_time)

                # Add to results queue
                await self.temp_results_queue.put(
                    {"did": did, "content": filtered_records}
                )

                # Update metrics
                pdm.BACKFILL_DID_STATUS.labels(
                    endpoint=self.pds_endpoint, status="success"
                ).inc()
                self.total_successes += 1

                if self.total_requests > 0:
                    success_ratio = self.total_successes / self.total_requests
                    pdm.BACKFILL_SUCCESS_RATIO.labels(endpoint=self.pds_endpoint).set(
                        success_ratio
                    )

                # Update queue metrics
                current_results_queue_size = self.temp_results_queue.qsize()
                pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=self.pds_endpoint).set(
                    current_results_queue_size
                )
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="results"
                ).set(current_results_queue_size)

                # Adjust backoff and concurrency
                if processing_time < 1.0:
                    self.backoff_factor = max(self.backoff_factor * 0.8, 1.0)

                self.response_times.append(processing_time)
                avg_response_time = sum(self.response_times) / len(self.response_times)

                if avg_response_time > 3.0:
                    await asyncio.sleep(avg_response_time / 2)

            else:
                # if they don't have records, put them in the
                # deadletter queue. Likely to be an inactive or deleted
                # account. Even if it's a valid account, if they don't
                # have any records, we treat them as if they were
                # deleted/suspended anyways.
                await self.temp_deadletter_queue.put({"did": did, "content": ""})
                pdm.BACKFILL_QUEUE_SIZES.labels(
                    endpoint=self.pds_endpoint, queue_type="deadletter"
                ).set(self.temp_deadletter_queue.qsize())

        except Exception as e:
            # Handle unexpected errors
            pdm.BACKFILL_ERRORS.labels(endpoint=self.pds_endpoint).inc()
            logger.error(
                f"(PDS endpoint: {self.pds_endpoint}): Unexpected error processing DID {did}: {e}"
            )
            logger.error(f"Traceback for unexpected error: {traceback.format_exc()}")
            await self.temp_deadletter_queue.put({"did": did, "content": ""})
            pdm.BACKFILL_DID_STATUS.labels(
                endpoint=self.pds_endpoint, status="unexpected_error"
            ).inc()
            pdm.BACKFILL_QUEUE_SIZES.labels(
                endpoint=self.pds_endpoint, queue_type="deadletter"
            ).set(self.temp_deadletter_queue.qsize())
            return None

    async def start(self):
        """Starts the backfill for a single PDS endpoint."""
        logger.info(f"(PDS endpoint: {self.pds_endpoint}): Start method ENTERED")

        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Initializing worker queue..."
        )
        await self._init_worker_queue()
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Worker queue initialized with {self.temp_work_queue.qsize()} items"
        )

        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Starting backfill with {len(self.dids)} DIDs."
        )

        logger.info(f"(PDS endpoint: {self.pds_endpoint}): Starting writer tasks...")
        # start the background writer to write to SQLite queues.
        self._writer_task = asyncio.create_task(self._writer())

        # start the background writer to write SQLite queues to persistent
        # DB storage.
        self._db_task = asyncio.create_task(self._write_queue_to_db())

        # create worker tasks. These worker tasks will pull DIDs from the
        # worker queue and process them. Fixed relative to desired QPS.
        num_workers = min(self.qps * 2, len(self.dids))

        producer_tasks = [
            asyncio.create_task(self._worker_loop()) for _ in range(num_workers)
        ]
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Starting {num_workers} worker tasks..."
        )
        self._producer_group = asyncio.gather(*producer_tasks)

        # give control back to the event loop without blocking.
        await asyncio.sleep(1.0)

    async def _worker_loop(self):
        """Worker loop that processes DIDs from the queue until it's empty.

        Allows us to manage DIDs by pulling from the temp worker queues and
        processing them instead of defining a fixed number of tasks.
        """
        try:
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
        except Exception as e:
            logger.error(
                f"(PDS endpoint: {self.pds_endpoint}): Worker loop ERROR: {e}",
                exc_info=True,
            )

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
        # we don't delete rows from the queue here because we delete the
        # queue later on anyways.
        loop = asyncio.get_running_loop()
        user_to_total_per_record_type_map = await loop.run_in_executor(
            self.cpu_pool, self._sync_persist_to_db, False
        )
        await loop.run_in_executor(
            self.cpu_pool,
            self.write_backfill_metadata_to_db,
            user_to_total_per_record_type_map,
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
        if self._db_task:
            self._db_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._db_task
        await self.output_results_queue.close()
        await self.output_deadletter_queue.close()

    async def _writer(self, batch_size: Optional[int] = None) -> None:
        """Background task to flush temp queues to persistent queues in batches."""
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

                total_buffer_size = len(result_buffer) + len(deadletter_buffer)

                # If we received final flush signal or batch size reached
                if final_flush_signal or total_buffer_size >= batch_size:
                    if result_buffer or deadletter_buffer:
                        # Create copies of buffers for processing
                        results_to_process = result_buffer.copy()
                        deadletters_to_process = deadletter_buffer.copy()

                        # Clear buffers immediately so we can continue collecting
                        if not final_flush_signal:
                            result_buffer = []
                            deadletter_buffer = []

                        # Offload CPU-intensive flush to thread pool
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(
                            self.cpu_pool,
                            self._sync_flush_buffers,
                            results_to_process,
                            deadletters_to_process,
                        )

                        global_total_records_flushed += total_buffer_size

                        # Log progress after work is done
                        log_progress(
                            pds_endpoint=self.pds_endpoint,
                            total_dids=self.original_total_dids,
                            total_dids_to_process=self.total_dids_to_process,
                            total_remaining_dids=self.temp_work_queue.qsize(),
                            total_records_flushed=global_total_records_flushed,
                            total_worker_tasks_running=len(
                                self._producer_group._children
                            ),
                            total_requests_made=self.total_requests,
                        )

                    if final_flush_signal:
                        return  # Exit the writer task

                # Yield control when queues are empty
                if (
                    self.temp_results_queue.empty()
                    and self.temp_deadletter_queue.empty()
                ):
                    await asyncio.sleep(1.0)  # 1 second is a good balance
        except Exception as e:
            logger.error(f"Error in writer: {e}", exc_info=True)
            raise
        finally:
            logger.info(f"Writer task for {self.pds_endpoint} finished.")

    def _sync_flush_buffers(
        self, result_buffer: list[dict], deadletter_buffer: list[dict]
    ) -> None:
        """Synchronous version of flush operation for thread pool execution."""
        flush_start = time.perf_counter()

        current_timestamp = generate_current_datetime_str()

        if result_buffer:
            self.output_results_queue.batch_add_items_to_queue(
                items=result_buffer,
                metadata={
                    "timestamp": current_timestamp,
                    "total_records": len(result_buffer),
                    "dids": [item["did"] for item in result_buffer],
                },
            )

        if deadletter_buffer:
            self.output_deadletter_queue.batch_add_items_to_queue(
                items=deadletter_buffer,
                metadata={
                    "timestamp": current_timestamp,
                    "total_records": len(deadletter_buffer),
                    "dids": [item["did"] for item in deadletter_buffer],
                },
            )

        flush_time = time.perf_counter() - flush_start
        pdm.BACKFILL_PROCESSING_SECONDS.labels(
            endpoint=self.pds_endpoint, operation_type="db_flush"
        ).observe(flush_time)

        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Finished flushing {len(result_buffer)} results and {len(deadletter_buffer)} deadletters to storage in {flush_time:.2f}s."
        )

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

    async def persist_to_db(self, delete_rows_from_queue: bool = True) -> dict:
        """Writes records to permanent .parquet storage.

        Returns counts of users per record type, in the form:
        {
            "<did>": {
                "post": 10,
                "repost": 5,
                "reply": 2,
            }
        }

        Takes as an optional argument whether to delete the rows from the queue.
        """

        logger.info("Persisting SQLite queue records to permanent storage...")

        items: list[dict] = self.output_results_queue.load_dict_items_from_queue()
        batch_ids: set[str] = {item["batch_id"] for item in items}
        deadletter_items: list[dict] = (
            self.output_deadletter_queue.load_dict_items_from_queue()
        )
        deadletter_batch_ids: set[str] = {item["batch_id"] for item in deadletter_items}

        if len(items) == 0 and len(deadletter_items) == 0:
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): No items to persist to DB."
            )
            return {}

        total_items: int = len(items)
        logger.info(
            f"(PDS endpoint: {self.pds_endpoint}): Persisting {total_items} items to DB..."
        )

        def process_batch(
            batch: list[dict],
            idx: int,
        ) -> dict:
            """Process a batch of items and return categorized records and user counts."""
            batch_posts = []
            batch_replies = []
            batch_reposts = []
            batch_follows = []
            batch_likes = []
            batch_blocks = []
            batch_user_counts = {}

            for item in batch:
                records = item["content"]
                user_did = item["did"]

                # Filter records
                records = self._filter_records(records=records)

                # Initialize user counts if needed. Done outside the 'records'
                # loop in case there are 0 records for a user.
                if user_did not in batch_user_counts:
                    batch_user_counts[user_did] = {
                        "post": 0,
                        "repost": 0,
                        "reply": 0,
                        "follow": 0,
                        "like": 0,
                        "block": 0,
                    }

                for record in records:
                    record_type: str = record.get(
                        "record_type", identify_record_type(record)
                    )

                    # Transform and postprocess
                    record = transform_backfilled_record(
                        did=user_did,
                        record=record,
                        record_type=record_type,
                        start_timestamp="2024-09-01-00:00:00",
                        end_timestamp="2024-12-02-00:00:00",
                    )
                    record = postprocess_backfilled_record(record)

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
                    elif record_type == "follow":
                        batch_follows.append(record)
                        batch_user_counts[user_did]["follow"] += 1
                    elif record_type == "like":
                        batch_likes.append(record)
                        batch_user_counts[user_did]["like"] += 1
                    elif record_type == "block":
                        batch_blocks.append(record)
                        batch_user_counts[user_did]["block"] += 1
                    else:
                        raise ValueError(
                            f"Unknown record type for item being written to DB: {record_type}"
                        )

            total_batch_posts = len(batch_posts)
            total_batch_replies = len(batch_replies)
            total_batch_reposts = len(batch_reposts)
            total_batch_follows = len(batch_follows)
            total_batch_likes = len(batch_likes)
            total_batch_blocks = len(batch_blocks)
            total_users = len(batch_user_counts)
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}) :Batch {idx} has {total_users} total users, {total_batch_posts} posts, {total_batch_replies} replies, {total_batch_reposts} reposts, {total_batch_follows} follows, {total_batch_likes} likes, and {total_batch_blocks} blocks."
            )
            return {
                "posts": batch_posts,
                "replies": batch_replies,
                "reposts": batch_reposts,
                "follows": batch_follows,
                "likes": batch_likes,
                "blocks": batch_blocks,
                "user_counts": batch_user_counts,
            }

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
            posts = []
            replies = []
            reposts = []
            follows = []
            likes = []
            blocks = []
            user_to_total_per_record_type_map = {}

            for result in results:
                posts.extend(result["posts"])
                replies.extend(result["replies"])
                reposts.extend(result["reposts"])
                follows.extend(result["follows"])
                likes.extend(result["likes"])
                blocks.extend(result["blocks"])
                batch_user_counts = result["user_counts"]

                # Merge user counts
                for user_did, counts in batch_user_counts.items():
                    if user_did not in user_to_total_per_record_type_map:
                        user_to_total_per_record_type_map[user_did] = {
                            "post": 0,
                            "repost": 0,
                            "reply": 0,
                            "follow": 0,
                            "like": 0,
                            "block": 0,
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
                    user_to_total_per_record_type_map[user_did]["follow"] += counts[
                        "follow"
                    ]
                    user_to_total_per_record_type_map[user_did]["like"] += counts[
                        "like"
                    ]
                    user_to_total_per_record_type_map[user_did]["block"] += counts[
                        "block"
                    ]

            logger.info(
                f"Parallel processing complete: {len(posts)} posts, {len(reposts)} reposts, {len(replies)} replies, {len(follows)} follows, {len(likes)} likes, and {len(blocks)} blocks."
            )

            # Create DataFrames
            posts_df = pd.DataFrame(posts)
            reposts_df = pd.DataFrame(reposts)
            replies_df = pd.DataFrame(replies)
            follows_df = pd.DataFrame(follows)
            likes_df = pd.DataFrame(likes)
            blocks_df = pd.DataFrame(blocks)

            total_posts = len(posts_df)
            total_reposts = len(reposts_df)
            total_replies = len(replies_df)
            total_follows = len(follows_df)
            total_likes = len(likes_df)
            total_blocks = len(blocks_df)

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Persisting {total_posts} posts, {total_reposts} reposts, {total_replies} replies, {total_follows} follows, {total_likes} likes, and {total_blocks} blocks to permanent storage..."
            )

            # get baseline measures of write size.
            sample_factor = 0.01  # 1% of total rows.

            total_parquet_size = 0

            for record_type, records in [
                ("post", posts_df),
                ("repost", reposts_df),
                ("reply", replies_df),
                ("follow", follows_df),
                ("like", likes_df),
                ("block", blocks_df),
            ]:
                if len(records) > 0:
                    sample_size = int(len(records) * sample_factor)
                    if sample_size == 0:
                        sample_size = 1
                    sample_df = records.sample(n=sample_size)
                    parquet_size = self._estimate_parquet_write_size(
                        record_type=record_type,
                        sample_df=sample_df,
                        sample_size=sample_size,
                        total_rows=len(records),
                    )
                    logger.info(f"- {record_type}: {parquet_size:.2f} MB")
                else:
                    logger.info(f"- {record_type}: 0 MB")
                total_parquet_size += parquet_size

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Total parquet size: {total_parquet_size:.2f} MB"
            )

            service = "raw_sync"

            for record_type, df in [
                ("post", posts_df),
                ("repost", reposts_df),
                ("reply", replies_df),
                ("follow", follows_df),
                ("like", likes_df),
                ("block", blocks_df),
            ]:
                if len(df) > 0:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Exporting {record_type} to local storage..."
                    )
                    export_data_to_local_storage(
                        service=service,
                        df=df,
                        custom_args={"record_type": record_type},
                    )
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Exported {record_type} to local storage."
                    )
                else:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): No {record_type} to export."
                    )

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Finished persisting {total_posts} posts, {total_reposts} reposts, {total_replies} replies, {total_follows} follows, {total_likes} likes, and {total_blocks} blocks to permanent storage."
            )

            # add deadletter queue records to counts.
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Loaded {len(deadletter_items)} deadletter items from queue..."
            )
            for item in deadletter_items:
                did = item["did"]
                if did not in user_to_total_per_record_type_map:
                    user_to_total_per_record_type_map[did] = {
                        "post": 0,
                        "repost": 0,
                        "reply": 0,
                        "follow": 0,
                        "like": 0,
                        "block": 0,
                    }
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Total user records to export as metadata (after adding deadletter items): {len(user_to_total_per_record_type_map)}"
            )

            # delete rows from queue, if relevant.
            if delete_rows_from_queue:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Deleting rows from queues..."
                )
                if len(batch_ids) > 0:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleting {len(batch_ids)} rows from queue..."
                    )
                    self.output_results_queue.batch_delete_items_by_ids(batch_ids)
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleted {len(batch_ids)} rows from queue."
                    )
                if len(deadletter_batch_ids) > 0:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleting {len(deadletter_batch_ids)} rows from deadletter queue..."
                    )
                    self.output_deadletter_queue.batch_delete_items_by_ids(
                        deadletter_batch_ids
                    )
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleted {len(deadletter_batch_ids)} rows from deadletter queue."
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
            f"(PDS endpoint: {self.pds_endpoint}): Total user records to export as metadata: {len(user_to_total_per_record_type_map)}"
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
            f"(PDS endpoint: {self.pds_endpoint}): Writing {len(user_backfill_metadata)} user backfill metadata to DB..."
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

    async def _write_queue_to_db(self):
        """Background task that writes the records in the SQLite queues to the DB."""
        try:
            while True:
                total_queued_results = self.output_results_queue.get_queue_length()
                total_queued_deadletters = (
                    self.output_deadletter_queue.get_queue_length()
                )
                total_queued = total_queued_results + total_queued_deadletters
                threshold_total_queued_result = 50

                if total_queued > threshold_total_queued_result:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}) Total queued records: {total_queued} > threshold, writing to DB..."
                    )

                    # Offload CPU-intensive persist_to_db to thread pool
                    loop = asyncio.get_running_loop()
                    user_to_total_per_record_type_map = await loop.run_in_executor(
                        self.cpu_pool, self._sync_persist_to_db
                    )

                    # Metadata writing can also be intensive, offload it too
                    await loop.run_in_executor(
                        self.cpu_pool,
                        self.write_backfill_metadata_to_db,
                        user_to_total_per_record_type_map,
                    )

                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}) Completed DB operations."
                    )
                else:
                    # More generous sleep when nothing to do
                    await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Error in _write_queue_to_db: {e}", exc_info=True)
            raise
        finally:
            logger.info(f"DB writer task for {self.pds_endpoint} finished.")

    def _sync_persist_to_db(self, delete_rows_from_queue: bool = True) -> dict:
        """Synchronous version of persist_to_db for thread pool execution.

        Writes records to permanent .parquet storage without blocking the event loop.

        Args:
            delete_rows_from_queue: Whether to delete processed items from the queue

        Returns:
            Dictionary mapping user DIDs to their record counts by type
        """
        logger.info("Persisting SQLite queue records to permanent storage...")

        items: list[dict] = self.output_results_queue.load_dict_items_from_queue()
        batch_ids: set[str] = {item["batch_id"] for item in items}

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
            batch_follows = []
            batch_likes = []
            batch_blocks = []
            batch_user_counts = {}

            for item in batch:
                records = item["content"]
                user_did = item["did"]

                # Filter records
                records = self._filter_records(records=records)

                # Initialize user counts if needed
                if user_did not in batch_user_counts:
                    batch_user_counts[user_did] = {
                        "post": 0,
                        "repost": 0,
                        "reply": 0,
                        "follow": 0,
                        "like": 0,
                        "block": 0,
                    }

                for record in records:
                    record_type: str = record.get(
                        "record_type", identify_record_type(record)
                    )

                    # Transform and postprocess
                    record = transform_backfilled_record(
                        did=user_did,
                        record=record,
                        record_type=record_type,
                        start_timestamp="2024-09-01-00:00:00",
                        end_timestamp="2024-12-02-00:00:00",
                    )
                    record = postprocess_backfilled_record(record)

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
                    elif record_type == "follow":
                        batch_follows.append(record)
                        batch_user_counts[user_did]["follow"] += 1
                    elif record_type == "like":
                        batch_likes.append(record)
                        batch_user_counts[user_did]["like"] += 1
                    elif record_type == "block":
                        batch_blocks.append(record)
                        batch_user_counts[user_did]["block"] += 1
                    else:
                        raise ValueError(
                            f"Unknown record type for item being written to DB: {record_type}"
                        )

            total_batch_posts = len(batch_posts)
            total_batch_replies = len(batch_replies)
            total_batch_reposts = len(batch_reposts)
            total_batch_follows = len(batch_follows)
            total_batch_likes = len(batch_likes)
            total_batch_blocks = len(batch_blocks)
            total_users = len(batch_user_counts)
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}) :Batch {idx} has {total_users} total users, {total_batch_posts} posts, {total_batch_replies} replies, {total_batch_reposts} reposts, {total_batch_follows} follows, {total_batch_likes} likes, and {total_batch_blocks} blocks."
            )
            return (
                batch_posts,
                batch_replies,
                batch_reposts,
                batch_follows,
                batch_likes,
                batch_blocks,
                batch_user_counts,
            )

        try:
            num_threads = 4
            batch_size = max(1, len(items) // num_threads)
            batches = create_batches(items, batch_size)
            num_batches = len(batches)
            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Processing {num_batches} batches of {batch_size} items each (total {total_items} items)..."
            )
            results = []
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [
                    executor.submit(process_batch, batch, i)
                    for i, batch in enumerate(batches)
                ]
                # Wait for all futures to complete and collect results
                for future in futures:
                    results.append(future.result())

            # Combine results
            posts, replies, reposts, follows, likes, blocks = [], [], [], [], [], []
            user_to_total_per_record_type_map = {}

            for (
                batch_posts,
                batch_replies,
                batch_reposts,
                batch_follows,
                batch_likes,
                batch_blocks,
                batch_user_counts,
            ) in results:
                posts.extend(batch_posts)
                replies.extend(batch_replies)
                reposts.extend(batch_reposts)
                follows.extend(batch_follows)
                likes.extend(batch_likes)
                blocks.extend(batch_blocks)

                # Merge user counts
                for user_did, counts in batch_user_counts.items():
                    if user_did not in user_to_total_per_record_type_map:
                        user_to_total_per_record_type_map[user_did] = {
                            "post": 0,
                            "repost": 0,
                            "reply": 0,
                            "follow": 0,
                            "like": 0,
                            "block": 0,
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
                    user_to_total_per_record_type_map[user_did]["follow"] += counts[
                        "follow"
                    ]
                    user_to_total_per_record_type_map[user_did]["like"] += counts[
                        "like"
                    ]
                    user_to_total_per_record_type_map[user_did]["block"] += counts[
                        "block"
                    ]

            logger.info(
                f"Parallel processing complete: {len(posts)} posts, {len(reposts)} reposts, {len(replies)} replies, {len(follows)} follows, {len(likes)} likes, and {len(blocks)} blocks"
            )

            # Create DataFrames - same as before
            posts_df = pd.DataFrame(posts) if posts else pd.DataFrame()
            reposts_df = pd.DataFrame(reposts) if reposts else pd.DataFrame()
            replies_df = pd.DataFrame(replies) if replies else pd.DataFrame()
            follows_df = pd.DataFrame(follows) if follows else pd.DataFrame()
            likes_df = pd.DataFrame(likes) if likes else pd.DataFrame()
            blocks_df = pd.DataFrame(blocks) if blocks else pd.DataFrame()

            total_posts = len(posts_df)
            total_reposts = len(reposts_df)
            total_replies = len(replies_df)
            total_follows = len(follows_df)
            total_likes = len(likes_df)
            total_blocks = len(blocks_df)

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Persisting {total_posts} posts, {total_reposts} reposts, and {total_replies} replies to permanent storage..."
            )

            # Get baseline measures of write size - same as before
            sample_factor = 0.01  # 1% of total rows.
            posts_parquet_size = 0
            reposts_parquet_size = 0
            replies_parquet_size = 0
            follows_parquet_size = 0
            likes_parquet_size = 0
            blocks_parquet_size = 0

            if total_posts > 0:
                posts_sample_size = max(1, int(total_posts * sample_factor))
                posts_sample_df = posts_df.sample(n=posts_sample_size)
                posts_parquet_size = self._estimate_parquet_write_size(
                    record_type="post",
                    sample_df=posts_sample_df,
                    sample_size=posts_sample_size,
                    total_rows=total_posts,
                )

            if total_reposts > 0:
                reposts_sample_size = max(1, int(total_reposts * sample_factor))
                reposts_sample_df = reposts_df.sample(n=reposts_sample_size)
                reposts_parquet_size = self._estimate_parquet_write_size(
                    record_type="repost",
                    sample_df=reposts_sample_df,
                    sample_size=reposts_sample_size,
                    total_rows=total_reposts,
                )

            if total_replies > 0:
                replies_sample_size = max(1, int(total_replies * sample_factor))
                replies_sample_df = replies_df.sample(n=replies_sample_size)
                replies_parquet_size = self._estimate_parquet_write_size(
                    record_type="reply",
                    sample_df=replies_sample_df,
                    sample_size=replies_sample_size,
                    total_rows=total_replies,
                )

            if total_follows > 0:
                follows_sample_size = max(1, int(total_follows * sample_factor))
                follows_sample_df = follows_df.sample(n=follows_sample_size)
                follows_parquet_size = self._estimate_parquet_write_size(
                    record_type="follow",
                    sample_df=follows_sample_df,
                    sample_size=follows_sample_size,
                    total_rows=total_follows,
                )

            if total_likes > 0:
                likes_sample_size = max(1, int(total_likes * sample_factor))
                likes_sample_df = likes_df.sample(n=likes_sample_size)
                likes_parquet_size = self._estimate_parquet_write_size(
                    record_type="like",
                    sample_df=likes_sample_df,
                    sample_size=likes_sample_size,
                    total_rows=total_likes,
                )

            if total_blocks > 0:
                blocks_sample_size = max(1, int(total_blocks * sample_factor))
                blocks_sample_df = blocks_df.sample(n=blocks_sample_size)
                blocks_parquet_size = self._estimate_parquet_write_size(
                    record_type="block",
                    sample_df=blocks_sample_df,
                    sample_size=blocks_sample_size,
                    total_rows=total_blocks,
                )

            logger.info(f"""
                (PDS endpoint: {self.pds_endpoint})
                Estimated parquet sizes:
                - posts: {posts_parquet_size:.2f} MB
                - reposts: {reposts_parquet_size:.2f} MB
                - replies: {replies_parquet_size:.2f} MB
                - follows: {follows_parquet_size:.2f} MB
                - likes: {likes_parquet_size:.2f} MB
                - blocks: {blocks_parquet_size:.2f} MB
            """)

            service = "raw_sync"

            # Export data to storage

            for (
                record_type,
                df,
            ) in [
                ("post", posts_df),
                ("repost", reposts_df),
                ("reply", replies_df),
                ("follow", follows_df),
                ("like", likes_df),
                ("block", blocks_df),
            ]:
                if len(df) > 0:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Exporting {record_type} records to local storage..."
                    )
                    export_data_to_local_storage(
                        service=service,
                        df=df,
                        custom_args={"record_type": record_type},
                    )
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Exported {record_type} to local storage."
                    )
                else:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): No {record_type} records to export."
                    )

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Finished persisting {total_posts} posts, {total_reposts} reposts, and {total_replies} replies to permanent storage."
            )

            # Handle deadletter queue - same as before
            deadletter_items = self.output_deadletter_queue.load_dict_items_from_queue()
            deadletter_batch_ids = {item["batch_id"] for item in deadletter_items}

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Loaded {len(deadletter_items)} deadletter items from queue..."
            )

            for item in deadletter_items:
                did = item["did"]
                if did not in user_to_total_per_record_type_map:
                    user_to_total_per_record_type_map[did] = {
                        "post": 0,
                        "repost": 0,
                        "reply": 0,
                        "follow": 0,
                        "like": 0,
                        "block": 0,
                    }

            logger.info(
                f"(PDS endpoint: {self.pds_endpoint}): Total user records to export as metadata (after adding deadletter items): {len(user_to_total_per_record_type_map)}"
            )

            # Delete rows from queue if requested
            if delete_rows_from_queue:
                logger.info(
                    f"(PDS endpoint: {self.pds_endpoint}): Deleting rows from queues..."
                )
                if batch_ids:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleting {len(batch_ids)} rows from queue..."
                    )
                    self.output_results_queue.batch_delete_items_by_ids(batch_ids)
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleted {len(batch_ids)} rows from queue."
                    )
                if deadletter_batch_ids:
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleting {len(deadletter_batch_ids)} rows from deadletter queue..."
                    )
                    self.output_deadletter_queue.batch_delete_items_by_ids(
                        deadletter_batch_ids
                    )
                    logger.info(
                        f"(PDS endpoint: {self.pds_endpoint}): Deleted {len(deadletter_batch_ids)} rows from deadletter queue."
                    )

            return user_to_total_per_record_type_map

        except Exception as e:
            logger.error(f"Error persisting to DB: {e}", exc_info=True)
            raise
