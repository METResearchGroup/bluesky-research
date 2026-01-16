"""Multithreaded backfill experiments."""

import aiohttp
import asyncio
import concurrent.futures
import os
import time

from lib.db.queue import Queue
from lib.log.logger import get_logger
from services.backfill.core.constants import MANUAL_RATE_LIMIT
from services.backfill.core.worker import AsyncTokenBucket

logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))


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
