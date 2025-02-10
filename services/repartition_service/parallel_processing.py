"""Parallel processing implementation for repartition service.

This module provides parallel processing capabilities for the repartition service,
enabling concurrent processing of multiple partition dates while maintaining data
integrity and providing progress monitoring.
"""

import concurrent.futures
import multiprocessing
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from lib.helper import get_partition_dates
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.repartition_service.helper import (
    OperationResult,
    OperationStatus,
    RepartitionError,
    repartition_data_for_partition_date,
)

logger = get_logger(__file__)


@dataclass
class ParallelConfig:
    """Configuration for parallel processing."""

    max_workers: int = 4  # Default number of worker processes
    chunk_size: int = 10  # Number of dates to process per worker
    timeout: int = 3600  # Maximum time (seconds) for a chunk to complete
    progress_interval: int = 5  # Seconds between progress updates


def process_date_chunk(
    dates: List[str],
    service: str,
    new_service_partition_key: str,
    shared_state: Dict,
) -> Dict[str, OperationResult]:
    """Process a chunk of dates in a single process.

    Args:
        dates (List[str]): List of partition dates to process
        service (str): Name of the service to repartition
        new_service_partition_key (str): Field name to use as the new partition key
        shared_state (Dict): Shared state dictionary for progress tracking

    Returns:
        Dict[str, OperationResult]: Results for each date in the chunk

    Behavior:
        1. Processes each date in the chunk sequentially
        2. Updates shared state with progress
        3. Returns results for all dates in chunk
    """
    results = {}
    for date in dates:
        result = repartition_data_for_partition_date(
            partition_date=date,
            service=service,
            new_service_partition_key=new_service_partition_key,
        )
        results[date] = result
        with shared_state.get_lock():
            shared_state.value += 1
    return results


def monitor_progress(
    shared_state: multiprocessing.Value,
    total_dates: int,
    update_interval: int = 5,
    stop_event: Optional[multiprocessing.Event] = None,
) -> None:
    """Monitor and log progress of parallel processing.

    Args:
        shared_state (multiprocessing.Value): Shared counter of processed dates
        total_dates (int): Total number of dates to process
        update_interval (int): Seconds between progress updates
        stop_event (Optional[multiprocessing.Event]): Event to signal monitoring should stop

    Behavior:
        1. Periodically checks progress
        2. Logs completion percentage
        3. Continues until all dates processed or stop event is set
    """
    while True:
        if stop_event and stop_event.is_set():
            break
        with shared_state.get_lock():
            processed = shared_state.value
        progress = (processed / total_dates) * 100
        logger.info(
            f"Progress: {progress:.2f}% ({processed}/{total_dates} dates processed)"
        )
        if processed >= total_dates:
            break
        time.sleep(update_interval)


def recover_failed_chunks(
    failed_dates: List[str],
    service: str,
    new_service_partition_key: str,
    max_retries: int = 3,
) -> Dict[str, OperationResult]:
    """Retry failed dates with exponential backoff.

    Args:
        failed_dates (List[str]): List of dates that failed processing
        service (str): Name of the service to repartition
        new_service_partition_key (str): Field name to use as the new partition key
        max_retries (int): Maximum number of retry attempts

    Returns:
        Dict[str, OperationResult]: Results of retry attempts

    Behavior:
        1. Attempts to process each failed date up to max_retries times
        2. Uses exponential backoff between attempts
        3. Returns results of all retry attempts, including failures
    """
    results = {}
    for date in failed_dates:
        last_result = None
        for attempt in range(max_retries):
            try:
                result = repartition_data_for_partition_date(
                    partition_date=date,
                    service=service,
                    new_service_partition_key=new_service_partition_key,
                )
                if result.status == OperationStatus.SUCCESS:
                    results[date] = result
                    break
                last_result = result
                time.sleep(2**attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Retry {attempt + 1} failed for {date}: {e}")
                last_result = OperationResult(
                    status=OperationStatus.FAILED,
                    error=RepartitionError(f"Retry {attempt + 1} failed: {str(e)}"),
                )

        # If all retries failed, include the last failure result
        if date not in results and last_result:
            results[date] = last_result

    return results


def repartition_data_for_partition_dates_parallel(
    start_date: str,
    end_date: str,
    service: str,
    new_service_partition_key: str = "created_at",
    exclude_partition_dates: List[str] = None,
    parallel_config: Optional[ParallelConfig] = None,
) -> Dict[str, OperationResult]:
    """Parallel implementation of repartitioning.

    Args:
        start_date (str): Start date in YYYY-MM-DD format (inclusive)
        end_date (str): End date in YYYY-MM-DD format (inclusive)
        service (str): Name of the service to repartition
        new_service_partition_key (str): Field name to use as the new partition key
        exclude_partition_dates (List[str], optional): List of dates to exclude
        parallel_config (Optional[ParallelConfig]): Configuration for parallel processing

    Returns:
        Dict[str, OperationResult]: Results for all processed dates

    Raises:
        ValueError: If service name is empty or invalid

    Behavior:
        1. Validates input parameters
        2. Sets up parallel processing infrastructure
        3. Processes date chunks in parallel
        4. Monitors progress
        5. Handles failures and retries
        6. Returns combined results
    """
    # Validation
    if not service:
        raise ValueError("Service name is required")
    if service not in MAP_SERVICE_TO_METADATA:
        raise ValueError(f"Unknown service: {service}")

    # Setup configuration
    config = parallel_config or ParallelConfig()
    exclude_partition_dates = exclude_partition_dates or ["2024-10-08"]

    # Get dates and create chunks
    dates = get_partition_dates(start_date, end_date, exclude_partition_dates)
    if not dates:
        logger.warning("No dates to process")
        return {}

    date_chunks = [
        dates[i : i + config.chunk_size]
        for i in range(0, len(dates), config.chunk_size)
    ]

    # Setup shared state and monitoring
    shared_state = multiprocessing.Value("i", 0)
    stop_event = multiprocessing.Event()
    monitor = multiprocessing.Process(
        target=monitor_progress,
        args=(shared_state, len(dates), config.progress_interval, stop_event),
    )
    monitor.start()

    # Process chunks in parallel
    results = {}
    failed_dates = []
    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=config.max_workers
        ) as executor:
            futures = []
            for chunk in date_chunks:
                future = executor.submit(
                    process_date_chunk,
                    chunk,
                    service,
                    new_service_partition_key,
                    shared_state,
                )
                futures.append(future)

            # Collect results
            for future in concurrent.futures.as_completed(futures):
                try:
                    chunk_results = future.result(timeout=config.timeout)
                    results.update(chunk_results)
                    # Identify failed dates
                    for date, result in chunk_results.items():
                        if result.status == OperationStatus.FAILED:
                            failed_dates.append(date)
                except Exception as e:
                    logger.error(f"Chunk processing failed: {e}")

    finally:
        # Stop progress monitoring
        stop_event.set()
        monitor.join()

    # Retry failed dates
    if failed_dates:
        logger.info(f"Retrying {len(failed_dates)} failed dates...")
        retry_results = recover_failed_chunks(
            failed_dates, service, new_service_partition_key
        )
        results.update(retry_results)

    return results
