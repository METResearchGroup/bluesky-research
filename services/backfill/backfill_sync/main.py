"""Backfill module for syncing data from Jetstream.

This module provides functionality to backfill data from the Bluesky firehose
via Jetstream, supporting filtering by DIDs and timestamps.
"""

import asyncio
from typing import Optional, Any

from lib.log.logger import get_logger
from services.sync.jetstream.helper import timestamp_to_unix_microseconds
from services.sync.jetstream.jetstream_connector import (
    JetstreamConnector,
    PUBLIC_INSTANCES,
)

logger = get_logger(__file__)


async def backfill_sync(
    wanted_dids: Optional[list[str]] = None,
    start_timestamp: Optional[str] = None,
    wanted_collections: Optional[list[str]] = None,
    num_records: int = 10000,
    instance: str = PUBLIC_INSTANCES[0],
    max_time: int = 900,  # 15 minutes default
    queue_name: str = "jetstream_sync",
    batch_size: int = 100,
    compress: bool = False,
) -> dict[str, Any]:
    """Backfill data from the Bluesky firehose via Jetstream.

    Args:
        wanted_dids: Optional list of DIDs to filter for
        start_timestamp: Optional timestamp to start from (YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS)
        wanted_collections: Optional list of collection types to include
        num_records: Number of records to collect (default: 10000)
        instance: Jetstream instance to connect to
        max_time: Maximum time to run in seconds (default: 900 - 15 minutes)
        queue_name: Name for the queue (default: jetstream_sync)
        batch_size: Number of records to batch together for queue insertion
        compress: Whether to use zstd compression for the stream

    Returns:
        Dictionary with statistics about the ingestion process
    """
    # Use default collections if none specified
    if wanted_collections is None:
        wanted_collections = ["app.bsky.feed.post"]

    # Convert start_timestamp to cursor if provided
    cursor = None
    if start_timestamp:
        cursor = str(timestamp_to_unix_microseconds(start_timestamp))
        logger.info(f"Using cursor: {cursor} (from timestamp: {start_timestamp})")

    # Initialize JetstreamConnector
    connector = JetstreamConnector(queue_name=queue_name, batch_size=batch_size)

    # Log information about the operation
    logger.info(f"Starting backfill sync from Jetstream instance: {instance}")
    logger.info(f"Wanted collections: {', '.join(wanted_collections)}")

    if wanted_dids:
        logger.info(
            f"Filtering for DIDs: {', '.join(wanted_dids[:5])}{'...' if len(wanted_dids) > 5 else ''}"
        )

    logger.info(f"Target record count: {num_records}, Max time: {max_time}s")
    logger.info(f"Queue name: {queue_name}, Batch size: {batch_size}")

    # Run the ingestion
    try:
        stats = await connector.listen_until_count(
            instance=instance,
            wanted_collections=wanted_collections,
            target_count=num_records,
            max_time=max_time,
            cursor=cursor,
            wanted_dids=wanted_dids,
        )

        # Log the results
        logger.info(f"Backfill sync completed. Stats: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Error during backfill sync: {e}")
        raise


def run_backfill_sync(
    wanted_dids: Optional[list[str]] = None,
    start_timestamp: Optional[str] = None,
    wanted_collections: Optional[list[str]] = None,
    num_records: int = 10000,
    instance: str = PUBLIC_INSTANCES[0],
    max_time: int = 900,
    queue_name: str = "jetstream_sync",
    batch_size: int = 100,
    compress: bool = False,
) -> dict[str, Any]:
    """Run the backfill sync process synchronously.

    This is a wrapper around backfill_sync that handles the asyncio event loop.
    See backfill_sync for parameter details.

    Returns:
        Dictionary with statistics about the ingestion process
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        backfill_sync(
            wanted_dids=wanted_dids,
            start_timestamp=start_timestamp,
            wanted_collections=wanted_collections,
            num_records=num_records,
            instance=instance,
            max_time=max_time,
            queue_name=queue_name,
            batch_size=batch_size,
            compress=compress,
        )
    )
