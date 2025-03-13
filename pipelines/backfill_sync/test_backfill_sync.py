#!/usr/bin/env python
"""Test script for backfill_sync with a specific DID.

This script demonstrates how to run backfill_sync for a specific DID using
the default collections and timestamp.
"""

import sys
from typing import Optional

from lib.log.logger import get_logger
from services.backfill.backfill_sync import run_backfill_sync
from pipelines.backfill_sync.backfill_all_users import DEFAULT_START_TIMESTAMP

# The target DID to test with
TEST_DID = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"

logger = get_logger(__name__)


def test_backfill_sync(
    did: str = TEST_DID,
    start_timestamp: str = DEFAULT_START_TIMESTAMP,
    end_timestamp: Optional[str] = None,
    collections: Optional[list[str]] = None,
    num_records: int = 1000,
    max_time: int = 300,  # Shorter timeout for testing
) -> None:
    """Run a test backfill sync for a specific DID.

    Args:
        did: The DID to backfill
        start_timestamp: Start timestamp for the backfill
        end_timestamp: Optional end timestamp to stop at
        collections: List of collections to include (defaults to ["app.bsky.feed.post"])
        num_records: Number of records to collect
        max_time: Maximum time to run in seconds
    """
    # Set default collections if None
    if collections is None:
        collections = ["app.bsky.feed.post"]

    logger.info(f"Starting test backfill sync for DID: {did}")
    logger.info(f"Using start timestamp: {start_timestamp}")
    if end_timestamp:
        logger.info(f"Using end timestamp: {end_timestamp}")
    logger.info(f"Using collections: {collections}")

    try:
        # Run the backfill sync
        stats = run_backfill_sync(
            wanted_dids=[did],
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            wanted_collections=collections,
            num_records=num_records,
            max_time=max_time,
        )

        # Display results
        logger.info("\nBackfill sync completed!")
        logger.info(f"Records stored: {stats['records_stored']}")
        logger.info(f"Messages received: {stats['messages_received']}")
        logger.info(f"Collections seen: {', '.join(stats['collections'])}")
        logger.info(f"Time taken: {stats['total_time']:.2f} seconds")
        logger.info(f"Rate: {stats['records_per_second']:.2f} records/second")
        logger.info(f"Queue length: {stats['queue_length']} items")

        if stats.get("current_date"):
            logger.info(f"Current date reached: {stats['current_date']}")

        if stats.get("end_cursor_reached"):
            logger.info("Stopped because end timestamp was reached.")

        if stats.get("latest_cursor"):
            logger.info(f"\nLatest cursor: {stats['latest_cursor']}")

    except Exception as e:
        logger.error(f"Error during test backfill sync: {e}")
        sys.exit(1)


if __name__ == "__main__":
    test_backfill_sync()
    logger.info("Test completed successfully!")
