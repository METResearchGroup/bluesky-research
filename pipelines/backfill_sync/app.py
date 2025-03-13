#!/usr/bin/env python
"""CLI app for backfilling data from the Bluesky firehose via Jetstream.

This module provides a command-line interface for syncing data from Bluesky via
Jetstream with options for filtering by DIDs, collections, and timestamps.
"""

import sys
import click
from typing import Optional

from lib.constants import timestamp_format
from lib.log.logger import get_logger
from services.backfill.backfill_sync import run_backfill_sync
from services.sync.jetstream.constants import VALID_COLLECTIONS
from services.sync.jetstream.helper import validate_timestamp, parse_handles
from services.sync.jetstream.jetstream_connector import PUBLIC_INSTANCES

logger = get_logger(__name__)


def validate_did_list(ctx, param, value):
    """Validate and parse a comma-separated list of DIDs."""
    if value is None:
        return None

    dids = [did.strip() for did in value.split(",") if did.strip()]

    # Validate that each DID follows the expected format
    for did in dids:
        if not did.startswith("did:plc:"):
            raise click.BadParameter(
                f"Invalid DID format: {did}. DIDs should start with 'did:plc:'"
            )

    return dids


def validate_flexible_timestamp(ctx, param, value):
    """Validate timestamp in either YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS format."""
    return validate_timestamp(ctx, param, value, format=None)


@click.command()
@click.option(
    "--wanted-collections",
    "-c",
    type=click.Choice(VALID_COLLECTIONS),
    multiple=True,
    default=["app.bsky.feed.post"],
    help="Collection types to include (can be specified multiple times)",
)
@click.option(
    "--wanted-dids",
    "-d",
    callback=validate_did_list,
    help="Comma-separated list of DIDs to filter for",
)
@click.option(
    "--start-timestamp",
    "-s",
    callback=validate_flexible_timestamp,
    help=f"Start timestamp in YYYY-MM-DD or {timestamp_format} format",
)
@click.option(
    "--end-timestamp",
    "-e",
    callback=validate_flexible_timestamp,
    help=f"End timestamp in YYYY-MM-DD or {timestamp_format} format (stop when reaching this time)",
)
@click.option(
    "--num-records",
    "-n",
    type=int,
    default=10000,
    help="Number of records to collect (default: 10000)",
)
@click.option(
    "--instance",
    "-i",
    type=click.Choice(PUBLIC_INSTANCES),
    default=PUBLIC_INSTANCES[0],
    help="Jetstream instance to connect to",
)
@click.option(
    "--max-time",
    "-t",
    type=int,
    default=900,
    help="Maximum time to run in seconds (default: 900 - 15 minutes)",
)
@click.option(
    "--queue-name",
    type=str,
    default="jetstream_sync",
    help="Name for the queue (default: jetstream_sync)",
)
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="Number of records to batch together for queue insertion (default: 100)",
)
@click.option(
    "--compress",
    is_flag=True,
    default=False,
    help="Use zstd compression for the stream",
)
@click.option(
    "--bluesky-user-handles",
    "-u",
    callback=parse_handles,
    help="List of Bluesky user handles to get data for (comma-separated). Note: This is only for future compatibility and is not yet implemented",
)
def backfill_sync_cli(
    wanted_collections: tuple[str, ...],
    wanted_dids: Optional[list[str]],
    start_timestamp: Optional[str],
    end_timestamp: Optional[str],
    num_records: int,
    instance: str,
    max_time: int,
    queue_name: str,
    batch_size: int,
    compress: bool,
    bluesky_user_handles: Optional[list[str]],
):
    """Backfill data from the Bluesky firehose via Jetstream.

    This command connects to a Bluesky Jetstream instance, subscribes to specific
    collections and DIDs, and stores the resulting records in a queue for further
    processing by other components.

    Examples:
        # Backfill 10000 posts from the firehose
        $ python -m pipelines.backfill_sync.app

        # Backfill 5000 likes and follows from a specific date
        $ python -m pipelines.backfill_sync.app -c app.bsky.feed.like -c app.bsky.graph.follow -n 5000 -s 2024-01-01

        # Backfill 5000 likes and follows from a specific date and time range
        $ python -m pipelines.backfill_sync.app -c app.bsky.feed.like -c app.bsky.graph.follow -n 5000 -s 2024-01-01-12:30:00 -e 2024-01-02-12:30:00

        # Backfill posts from specific DIDs
        $ python -m pipelines.backfill_sync.app -d did:plc:abcdef123456,did:plc:xyz789

        # Backfill posts to a custom queue with a specific batch size
        $ python -m pipelines.backfill_sync.app --queue-name custom_queue --batch-size 50
    """
    # Log warning if bluesky_user_handles is provided
    if bluesky_user_handles:
        logger.warning(
            f"Bluesky user handles specified: {', '.join(bluesky_user_handles)}"
        )
        logger.warning("Note: Filtering by user handles is not yet implemented.")

    try:
        logger.info("Starting backfill sync process...")
        stats = run_backfill_sync(
            wanted_dids=wanted_dids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            wanted_collections=list(wanted_collections),
            num_records=num_records,
            instance=instance,
            max_time=max_time,
            queue_name=queue_name,
            batch_size=batch_size,
            compress=compress,
        )

        # Print results
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
            logger.info(
                "Use this cursor value to resume from this point in a future run."
            )

    except Exception as e:
        logger.error(f"Error during backfill sync: {e}")
        sys.exit(1)


if __name__ == "__main__":
    backfill_sync_cli()
