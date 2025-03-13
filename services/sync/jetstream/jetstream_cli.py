#!/usr/bin/env python
"""BlueskyStream CLI - Connect to the Bluesky firehose through Jetstream.

This script provides a command-line interface to connect to the Bluesky firehose
via Jetstream, filter events by collection types and DIDs, and store records in a
queue for further processing.
"""

import asyncio
import click
import sys
from typing import Optional

from lib.log.logger import get_logger
from services.sync.jetstream.constants import VALID_COLLECTIONS
from services.sync.jetstream.helper import (
    timestamp_to_unix_microseconds,
    validate_timestamp,
    parse_handles,
)
from services.sync.jetstream.jetstream_connector import (
    JetstreamConnector,
    PUBLIC_INSTANCES,
)

logger = get_logger(__file__)


# Callback for timestamp validation with YYYY-MM-DD format
def validate_date_format(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Click callback for validating date in YYYY-MM-DD format."""
    return validate_timestamp(ctx, param, value, format="%Y-%m-%d")


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
    multiple=True,
    help="Specific DIDs to include (can be specified multiple times)",
)
@click.option(
    "--start-timestamp",
    "-s",
    callback=validate_date_format,
    help="Start timestamp in YYYY-MM-DD format",
)
@click.option(
    "--num-records",
    "-n",
    type=int,
    default=1000,
    help="Number of records to collect (default: 1000)",
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
    default=300,
    help="Maximum time to run in seconds (default: 300)",
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
    help="List of Bluesky user handles to get data for (comma-separated)",
)
def stream_bluesky(
    wanted_collections: list[str],
    wanted_dids: list[str],
    start_timestamp: Optional[str],
    num_records: int,
    instance: str,
    max_time: int,
    queue_name: str,
    batch_size: int,
    compress: bool,
    bluesky_user_handles: Optional[list[str]],
):
    """Stream data from the Bluesky firehose via Jetstream.

    This command connects to a Bluesky Jetstream instance, subscribes to specific
    collections and DIDs, and stores the resulting records in a queue for further processing.

    Examples:
        # Stream 1000 posts from the firehose
        $ python -m services.sync.jetstream.jetstream_cli

        # Stream 500 likes and follows from a specific date
        $ python -m services.sync.jetstream.jetstream_cli -c app.bsky.feed.like -c app.bsky.graph.follow -n 500 -s 2024-01-01

        # Stream posts from specific DIDs
        $ python -m services.sync.jetstream.jetstream_cli -d did:plc:abcdef123456 -d did:plc:xyz789

        # Stream posts to a custom queue with a specific batch size
        $ python -m services.sync.jetstream.jetstream_cli --queue-name custom_queue --batch-size 50

        # Stream posts from specific Bluesky user handles
        $ python -m services.sync.jetstream.jetstream_cli -u alice.bsky.social,bob.bsky.social
    """
    # Convert start_timestamp to cursor if provided
    cursor = None
    if start_timestamp:
        cursor = str(timestamp_to_unix_microseconds(start_timestamp, format="%Y-%m-%d"))
        logger.info(f"Using cursor: {cursor} (from timestamp: {start_timestamp})")

    # Initialize JetstreamConnector
    connector = JetstreamConnector(queue_name=queue_name, batch_size=batch_size)

    # Prepare parameters for Jetstream
    params = {}

    if compress:
        params["compress"] = True

    # Log user handles if provided
    if bluesky_user_handles:
        logger.info(
            f"Bluesky user handles specified: {', '.join(bluesky_user_handles)}"
        )
        logger.info("Note: Filtering by user handles is not yet implemented.")

    # Run the async event loop
    loop = asyncio.get_event_loop()
    try:
        logger.info(f"Connecting to Jetstream instance: {instance}")
        logger.info(f"Wanted collections: {', '.join(wanted_collections)}")
        if wanted_dids:
            logger.info(
                f"Wanted DIDs: {', '.join(wanted_dids[:5])}{'...' if len(wanted_dids) > 5 else ''}"
            )
        logger.info(f"Target record count: {num_records}, Max time: {max_time}s")
        logger.info(f"Queue name: {queue_name}, Batch size: {batch_size}")

        stats = loop.run_until_complete(
            connector.listen_until_count(
                instance=instance,
                wanted_collections=wanted_collections,
                target_count=num_records,
                max_time=max_time,
                cursor=cursor,
                wanted_dids=wanted_dids if wanted_dids else None,
            )
        )

        # Print results
        logger.info("\nIngestion complete!")
        logger.info(f"Records stored: {stats['records_stored']}")
        logger.info(f"Messages received: {stats['messages_received']}")
        logger.info(f"Collections seen: {', '.join(stats['collections'])}")
        logger.info(f"Time taken: {stats['total_time']:.2f} seconds")
        logger.info(f"Rate: {stats['records_per_second']:.2f} records/second")
        logger.info(f"Queue length: {stats['queue_length']} items")

        if stats.get("latest_cursor"):
            logger.info(f"\nLatest cursor: {stats['latest_cursor']}")
            logger.info(
                "Use this cursor value to resume from this point in a future run."
            )

    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    stream_bluesky()
