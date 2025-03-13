#!/usr/bin/env python
"""BlueskyStream CLI - Connect to the Bluesky firehose through Jetstream.

This script provides a command-line interface to connect to the Bluesky firehose
via Jetstream, filter events by collection types and DIDs, and store records in a
local SQLite database.
"""

import asyncio
import click
import sys
from datetime import datetime
from typing import Optional

from demos.firehose_ingestion.constants import VALID_COLLECTIONS
from demos.firehose_ingestion.jetstream_connector import JetstreamConnector, PUBLIC_INSTANCES
from lib.log.logger import get_logger

logger = get_logger(__file__)

def timestamp_to_unix_microseconds(timestamp: str) -> int:
    """Convert a timestamp string to Unix microseconds.
    
    Args:
        timestamp: Timestamp string in YYYY-MM-DD format
    
    Returns:
        Unix microseconds since epoch
    """
    # Convert YYYY-MM-DD to datetime at start of day (00:00:00)
    dt = datetime.strptime(timestamp, "%Y-%m-%d")
    # Return microseconds (seconds * 1_000_000)
    return int(dt.timestamp() * 1_000_000)

def validate_timestamp(ctx, param, value):
    """Validate timestamp format (YYYY-MM-DD)."""
    if value is None:
        return None
    try:
        # Just validate the format, actual conversion happens later
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise click.BadParameter("Invalid date format. Please use YYYY-MM-DD format.")

def parse_handles(ctx, param, value):
    """Parse comma-separated list of handles into a list."""
    if value is None:
        return None
    return [handle.strip() for handle in value.split(",") if handle.strip()]

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
    callback=validate_timestamp,
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
    "--db-name",
    type=str,
    help="Name for the database file (default: auto-generated based on timestamp)",
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
    db_name: Optional[str],
    compress: bool,
    bluesky_user_handles: Optional[list[str]],
):
    """Stream data from the Bluesky firehose via Jetstream.

    This command connects to a Bluesky Jetstream instance, subscribes to specific
    collections and DIDs, and stores the resulting records in a SQLite database.

    Examples:
        # Stream 1000 posts from the firehose
        $ python -m demos.firehose_ingestion.jetstream_cli

        # Stream 500 likes and follows from a specific date
        $ python -m demos.firehose_ingestion.jetstream_cli -c app.bsky.feed.like -c app.bsky.graph.follow -n 500 -s 2024-01-01
        
        # Stream posts from specific DIDs
        $ python -m demos.firehose_ingestion.jetstream_cli -d did:plc:abcdef123456 -d did:plc:xyz789

        # Stream posts to a named database with compression
        $ python -m demos.firehose_ingestion.jetstream_cli --db-name my_posts --compress
        
        # Stream posts from specific Bluesky user handles
        $ python -m demos.firehose_ingestion.jetstream_cli -u alice.bsky.social,bob.bsky.social
    """
    # Convert start_timestamp to cursor if provided
    cursor = None
    if start_timestamp:
        cursor = str(timestamp_to_unix_microseconds(start_timestamp))
        logger.info(f"Using cursor: {cursor} (from timestamp: {start_timestamp})")

    # Initialize JetstreamConnector
    connector = JetstreamConnector(db_name=db_name, create_new_db=True)
    
    # Prepare parameters for Jetstream
    params = {}
    
    if compress:
        params["compress"] = True
    
    # Log user handles if provided
    if bluesky_user_handles:
        logger.info(f"Bluesky user handles specified: {', '.join(bluesky_user_handles)}")
        logger.info("Note: Filtering by user handles is not yet implemented.")
    
    # Run the async event loop
    loop = asyncio.get_event_loop()
    try:
        logger.info(f"Connecting to Jetstream instance: {instance}")
        logger.info(f"Wanted collections: {', '.join(wanted_collections)}")
        if wanted_dids:
            logger.info(f"Wanted DIDs: {', '.join(wanted_dids[:5])}{'...' if len(wanted_dids) > 5 else ''}")
        logger.info(f"Target record count: {num_records}, Max time: {max_time}s")
        
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
        logger.info(f"Database path: {connector.db.db_path}")
        
        if stats.get('latest_cursor'):
            logger.info(f"\nLatest cursor: {stats['latest_cursor']}")
            logger.info("Use this cursor value to resume from this point in a future run.")
        
    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    stream_bluesky() 