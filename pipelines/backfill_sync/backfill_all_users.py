#!/usr/bin/env python
"""Script to backfill data for all users in the system.

This script loads all users from the participant data service, extracts their
DIDs, and runs the backfill sync process in configurable chunks.
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from typing import Optional, Any

from lib.constants import timestamp_format
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.backfill.backfill_sync import run_backfill_sync
from services.participant_data.helper import get_all_users
from services.sync.jetstream.constants import VALID_COLLECTIONS

# Default timestamp for backfill (September 28th, 2024, at 12am)
DEFAULT_START_TIMESTAMP = "2024-09-28-00:00:00"

logger = get_logger(__name__)


def chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    """Split a list into chunks of specified size.

    Args:
        items: List of items to chunk
        chunk_size: Maximum size of each chunk

    Returns:
        List of chunks, where each chunk is a list
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def backfill_all_users(
    chunk_size: int = 100,
    start_timestamp: Optional[str] = DEFAULT_START_TIMESTAMP,
    end_timestamp: Optional[str] = None,
    wanted_collections: Optional[list[str]] = None,
    num_records: int = 10000,
    max_time: int = 900,
    queue_name: str = "jetstream_sync",
    batch_size: int = 100,
    compress: bool = False,
    output_dir: str = ".",
    dry_run: bool = False,
) -> str:
    """Backfill data for all users in the system.

    Args:
        chunk_size: Number of DIDs to process in each backfill run
        start_timestamp: Optional timestamp to start from (YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS)
                        Defaults to September 28th, 2024, at 12am
        end_timestamp: Optional timestamp to stop at (YYYY-MM-DD or YYYY-MM-DD-HH:MM:SS)
        wanted_collections: Optional list of collection types to include
        num_records: Number of records to collect per backfill run
        max_time: Maximum time to run each backfill in seconds
        queue_name: Name for the queue
        batch_size: Number of records to batch together for queue insertion
        compress: Whether to use zstd compression for the stream
        output_dir: Directory to save the output CSV file
        dry_run: If True, don't actually run backfill, just print commands

    Returns:
        Path to the output CSV file
    """
    # Set default collections if None
    if wanted_collections is None:
        wanted_collections = ["app.bsky.feed.post"]

    # Load all users
    logger.info("Loading all users from the system...")
    users = get_all_users()
    logger.info(f"Loaded {len(users)} users")

    # Extract DIDs
    user_dids = [user.bluesky_user_did for user in users]
    logger.info(f"Extracted {len(user_dids)} DIDs")

    # Create chunks
    did_chunks = chunk_list(user_dids, chunk_size)
    logger.info(f"Split into {len(did_chunks)} chunks of size {chunk_size}")

    # Create output CSV file
    timestamp = generate_current_datetime_str()
    csv_filename = f"backfill_sync_commands_{timestamp}.csv"
    csv_path = os.path.join(output_dir, csv_filename)

    logger.info(f"Writing commands to {csv_path}")

    # Set up CSV headers
    fieldnames = [
        "chunk_id",
        "start_time",
        "end_time",
        "duration_seconds",
        "did_count",
        "dids",
        "collections",
        "start_timestamp",
        "end_timestamp",
        "num_records",
        "max_time",
        "queue_name",
        "records_stored",
        "latest_cursor",
        "current_date",
        "end_cursor_reached",
        "status",
    ]

    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Process each chunk
        for i, did_chunk in enumerate(did_chunks):
            chunk_id = i + 1
            logger.info(
                f"Processing chunk {chunk_id}/{len(did_chunks)} ({len(did_chunk)} DIDs)"
            )

            # Prepare CSV row
            row = {
                "chunk_id": chunk_id,
                "start_time": datetime.now().strftime(timestamp_format),
                "did_count": len(did_chunk),
                "dids": ",".join(did_chunk[:5]) + ("..." if len(did_chunk) > 5 else ""),
                "collections": ",".join(wanted_collections),
                "start_timestamp": start_timestamp or "",
                "end_timestamp": end_timestamp or "",
                "num_records": num_records,
                "max_time": max_time,
                "queue_name": queue_name,
            }

            if dry_run:
                logger.info("DRY RUN: Would execute backfill sync with:")
                logger.info(f"  DIDs: {len(did_chunk)} DIDs")
                logger.info(f"  Collections: {wanted_collections}")
                logger.info(f"  Start timestamp: {start_timestamp}")
                if end_timestamp:
                    logger.info(f"  End timestamp: {end_timestamp}")
                logger.info(f"  Num records: {num_records}")
                logger.info(f"  Max time: {max_time}s")
                row["status"] = "DRY_RUN"
                row["end_time"] = row["start_time"]
                row["duration_seconds"] = 0
                row["records_stored"] = 0
                row["latest_cursor"] = ""
                row["current_date"] = ""
                row["end_cursor_reached"] = False
            else:
                # Run backfill sync
                start_time = time.time()
                try:
                    stats = run_backfill_sync(
                        wanted_dids=did_chunk,
                        start_timestamp=start_timestamp,
                        end_timestamp=end_timestamp,
                        wanted_collections=wanted_collections,
                        num_records=num_records,
                        max_time=max_time,
                        queue_name=queue_name,
                        batch_size=batch_size,
                        compress=compress,
                    )

                    end_time = time.time()
                    duration = end_time - start_time

                    # Update CSV row with results
                    row["status"] = "SUCCESS"
                    row["records_stored"] = stats["records_stored"]
                    row["latest_cursor"] = stats.get("latest_cursor", "")
                    row["current_date"] = stats.get("current_date", "")
                    row["end_cursor_reached"] = stats.get("end_cursor_reached", False)

                    logger.info(
                        f"Stored {stats['records_stored']} records in {duration:.2f}s"
                    )

                    if stats.get("end_cursor_reached"):
                        logger.info("Stopped because end timestamp was reached.")

                except Exception as e:
                    end_time = time.time()
                    duration = end_time - start_time

                    # Update CSV row with error
                    row["status"] = f"ERROR: {str(e)}"
                    row["records_stored"] = 0
                    row["latest_cursor"] = ""
                    row["current_date"] = ""
                    row["end_cursor_reached"] = False
                    logger.error(f"Error processing chunk {chunk_id}: {e}")

                row["end_time"] = datetime.now().strftime(timestamp_format)
                row["duration_seconds"] = round(duration, 2)

            # Write row to CSV
            writer.writerow(row)
            csvfile.flush()  # Ensure data is written to disk after each chunk

    logger.info(f"Completed processing {len(did_chunks)} chunks")
    logger.info(f"Results saved to {csv_path}")
    return csv_path


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Backfill data for all users in the system",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of DIDs to process in each backfill run",
    )
    parser.add_argument(
        "--start-timestamp",
        type=str,
        default=DEFAULT_START_TIMESTAMP,
        help=f"Start timestamp in YYYY-MM-DD or {timestamp_format} format",
    )
    parser.add_argument(
        "--end-timestamp",
        type=str,
        help="End timestamp (optional, will stop when reached)",
    )
    parser.add_argument(
        "--collections",
        type=str,
        default="app.bsky.feed.post",
        help=f"Comma-separated list of collections to include. Valid options: {', '.join(VALID_COLLECTIONS)}",
    )
    parser.add_argument(
        "--num-records",
        type=int,
        default=10000,
        help="Number of records to collect per backfill run",
    )
    parser.add_argument(
        "--max-time",
        type=int,
        default=900,
        help="Maximum time to run each backfill in seconds",
    )
    parser.add_argument(
        "--queue-name", type=str, default="jetstream_sync", help="Name for the queue"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of records to batch together for queue insertion",
    )
    parser.add_argument(
        "--compress", action="store_true", help="Use zstd compression for the stream"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directory to save the output CSV file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually run backfill, just print commands",
    )

    args = parser.parse_args()

    # Parse collections
    collections = [c.strip() for c in args.collections.split(",") if c.strip()]

    # Validate collections
    for collection in collections:
        if collection not in VALID_COLLECTIONS:
            sys.stderr.write(f"Error: Invalid collection '{collection}'\n")
            sys.stderr.write(f"Valid collections: {', '.join(VALID_COLLECTIONS)}\n")
            sys.exit(1)

    # Run backfill
    try:
        csv_path = backfill_all_users(
            chunk_size=args.chunk_size,
            start_timestamp=args.start_timestamp,
            end_timestamp=args.end_timestamp,
            wanted_collections=collections,
            num_records=args.num_records,
            max_time=args.max_time,
            queue_name=args.queue_name,
            batch_size=args.batch_size,
            compress=args.compress,
            output_dir=args.output_dir,
            dry_run=args.dry_run,
        )
        print(f"Success! Results saved to {csv_path}")

    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
