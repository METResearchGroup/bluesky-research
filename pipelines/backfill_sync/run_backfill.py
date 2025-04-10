"""
Script to run the backfill process from the backfill sync pipeline.

This script triggers the handler.py lambda_handler with the mode set to 'backfill',
which will synchronize data for DIDs loaded from the queue.
"""

import argparse

from lib.log.logger import get_logger
from pipelines.backfill_sync.handler import lambda_handler

logger = get_logger(__name__)


def main(skip_backfill: bool = False) -> None:
    """
    Run the backfill process.

    Args:
        skip_backfill: If True, skip the actual backfill process and only perform sync.
    """
    logger.info("Starting backfill process")

    # Create the event payload. Loads DIDs from queue.
    event = {
        "mode": "backfill",
        "dids": None,
        "skip_backfill": skip_backfill,
        "load_from_queue": True,
    }

    # Call the lambda handler
    result = lambda_handler(event, {})

    if result["status"] == "success":
        logger.info("Successfully completed backfill process")
    else:
        logger.error(
            f"Failed to complete backfill process: {result.get('message', 'Unknown error')}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run backfill for DIDs loaded from queue"
    )
    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Skip the actual backfill process and only write records from cache to DB.",
    )

    args = parser.parse_args()
    main(skip_backfill=args.skip_backfill)
