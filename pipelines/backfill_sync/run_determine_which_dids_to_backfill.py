"""
Script to run the determine_dids_to_backfill process from the backfill sync pipeline.

This script triggers the handler.py lambda_handler with the mode set to
'determine_dids_to_backfill', which will identify DIDs that need to be backfilled
based on interactions in the specified date range.
"""

import argparse
from typing import Optional

from lib.log.logger import get_logger
from pipelines.backfill_sync.handler import lambda_handler

logger = get_logger(__name__)


def main(start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    """
    Run the determine_dids_to_backfill process.

    Args:
        start_date: Optional start date in YYYY-MM-DD format. If not provided,
                   defaults to the value in services.backfill.sync.determine_dids_to_backfill.
        end_date: Optional end date in YYYY-MM-DD format. If not provided,
                 defaults to the value in services.backfill.sync.determine_dids_to_backfill.
    """
    logger.info("Starting determine_dids_to_backfill process")

    # Create the event payload
    event = {
        "mode": "determine_dids_to_backfill",
        "start_date": start_date,
        "end_date": end_date,
    }

    # Call the lambda handler
    result = lambda_handler(event, {})

    if result["status"] == "success":
        logger.info("Successfully determined DIDs to backfill")
    else:
        logger.error(
            f"Failed to determine DIDs to backfill: {result.get('message', 'Unknown error')}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Determine which DIDs need to be backfilled"
    )
    parser.add_argument(
        "--start-date", type=str, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument("--end-date", type=str, help="End date in YYYY-MM-DD format")

    args = parser.parse_args()
    main(start_date=args.start_date, end_date=args.end_date)
