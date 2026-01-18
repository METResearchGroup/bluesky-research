"""Script to count records for a given integration across a date range.

This script loads and counts records for a specified integration across multiple
partition dates, helping track record count changes over time (e.g., before and
after cache writes to DuckDB).
"""

import argparse
from datetime import datetime
from typing import Dict, List

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from lib.datetime_utils import get_partition_dates

logger = get_logger(__file__)


def count_records_for_dates(
    integration: str,
    partition_dates: List[str],
) -> Dict[str, int]:
    """Count records for each partition date.

    Args:
        integration: Name of the integration
        partition_dates: List of partition dates to count records for

    Returns:
        Dictionary mapping partition dates to record counts
    """
    counts = {}
    for date in partition_dates:
        try:
            df = load_data_from_local_storage(
                service=integration,
                storage_tiers=["cache"],
                partition_date=date,
            )
            counts[date] = len(df) if df is not None else 0
        except Exception as e:
            logger.error(f"Error loading data for {integration} on {date}: {e}")
            counts[date] = 0
    return counts


def print_record_counts(
    integration: str,
    counts: Dict[str, int],
    start_date: str,
    end_date: str,
) -> None:
    """Print record counts in a formatted table.

    Args:
        integration: Name of the integration
        counts: Dictionary of dates to record counts
        start_date: Start date of the range
        end_date: End date of the range
    """
    print(
        f'\nNumber of records for the "{integration}" integration for the range "{start_date}" to "{end_date}":\n'
    )

    # Print counts in chronological order
    for date in sorted(counts.keys()):
        print(f"{date} {counts[date]}")


def main():
    parser = argparse.ArgumentParser(
        description="Count records for an integration across dates"
    )
    parser.add_argument(
        "--integration",
        required=True,
        help="Name of the integration to count records for",
    )
    parser.add_argument(
        "--start_date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    # Validate date formats
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        parser.error("dates must be in YYYY-MM-DD format")

    # Generate list of dates to process
    dates = get_partition_dates(start_date=args.start_date, end_date=args.end_date)

    # Count records for each date
    counts = count_records_for_dates(args.integration, dates)

    # Print results
    print_record_counts(args.integration, counts, args.start_date, args.end_date)

    total_records = sum(counts.values())
    print(
        f"\nTotal records for the integration {args.integration} from {args.start_date} to {args.end_date}: {total_records}"
    )


if __name__ == "__main__":
    main()
