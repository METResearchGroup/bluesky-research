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
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
)

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
        "--partition_date", required=True, help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--num_days_lookback",
        type=int,
        default=7,
        help="Number of days to look back (default: 7)",
    )
    parser.add_argument(
        "--min_lookback_date",
        default="2024-09-28",
        help="Minimum date to look back to (default: 2024-09-28)",
    )

    args = parser.parse_args()

    # Validate partition date format
    try:
        datetime.strptime(args.partition_date, "%Y-%m-%d")
    except ValueError:
        parser.error("partition_date must be in YYYY-MM-DD format")

    # Calculate date range
    start_date, _ = calculate_start_end_date_for_lookback(
        partition_date=args.partition_date,
        num_days_lookback=args.num_days_lookback,
        min_lookback_date=args.min_lookback_date,
    )

    # Generate list of dates to process
    dates = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.partition_date, "%Y-%m-%d")
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date = current_date.replace(day=current_date.day + 1)

    # Count records for each date
    counts = count_records_for_dates(args.integration, dates)

    # Print results
    print_record_counts(args.integration, counts, start_date, args.partition_date)


if __name__ == "__main__":
    main()
