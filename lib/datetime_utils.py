"""Datetime utility functions for common datetime operations.

This module is intended to hold time/date utilities that were historically
implemented in `lib/helper.py`. Keeping datetime logic here avoids importing the
heavy, side-effectful `lib.helper` module (which loads environment variables at
import time) from code paths that only need date math.
"""

from datetime import datetime, timedelta, timezone
from enum import Enum

from lib.constants import (
    bsky_timestamp_formats,
    current_datetime,
    default_bsky_timestamp_format,
    partition_date_format,
    timestamp_format,
)


class TimestampFormat(Enum):
    """Enum for timestamp format options."""

    PIPELINE = "pipeline"
    BLUESKY = "bluesky"


def calculate_lookback_datetime_str(
    lookback_days: int,
    format: TimestampFormat = TimestampFormat.PIPELINE,  # noqa: A002
) -> str:
    """Calculate lookback datetime string in the specified format.

    Args:
        lookback_days: Number of days to look back from current datetime
        format: Format to return the datetime string in (PIPELINE or BLUESKY)

    Returns:
        Datetime string in the specified format

    Examples:
        >>> calculate_lookback_datetime_str(2)
        '2024-01-15-13:45:30'  # pipeline format

        >>> calculate_lookback_datetime_str(2, TimestampFormat.BLUESKY)
        '2024-01-15T13:45:30'  # bluesky format
    """
    lookback_datetime = current_datetime - timedelta(days=lookback_days)
    lookback_datetime_str = lookback_datetime.strftime(timestamp_format)

    if format == TimestampFormat.BLUESKY:
        lookback_datetime_str = convert_pipeline_to_bsky_dt_format(
            lookback_datetime_str
        )

    return lookback_datetime_str


def convert_pipeline_to_bsky_dt_format(pipeline_dt: str) -> str:
    """Converts a pipeline datetime string to a Bluesky datetime string."""
    dt = datetime.strptime(pipeline_dt, timestamp_format)
    dt_formatted: str = dt.strftime(default_bsky_timestamp_format)
    return dt_formatted


def normalize_timestamp(timestamp: str) -> str:
    """Normalizes timestamps that use hour 24 instead of 00.

    Args:
        timestamp: A timestamp string that might contain hour 24

    Returns:
        Normalized timestamp with hour 00 of the next day
    """
    if "T24:" in timestamp:
        return timestamp.replace("T24:", "T00:")
    return timestamp


def try_default_ts_truncation(timestamp: str) -> str:
    """Truncates a timestamp to seconds precision.

    Args:
        timestamp: A timestamp string that starts with format "YYYY-MM-DDThh:mm:ss"
            but may have varying precision endings

    Returns:
        The timestamp truncated to seconds precision (YYYY-MM-DDThh:mm:ss)
    """
    return timestamp[:19]  # Truncate after seconds (19 chars: YYYY-MM-DDThh:mm:ss)


def convert_bsky_dt_to_pipeline_dt(bsky_dt: str) -> str:
    """Converts a Bluesky datetime string to a pipeline datetime string.

    There's been various timestamp formats used in Bluesky, so we try to convert
    the timestamp to a pipeline datetime string by trying each format.

    Timestamps are the worst...
    """
    bsky_dt = normalize_timestamp(bsky_dt)
    for bsky_timestamp_format in bsky_timestamp_formats:
        try:
            dt = datetime.strptime(bsky_dt, bsky_timestamp_format)
            dt_formatted: str = dt.strftime(timestamp_format)
            return dt_formatted
        except ValueError:
            continue
    default_ts_truncation = try_default_ts_truncation(bsky_dt)
    try:
        dt = datetime.strptime(default_ts_truncation, default_bsky_timestamp_format)
        dt_formatted: str = dt.strftime(timestamp_format)
        return dt_formatted
    except ValueError:
        raise ValueError(f"Invalid Bluesky datetime string: {bsky_dt}")


def generate_current_datetime_str() -> str:
    """Generate current UTC timestamp string in pipeline `timestamp_format`."""
    return datetime.now(timezone.utc).strftime(timestamp_format)


def get_partition_dates(
    start_date: str,
    end_date: str,
    exclude_partition_dates: list[str] | None = None,
) -> list[str]:
    """Returns a list of dates between start_date and end_date, inclusive.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        exclude_partition_dates: Optional list of YYYY-MM-DD dates to skip

    Returns:
        List of dates in YYYY-MM-DD format
    """
    if exclude_partition_dates is None:
        exclude_partition_dates = ["2024-10-08"]  # server crashed.
    partition_dates: list[str] = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_timestamp:
        date_str = current_date.strftime("%Y-%m-%d")
        if exclude_partition_dates and date_str in exclude_partition_dates:
            current_date += timedelta(days=1)
            continue
        partition_dates.append(date_str)
        current_date += timedelta(days=1)

    return partition_dates


def calculate_start_end_date_for_lookback(
    partition_date: str,
    num_days_lookback: int,
    min_lookback_date: str,
) -> tuple[str, str]:
    """Calculate the start and end date for the lookback period.

    The lookback period is the period of time before the partition date that
    we want to load posts from. The earliest that it can be is the
    min_lookback_date, and the latest that it can be is the partition date.
    """
    partition_dt = datetime.strptime(partition_date, partition_date_format)
    lookback_dt = partition_dt - timedelta(days=num_days_lookback)
    lookback_date = lookback_dt.strftime(partition_date_format)
    start_date = max(min_lookback_date, lookback_date)
    end_date = partition_date
    return start_date, end_date


def calculate_week_number_for_date(
    date: str,
    study_start_date: str,
    study_end_date: str,
) -> int:
    """Calculate the week number (1-9) for a given date based on study period.

    Args:
        date: Date in YYYY-MM-DD format
        study_start_date: Study start date in YYYY-MM-DD format
        study_end_date: Study end date in YYYY-MM-DD format

    Returns:
        Week number (1-9) for the given date

    Raises:
        ValueError: If date is outside the study period
    """
    date_dt = datetime.strptime(date, "%Y-%m-%d")
    start_dt = datetime.strptime(study_start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(study_end_date, "%Y-%m-%d")

    # Check if date is within study period
    if date_dt < start_dt or date_dt > end_dt:
        raise ValueError(
            f"Date {date} is outside study period ({study_start_date} to {study_end_date})"
        )

    # Calculate days since study start
    days_since_start = (date_dt - start_dt).days

    # Calculate week number (1-based, 7 days per week)
    week_number = (days_since_start // 7) + 1

    # Ensure week number is within valid range (1-9)
    if week_number < 1 or week_number > 9:
        raise ValueError(
            f"Calculated week number {week_number} is outside valid range (1-9)"
        )

    return week_number


def create_date_to_week_mapping(
    partition_dates: list[str],
    study_start_date: str,
    study_end_date: str,
) -> dict[str, int]:
    """Create a mapping from dates to week numbers for baseline analysis.

    Args:
        partition_dates: List of dates in YYYY-MM-DD format
        study_start_date: Study start date in YYYY-MM-DD format
        study_end_date: Study end date in YYYY-MM-DD format

    Returns:
        Dictionary mapping dates to week numbers
    """
    date_to_week: dict[str, int] = {}
    for date in partition_dates:
        try:
            week_number = calculate_week_number_for_date(
                date=date,
                study_start_date=study_start_date,
                study_end_date=study_end_date,
            )
            date_to_week[date] = week_number
        except ValueError:
            # Skip dates outside study period
            continue

    return date_to_week
