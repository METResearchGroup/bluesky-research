"""Datetime utility functions for common datetime operations."""

from datetime import datetime, timedelta
from enum import Enum

from lib.constants import (
    bsky_timestamp_formats,
    current_datetime,
    default_bsky_timestamp_format,
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
