"""Datetime utility functions for common datetime operations."""

from datetime import timedelta
from enum import Enum

from lib.constants import (
    convert_pipeline_to_bsky_dt_format,
    current_datetime,
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
