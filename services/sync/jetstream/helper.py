"""Helper functions for the Jetstream connector module.

This module provides utility functions for working with the Bluesky Jetstream
connector, including timestamp conversions and data parsing utilities.
"""

from datetime import datetime
from typing import Optional, Any

import click

from lib.constants import timestamp_format


def timestamp_to_unix_microseconds(timestamp: str, format: Optional[str] = None) -> int:
    """Convert a timestamp string to Unix microseconds.

    Args:
        timestamp: Timestamp string in the format specified by format
        format: Optional timestamp format string
               If None, will try both YYYY-MM-DD and lib.constants.timestamp_format

    Returns:
        Unix microseconds since epoch

    Raises:
        ValueError: If the timestamp format is invalid
    """
    if not isinstance(timestamp, str):
        raise ValueError(f"Expected string timestamp, got {type(timestamp)}")

    # Special case for Unix epoch start (1970-01-01)
    if timestamp == "1970-01-01":
        return 0

    # If format is provided, use it
    if format is not None:
        try:
            dt = datetime.strptime(timestamp, format)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            raise ValueError(
                f"Invalid timestamp format: {timestamp}. Expected format: {format}"
            )

    # Try YYYY-MM-DD format first (most common)
    if len(timestamp) == 10 and timestamp[4] == "-" and timestamp[7] == "-":
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d")
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            # Let it fall through to the next format attempt
            pass

    # Try default timestamp format (YYYY-MM-DD-HH:MM:SS)
    try:
        dt = datetime.strptime(timestamp, timestamp_format)
        return int(dt.timestamp() * 1_000_000)
    except ValueError:
        # If both formats fail, provide a helpful error message
        raise ValueError(
            f"Invalid timestamp format: {timestamp}. Expected either YYYY-MM-DD or {timestamp_format}"
        )


def unix_microseconds_to_timestamp(unix_microseconds: int) -> str:
    """Convert Unix microseconds to a timestamp string.

    Args:
        unix_microseconds: Unix microseconds since epoch

    Returns:
        Timestamp string in the format YYYY-MM-DD-HH:MM:SS
    """
    return datetime.fromtimestamp(unix_microseconds / 1_000_000).strftime(
        timestamp_format
    )


def unix_microseconds_to_date(unix_microseconds: int) -> str:
    """Convert Unix microseconds to a date string.

    Args:
        unix_microseconds: Unix microseconds since epoch

    Returns:
        Date string in the format YYYY-MM-DD
    """
    return datetime.fromtimestamp(unix_microseconds / 1_000_000).strftime("%Y-%m-%d")


def validate_timestamp(
    ctx: Any, param: Any, value: Optional[str], format: Optional[str] = None
) -> Optional[str]:
    """Validate timestamp format.

    This function is designed to be used as a Click parameter callback
    to validate and parse timestamp strings.

    Args:
        ctx: Click context
        param: Click parameter
        value: The timestamp value to validate
        format: Optional timestamp format string
               If None, will try both YYYY-MM-DD and lib.constants.timestamp_format

    Returns:
        The validated timestamp value or None if input is None

    Raises:
        click.BadParameter: If the timestamp format is invalid
    """
    if value is None:
        return None

    # If format is provided, use it for validation
    if format is not None:
        try:
            datetime.strptime(value, format)
            return value
        except ValueError:
            raise click.BadParameter(
                f"Invalid date format: {value}. Expected format: {format}"
            )

    # Try YYYY-MM-DD format first
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return value
        except ValueError:
            # Fall through to try the next format
            pass

    # Try timestamp_format
    try:
        datetime.strptime(value, timestamp_format)
        return value
    except ValueError:
        # If both formats fail, provide a helpful error message
        raise click.BadParameter(
            f"Invalid timestamp format: {value}. Expected either YYYY-MM-DD or {timestamp_format}"
        )


def parse_handles(ctx: Any, param: Any, value: Optional[str]) -> Optional[list[str]]:
    """Parse comma-separated list of handles into a list.

    This function is designed to be used as a Click parameter callback
    to parse comma-separated Bluesky handles.

    Args:
        ctx: Click context
        param: Click parameter
        value: The comma-separated string of handles

    Returns:
        List of handles or None if input is None
    """
    if value is None:
        return None
    return [handle.strip() for handle in value.split(",") if handle.strip()]
