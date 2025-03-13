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
        format: Optional timestamp format string (default: uses lib.constants.timestamp_format)
               If None and timestamp is in YYYY-MM-DD format, it will be parsed accordingly

    Returns:
        Unix microseconds since epoch

    Raises:
        ValueError: If the timestamp format is invalid
    """
    if not isinstance(timestamp, str):
        raise ValueError(f"Expected string timestamp, got {type(timestamp)}")

    # If format is provided, use it
    if format is not None:
        try:
            dt = datetime.strptime(timestamp, format)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            raise ValueError(
                f"Invalid timestamp format: {timestamp}. Expected format: {format}"
            )

    # Special case for Unix epoch start (1970-01-01)
    if timestamp == "1970-01-01":
        return 0

    # If no format provided but looks like YYYY-MM-DD, use that format
    if len(timestamp) == 10 and timestamp[4] == "-" and timestamp[7] == "-":
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d")
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            raise ValueError(
                f"Invalid timestamp format: {timestamp}. Expected YYYY-MM-DD format."
            )

    # Otherwise, use the default timestamp format
    try:
        dt = datetime.strptime(timestamp, timestamp_format)
        return int(dt.timestamp() * 1_000_000)
    except ValueError:
        raise ValueError(
            f"Invalid timestamp format: {timestamp}. Expected format: {timestamp_format}"
        )


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
        format: Optional timestamp format string (default: YYYY-MM-DD)
               If None, validates against YYYY-MM-DD format

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

    # Default validation for YYYY-MM-DD format
    if (
        not isinstance(value, str)
        or len(value) != 10
        or value[4] != "-"
        or value[7] != "-"
    ):
        raise click.BadParameter(
            f"Invalid date format: {value}. Please use YYYY-MM-DD format."
        )

    try:
        # Just validate the format, actual conversion happens later
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise click.BadParameter(
            f"Invalid date format: {value}. Please use YYYY-MM-DD format."
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
