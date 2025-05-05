from datetime import datetime, timezone, timedelta

from typing import Literal, Optional

from lib.constants import timestamp_format
from lib.log.logger import get_logger

logger = get_logger(__name__)


def determine_backfill_latest_timestamp(
    backfill_duration: Optional[int] = None,
    backfill_period: Optional[Literal["days", "hours"]] = None,
) -> str:
    """Calculates the timestamp for backfilling data based on a duration and period.

    This function computes a historical timestamp by subtracting a specified duration
    from the current UTC time. The duration can be specified in either days or hours.

    Args:
        backfill_duration (int): The number of time units to look back. Must be a positive integer.
        backfill_period (Literal["days", "hours"]): The time unit for backfilling.
            Must be either "days" or "hours".

    Returns:
        str: A timestamp string in format YYYY-MM-DD-HH:MM:SS (from lib/constants.py timestamp_format)
            representing the calculated historical point in time, or None if invalid parameters
            are provided.

    Control Flow:
        1. Validates input parameters (backfill_duration not None and period is valid)
        2. Gets current UTC time
        3. If period is "days":
            a. Subtracts backfill_duration days from current time
        4. If period is "hours":
            a. Subtracts backfill_duration hours from current time
        5. If parameters were valid:
            a. Formats backfill time as timestamp string
            b. Returns formatted timestamp
        6. If parameters were invalid:
            a. Returns None
    """
    if backfill_duration is not None and backfill_period in ["days", "hours"]:
        current_time = datetime.now(timezone.utc)
        if backfill_period == "days":
            backfill_time = current_time - timedelta(days=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} days of data.")
        elif backfill_period == "hours":
            backfill_time = current_time - timedelta(hours=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} hours of data.")
    else:
        backfill_time = None
    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = None
    return timestamp
