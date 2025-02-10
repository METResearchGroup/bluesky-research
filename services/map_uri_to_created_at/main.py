"""Service for mapping URIs to their creation timestamps.

This service maintains a source of truth for when records were created by mapping
URIs to their creation timestamps. It processes data in partition date batches
for efficient memory management and data organization.
"""

from lib.log.logger import get_logger
from services.map_uri_to_created_at.helper import (
    map_uris_to_created_at_for_partition_dates,
)

logger = get_logger(__file__)


def map_uri_to_created_at(payload: dict):
    """Maps URIs to their creation timestamps based on the provided payload.

    Args:
        payload (dict): Configuration for mapping URIs. Expected format:
            {
                "start_date" (Optional[str]): Start date in YYYY-MM-DD format (inclusive)
                "end_date" (Optional[str]): End date in YYYY-MM-DD format (inclusive)
                "exclude_partition_dates" (Optional[list[str]]): List of dates to exclude
            }

    Example:
        payload = {
            "start_date": "2024-09-28",
            "end_date": "2025-12-01",
            "exclude_partition_dates": ["2024-10-08"]
        }
    """
    start_date = payload.get("start_date", "2024-09-28")
    end_date = payload.get("end_date", "2025-12-01")
    exclude_partition_dates = payload.get("exclude_partition_dates", ["2024-10-08"])

    logger.info(
        f"Mapping URIs to creation timestamps from {start_date} to {end_date}, "
        f"excluding dates: {exclude_partition_dates}"
    )

    map_uris_to_created_at_for_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    logger.info("Finished mapping URIs to creation timestamps.")


if __name__ == "__main__":
    payload = {}
    map_uri_to_created_at(payload)
