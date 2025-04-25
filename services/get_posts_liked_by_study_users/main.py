"""Service for getting posts liked by study users.

This service coordinates the process of retrieving posts that were liked by study users.
It handles date range management and provides a payload-based interface for the data
retrieval process. The service uses a lookback window to ensure complete data capture
and supports date exclusion for data quality control.
"""

from lib.log.logger import get_logger
from services.get_posts_liked_by_study_users.helper import (
    get_and_export_liked_posts_for_partition_dates,
)

logger = get_logger(__file__)


def get_posts_liked_by_study_users(payload: dict):
    """Gets posts liked by study users based on the provided payload.

    Args:
        payload (dict): Configuration dictionary controlling the data retrieval process.
            {
                "start_date" (Optional[str]): Start date in YYYY-MM-DD format (inclusive).
                    Defaults to "2024-09-28".
                "end_date" (Optional[str]): End date in YYYY-MM-DD format (inclusive).
                    Defaults to "2025-12-01".
                "exclude_partition_dates" (Optional[list[str]]): List of dates to exclude
                    in YYYY-MM-DD format. Defaults to ["2024-10-08"].
            }

    Behavior:
        1. Extracts date parameters from payload, using defaults if not provided
        2. Logs the date range and exclusions being processed
        3. Calls helper function to process each partition date in the range
        4. Uses a lookback window for each partition date to ensure complete data
        5. Logs completion status

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
        f"Getting posts liked by study users from {start_date} to {end_date}, "
        f"excluding dates: {exclude_partition_dates}"
    )

    get_and_export_liked_posts_for_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    logger.info("Finished getting posts liked by study users.")


if __name__ == "__main__":
    payload = {}
    get_posts_liked_by_study_users(payload)
