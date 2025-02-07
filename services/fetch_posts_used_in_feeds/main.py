"""Service for fetching posts used in feeds."""

from lib.log.logger import get_logger
from services.fetch_posts_used_in_feeds.helper import (
    get_and_export_posts_used_in_feeds_for_partition_dates,
)

logger = get_logger(__file__)


def fetch_posts_used_in_feeds(payload: dict):
    """Fetches posts used in feeds based on the provided payload.

    Args:
        payload (dict): Configuration for fetching posts. Expected format:
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
        f"Fetching posts used in feeds from {start_date} to {end_date}, "
        f"excluding dates: {exclude_partition_dates}"
    )

    get_and_export_posts_used_in_feeds_for_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    logger.info("Finished fetching posts used in feeds.")


if __name__ == "__main__":
    payload = {}
    fetch_posts_used_in_feeds(payload)
