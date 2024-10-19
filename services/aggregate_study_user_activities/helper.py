"""Helper service for fetching and processing all the study user activities
in one large table."""

from datetime import timedelta

import pandas as pd

from lib.constants import current_datetime
from lib.log.logger import get_logger


default_lookback_days = 1

logger = get_logger(__name__)


def generate_partition_dates(lookback_days: int = default_lookback_days) -> list[str]:
    """Generates the partition dates for the given lookback days."""
    partition_dates = []
    for i in range(lookback_days):
        partition_date = (current_datetime - timedelta(days=i)).strftime("%Y-%m-%d")
        partition_dates.append(partition_date)
    return partition_dates


def aggregate_latest_user_likes(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes for the given partition date."""
    pass


def aggregate_latest_user_follows(partition_date: str) -> pd.DataFrame:
    """Collects the latest user follows for the given partition date."""
    pass


def aggregate_latest_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user posts for the given partition date."""
    pass


def aggregate_latest_user_likes_on_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes on user posts for the given partition date."""
    pass


def aggregate_latest_user_reply_to_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user replies to user posts for the given partition date."""
    pass


def aggregate_latest_user_activities(partition_date: str) -> pd.DataFrame:
    """Collects the latest user activities for the given partition date."""
    latest_user_likes = aggregate_latest_user_likes(partition_date)
    latest_user_follows = aggregate_latest_user_follows(partition_date)
    latest_user_posts = aggregate_latest_user_posts(partition_date)
    latest_user_likes_on_user_posts = aggregate_latest_user_likes_on_user_posts(
        partition_date
    )
    latest_user_reply_to_user_posts = aggregate_latest_user_reply_to_user_posts(
        partition_date
    )
    latest_activities = pd.concat(
        [
            latest_user_likes,
            latest_user_follows,
            latest_user_posts,
            latest_user_likes_on_user_posts,
            latest_user_reply_to_user_posts,
        ]
    )
    return latest_activities


def export_latest_user_activities(
    activities_df: pd.DataFrame,
    partition_date: str,
) -> None:
    """Exports the latest user activities for the given partition date."""
    pass


def main():
    partition_dates: list[str] = generate_partition_dates(
        lookback_days=default_lookback_days
    )
    for partition_date in partition_dates:
        logger.info("*" * 10)
        logger.info(
            f"Aggregating latest user activities for partition date: {partition_date}"
        )
        latest_activities_df = aggregate_latest_user_activities(partition_date)
        logger.info(
            f"Finished aggregating latest user activities for partition date: {partition_date}. Exporting..."
        )
        export_latest_user_activities(
            activities_df=latest_activities_df,
            partition_date=partition_date,
        )
        logger.info(
            f"Finished exporting latest user activities for partition date: {partition_date}"
        )
        logger.info("*" * 10)


if __name__ == "__main__":
    main()
