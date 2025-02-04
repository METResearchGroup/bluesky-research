from typing import Optional

from lib.helper import get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.preprocess.enrich_hydrate_posts import (
    enrich_hydrate_posts_for_partition_date,
)
from services.calculate_analytics.study_analytics.preprocess.link_feeds_to_user_session_logs import (
    link_feeds_to_user_session_logs_for_partition_date,
)
from services.calculate_analytics.study_analytics.preprocess.link_posts_to_feeds import (
    link_posts_to_feeds_for_partition_date,
)

logger = get_logger(__file__)


def preprocess_data_for_partition_date(partition_date: str) -> None:
    """Preprocessing data for a given partition date."""
    logger.info(f"Preprocessing data for partition date: {partition_date}")
    link_posts_to_feeds_for_partition_date(partition_date)
    enrich_hydrate_posts_for_partition_date(partition_date)
    link_feeds_to_user_session_logs_for_partition_date(partition_date)
    logger.info(f"Completed preprocess data for partition date: {partition_date}")


def main(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if not start_date:
        start_date = "2024-10-01"
    if not end_date:
        end_date = "2024-12-01"

    partition_dates = get_partition_dates(start_date, end_date)

    logger.info(
        f"Preprocessing data for {len(partition_dates)} partition dates between {start_date} and {end_date}"
    )
    for partition_date in partition_dates:
        preprocess_data_for_partition_date(partition_date)

    logger.info("Completed preprocess data for all partition dates")


if __name__ == "__main__":
    main()
