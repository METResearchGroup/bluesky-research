"""Aggregations."""

import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.shared.analysis.content_analysis import (
    calculate_content_label_metrics,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    get_feed_posts_with_labels_per_user,
)

logger = get_logger(__file__)


# TODO: NEEDS BOTH AI REVIEW AND HUMAN REVIEW.
def get_per_user_per_day_content_label_proportions(
    user_to_content_engaged_with: dict[str, dict],
    labels_for_engaged_content: dict[str, dict],
):
    pass


# could specify it better
def weekly_aggregation():
    pass


# TODO: filter user DIDs like I do for the engagement content logic.
def single_day_feed_content_aggregation(partition_date: str):
    map_user_to_posts_df: dict[str, pd.DataFrame] = get_feed_posts_with_labels_per_user(
        partition_date=partition_date
    )
    feed_content_metrics_per_user: list[dict] = []
    for user, posts_df in map_user_to_posts_df.items():
        feed_content_metrics: dict = calculate_content_label_metrics(posts_df=posts_df)
        feed_content_metrics["user"] = user
        feed_content_metrics["user_did"] = user
        feed_content_metrics["partition_date"] = partition_date
        feed_content_metrics_per_user.append(feed_content_metrics)

    feed_content_metrics_df: pd.DataFrame = pd.DataFrame(feed_content_metrics_per_user)
    feed_content_metrics_df = feed_content_metrics_df.set_index("user")
    logger.info(
        f"[Daily feed content analysis] Finished calculating feed content metrics for partition date {partition_date}"
    )
    return feed_content_metrics_df


# TODO: filter user DIDs like I do for the engagement content logic.
def daily_feed_content_aggregation(partition_dates: list[str]) -> pd.DataFrame:
    """Perform analysis of feed content, on a per-user, per-day basis."""
    feed_content_metrics_dfs: list[pd.DataFrame] = []
    for partition_date in partition_dates:
        feed_content_metrics_df = single_day_feed_content_aggregation(partition_date)
        feed_content_metrics_dfs.append(feed_content_metrics_df)

    daily_level_feed_content_metrics_df = pd.concat(feed_content_metrics_dfs)
    daily_level_feed_content_metrics_df = (
        daily_level_feed_content_metrics_df.sort_values(
            ["user", "partition_date"], ascending=[True, True]
        )
    )
    daily_level_feed_content_metrics_df.reset_index()
    logger.info(
        f"[Daily feed content analysis] Finished calculating feed content metrics for partition dates {partition_dates[0]} to {partition_dates[-1]}"
    )
    return daily_level_feed_content_metrics_df


# TODO: I thin I can structure this like I do for the engagement content weekly
# logic, as that is much cleaner compared to how I did it feed_analytics.py and
# condition_aggregated.py.
def weekly_feed_content_aggregation():
    """Perform analysis of feed content, on a per-user, per-week basis."""
    pass
