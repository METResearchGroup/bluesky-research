"""Calculate per-user, per-feed aggregate analyses.

Refactored and migrated version of condition_aggregated.py
"""

import os

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.shared.analysis.content_analysis import (
    get_daily_feed_content_per_user_metrics,
    get_weekly_content_per_user_metrics,
    transform_daily_content_per_user_metrics,
    transform_weekly_content_per_user_metrics,
)
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    get_all_post_uris_used_in_feeds,
    get_post_uris_used_in_feeds_per_user_per_day,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_all_labels_for_posts,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()
feed_content_daily_aggregated_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"daily_feed_content_aggregated_results_per_user_{current_datetime_str}.csv",
)
feed_content_weekly_aggregated_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"weekly_feed_content_aggregated_results_per_user_{current_datetime_str}.csv",
)

logger = get_logger(__file__)


def do_setup():
    """Setup steps for analysis."""

    # load users and partition dates.
    try:
        user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,  # feed generation was broken on this date.
        )
    except Exception as e:
        logger.error(f"Failed to load user data and/or partition dates: {e}")
        raise

    # load feeds: per-user, per-date, get list of URIs of posts in feeds.
    try:
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = (
            get_post_uris_used_in_feeds_per_user_per_day(
                valid_study_users_dids=valid_study_users_dids
            )
        )
    except Exception as e:
        logger.error(f"Failed to get feeds per user: {e}")
        raise

    # load labels for feed content
    try:
        feed_content_uris: set[str] = get_all_post_uris_used_in_feeds(
            user_to_content_in_feeds=user_to_content_in_feeds
        )
        labels_for_feed_content: dict[str, dict] = get_all_labels_for_posts(
            post_uris=feed_content_uris, partition_dates=partition_dates
        )
    except Exception as e:
        logger.error(f"Failed to get labels for feed content: {e}")
        raise

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "valid_study_users_dids": valid_study_users_dids,
        "user_to_content_in_feeds": user_to_content_in_feeds,
        "labels_for_feed_content": labels_for_feed_content,
        "partition_dates": partition_dates,
    }


def do_aggregations_and_export_results(
    user_df: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
    labels_for_feed_content: dict[str, dict],
    partition_dates: list[str],
):
    """Perform aggregated analyses and export results.

    Aggregations are done at two levels:
    - (1) daily
    - (2) weekly.
    """
    # (1) Daily aggregations
    logger.info("[Daily analysis] Getting per-user, per-day content label metrics...")

    try:
        user_per_day_content_label_metrics: dict[
            str, dict[str, dict[str, float | None]]
        ] = get_daily_feed_content_per_user_metrics(
            user_to_content_in_feeds=user_to_content_in_feeds,
            labels_for_feed_content=labels_for_feed_content,
        )
    except Exception as e:
        logger.error(f"Failed to get daily feed content per user metrics: {e}")
        raise

    try:
        transformed_per_user_per_day_content_label_metrics: pd.DataFrame = (
            transform_daily_content_per_user_metrics(
                user_per_day_content_label_metrics=user_per_day_content_label_metrics,
                users=user_df,
                partition_dates=partition_dates,
            )
        )
    except Exception as e:
        logger.error(f"Failed to transform daily feed content per user metrics: {e}")
        raise

    try:
        logger.info(
            f"[Daily analysis] Exporting per-user, per-day content label metrics to {feed_content_daily_aggregated_results_export_fp}..."
        )
        os.makedirs(
            os.path.dirname(feed_content_daily_aggregated_results_export_fp),
            exist_ok=True,
        )
        transformed_per_user_per_day_content_label_metrics.to_csv(
            feed_content_daily_aggregated_results_export_fp, index=False
        )
        logger.info(
            f"[Daily analysis] Exported per-user, per-day content label metrics to {feed_content_daily_aggregated_results_export_fp}..."
        )
    except Exception as e:
        logger.error(f"Failed to export daily feed content per user metrics: {e}")
        raise

    # (2) Weekly aggregations.
    logger.info("[Weekly analysis] Getting per-user, per-week content label metrics...")

    try:
        user_per_week_content_label_metrics: dict[
            str, dict[str, dict[str, float | None]]
        ] = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics=user_per_day_content_label_metrics,
            user_date_to_week_df=user_date_to_week_df,
        )
    except Exception as e:
        logger.error(f"Failed to get weekly feed content per user metrics: {e}")
        raise

    try:
        transformed_per_user_per_week_feed_content_metrics: pd.DataFrame = (
            transform_weekly_content_per_user_metrics(
                user_per_week_content_label_metrics=user_per_week_content_label_metrics,
                users=user_df,
                user_date_to_week_df=user_date_to_week_df,
            )
        )
    except Exception as e:
        logger.error(f"Failed to transform weekly feed content per user metrics: {e}")
        raise

    try:
        logger.info(
            f"[Weekly analysis] Exporting per-user, per-week content label metrics to {feed_content_weekly_aggregated_results_export_fp}..."
        )
        os.makedirs(
            os.path.dirname(feed_content_weekly_aggregated_results_export_fp),
            exist_ok=True,
        )
        transformed_per_user_per_week_feed_content_metrics.to_csv(
            feed_content_weekly_aggregated_results_export_fp, index=False
        )
        logger.info(
            f"[Weekly analysis] Exported per-user, per-week content label metrics to {feed_content_weekly_aggregated_results_export_fp}..."
        )
    except Exception as e:
        logger.error(f"Failed to export weekly feed content per user metrics: {e}")
        raise


def main():
    """Execute the steps required for doing analysis of the content that
    appeared in users' feeds during the study, both at the daily and weekly
    aggregation levels.
    """
    try:
        setup_objs = do_setup()

        user_df: pd.DataFrame = setup_objs["user_df"]
        user_date_to_week_df: pd.DataFrame = setup_objs["user_date_to_week_df"]
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = setup_objs[
            "user_to_content_in_feeds"
        ]
        labels_for_feed_content: dict[str, dict] = setup_objs["labels_for_feed_content"]
        partition_dates: list[str] = setup_objs["partition_dates"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        do_aggregations_and_export_results(
            user_df=user_df,
            user_date_to_week_df=user_date_to_week_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
            labels_for_feed_content=labels_for_feed_content,
            partition_dates=partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to do aggregations and export results: {e}")
        raise


if __name__ == "__main__":
    main()
