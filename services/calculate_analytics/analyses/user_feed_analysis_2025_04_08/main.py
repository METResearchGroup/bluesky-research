"""Calculate per-user, per-feed aggregate analyses.

Refactored and migrated version of condition_aggregated.py
"""

import os

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from services.calculate_analytics.shared.analysis.content_analysis import (
    get_daily_feed_content_per_user_metrics,
    get_weekly_feed_content_per_user_metrics,
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
    get_feeds_per_user,
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


def do_setup():
    """Setup steps for analysis."""

    # load users and partition dates.
    user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
    partition_dates: list[str] = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,  # feed generation was broken on this date.
    )

    # load feeds: per-user, per-date, get list of URIs of posts in feeds.
    user_to_content_in_feeds: dict[str, dict[str, set[str]]] = get_feeds_per_user(
        valid_study_users_dids=valid_study_users_dids
    )

    # load labels for feed content
    feed_content_uris: set[str] = get_all_post_uris_used_in_feeds(
        user_to_content_in_feeds=user_to_content_in_feeds
    )
    labels_for_feed_content: dict[str, dict] = get_all_labels_for_posts(
        post_uris=feed_content_uris, partition_dates=partition_dates
    )

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
    print("[Daily analysis] Getting per-user, per-day content label metrics...")

    user_per_day_content_label_metrics: dict[
        str, dict[str, dict[str, float | None]]
    ] = get_daily_feed_content_per_user_metrics(
        user_to_content_in_feeds=user_to_content_in_feeds,
        labels_for_feed_content=labels_for_feed_content,
    )

    transformed_per_user_per_day_content_label_metrics: pd.DataFrame = (
        transform_daily_content_per_user_metrics(
            user_per_day_content_label_metrics=user_per_day_content_label_metrics,
            users=user_df,
            partition_dates=partition_dates,
        )
    )

    print(
        f"[Daily analysis] Exporting per-user, per-day content label metrics to {feed_content_daily_aggregated_results_export_fp}..."
    )
    os.makedirs(
        os.path.dirname(feed_content_daily_aggregated_results_export_fp),
        exist_ok=True,
    )
    transformed_per_user_per_day_content_label_metrics.to_csv(
        feed_content_daily_aggregated_results_export_fp, index=False
    )
    print(
        f"[Daily analysis] Exported per-user, per-day content label metrics to {feed_content_daily_aggregated_results_export_fp}..."
    )

    # (2) Weekly aggregations.
    print("[Weekly analysis] Getting per-user, per-week content label metrics...")

    user_per_week_content_label_metrics: dict[
        str, dict[str, dict[str, float | None]]
    ] = get_weekly_feed_content_per_user_metrics(
        user_per_day_content_label_metrics=user_per_day_content_label_metrics,
        user_date_to_week_df=user_date_to_week_df,
    )

    transformed_per_user_per_week_feed_content_metrics: pd.DataFrame = (
        transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics=user_per_week_content_label_metrics,
            users=user_df,
            user_date_to_week_df=user_date_to_week_df,
        )
    )

    print(
        f"[Weekly analysis] Exporting per-user, per-week content label metrics to {feed_content_weekly_aggregated_results_export_fp}..."
    )
    os.makedirs(
        os.path.dirname(feed_content_weekly_aggregated_results_export_fp),
        exist_ok=True,
    )
    transformed_per_user_per_week_feed_content_metrics.to_csv(
        feed_content_weekly_aggregated_results_export_fp, index=False
    )
    print(
        f"[Weekly analysis] Exported per-user, per-week content label metrics to {feed_content_weekly_aggregated_results_export_fp}..."
    )


def main():
    """Execute the steps required for doing analysis of the content that
    appeared in users' feeds during the study, both at the daily and weekly
    aggregation levels.
    """
    setup_objs = do_setup()

    user_df: pd.DataFrame = setup_objs["user_df"]
    user_date_to_week_df: pd.DataFrame = setup_objs["user_date_to_week_df"]
    user_to_content_in_feeds: dict[str, dict[str, set[str]]] = setup_objs[
        "user_to_content_in_feeds"
    ]
    labels_for_feed_content: dict[str, dict] = setup_objs["labels_for_feed_content"]
    partition_dates: list[str] = setup_objs["partition_dates"]

    do_aggregations_and_export_results(
        user_df=user_df,
        user_date_to_week_df=user_date_to_week_df,
        user_to_content_in_feeds=user_to_content_in_feeds,
        labels_for_feed_content=labels_for_feed_content,
        partition_dates=partition_dates,
    )


if __name__ == "__main__":
    main()
