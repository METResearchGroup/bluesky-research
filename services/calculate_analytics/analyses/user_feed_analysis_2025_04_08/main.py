"""Calculate per-user, per-feed aggregate analyses.

Refactored and migrated version of condition_aggregated.py
"""

import os

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data
from services.calculate_analytics.shared.transformations.aggregation import (
    daily_feed_content_aggregation,
    weekly_feed_content_aggregation,
)

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

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "valid_study_users_dids": valid_study_users_dids,
        "partition_dates": partition_dates,
    }


def do_aggregations_and_export_results():
    # (1) Daily aggregations
    print("[Daily analysis] Getting per-user, per-day feed analysis...")

    def transform_daily_level_feed_content_metrics():
        pass

    daily_level_feed_content_metrics_df: pd.DataFrame = daily_feed_content_aggregation()
    transformed_daily_level_feed_content_metrics_df: pd.DataFrame = (
        transform_daily_level_feed_content_metrics(daily_level_feed_content_metrics_df)
    )
    print(
        f"[Daily analysis] Exporting per-user, per-day feed analysis to {feed_content_daily_aggregated_results_export_fp}..."
    )
    os.makedirs(
        os.path.dirname(feed_content_daily_aggregated_results_export_fp),
        exist_ok=True,
    )
    transformed_daily_level_feed_content_metrics_df.to_csv(
        feed_content_daily_aggregated_results_export_fp, index=False
    )
    print(
        f"[Daily analysis] Exported per-user, per-day feed analysis to {feed_content_daily_aggregated_results_export_fp}..."
    )

    # (2) Weekly aggregations
    print("[Weekly analysis] Getting per-user, per-week feed analysis...")

    def transform_weekly_level_feed_content_metrics():
        pass

    weekly_level_feed_content_metrics_df: pd.DataFrame = (
        weekly_feed_content_aggregation()
    )
    transformed_weekly_level_feed_content_metrics_df: pd.DataFrame = (
        transform_weekly_level_feed_content_metrics(
            weekly_level_feed_content_metrics_df
        )
    )
    print(
        f"[Weekly analysis] Exporting per-user, per-week feed analysis to {feed_content_weekly_aggregated_results_export_fp}..."
    )
    os.makedirs(
        os.path.dirname(feed_content_weekly_aggregated_results_export_fp),
        exist_ok=True,
    )
    transformed_weekly_level_feed_content_metrics_df.to_csv(
        feed_content_weekly_aggregated_results_export_fp, index=False
    )
    print(
        f"[Weekly analysis] Exported per-user, per-week feed analysis to {feed_content_weekly_aggregated_results_export_fp}..."
    )


def main():
    setup_objs = do_setup()
    # TODO: delete, this is for just ruff linter.
    print(f"Setup complete. Partition dates: {setup_objs['partition_dates']}")
    do_aggregations_and_export_results()


if __name__ == "__main__":
    main()
