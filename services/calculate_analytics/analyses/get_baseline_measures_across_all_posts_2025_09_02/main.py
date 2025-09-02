"""
Main script for calculating baseline measures across all labeled posts.

This script calculates label averages and proportions across ALL labeled posts
in the dataset, providing baseline measures without any user-specific filtering.
"""

import os

import pandas as pd

from lib.helper import (
    generate_current_datetime_str,
    get_partition_dates,
    create_date_to_week_mapping,
)
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_all_labels_for_posts,
)
from services.calculate_analytics.shared.analysis.content_analysis import (
    get_daily_baseline_content_metrics,
    get_weekly_baseline_content_metrics,
    transform_daily_content_per_user_metrics,
    transform_weekly_content_per_user_metrics,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()
baseline_daily_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"daily_baseline_content_label_metrics_{current_datetime_str}.csv",
)
baseline_weekly_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"weekly_baseline_content_label_metrics_{current_datetime_str}.csv",
)

logger = get_logger(__file__)


def create_baseline_week_dataframe(
    date_to_week_mapping: dict[str, int],
) -> pd.DataFrame:
    """Create a baseline week DataFrame for use with transform_weekly_content_per_user_metrics.

    Args:
        date_to_week_mapping: Dictionary mapping dates to week numbers

    Returns:
        DataFrame with columns ['bluesky_user_did', 'date', 'week'] for baseline user
    """
    records = []
    for date, week in date_to_week_mapping.items():
        records.append({"bluesky_user_did": "baseline", "date": date, "week": week})

    return pd.DataFrame(records)


def do_setup():
    """Setup steps for analysis. Includes:
    - 1. Getting partition dates for the study period
    - 2. Creating date-to-week mapping for baseline analysis
    """
    # get partition dates for the study period
    try:
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
        )
    except Exception as e:
        logger.error(f"Failed to get partition dates: {e}")
        raise

    # create date-to-week mapping for baseline analysis
    try:
        date_to_week_mapping = create_date_to_week_mapping(
            partition_dates=partition_dates,
            study_start_date=STUDY_START_DATE,
            study_end_date=STUDY_END_DATE,
        )
        logger.info(
            f"Created date-to-week mapping for {len(date_to_week_mapping)} dates"
        )
    except Exception as e:
        logger.error(f"Failed to create date-to-week mapping: {e}")
        raise

    return {
        "partition_dates": partition_dates,
        "date_to_week_mapping": date_to_week_mapping,
    }


def calculate_baseline_metrics_per_day(
    partition_dates: list[str],
) -> dict[str, dict[str, float | None]]:
    """Calculate baseline metrics for each day by loading labels day by day.

    This function loads labels for each partition date individually to manage memory
    usage, then calculates baseline metrics for that day's worth of labels.

    Args:
        partition_dates: List of partition dates to process

    Returns:
        Dictionary mapping partition dates to their baseline metrics
    """
    baseline_per_day_content_label_metrics: dict[str, dict[str, float | None]] = {}

    for partition_date in partition_dates:
        logger.info(f"[Baseline analysis] Processing partition date: {partition_date}")

        try:
            # Load all labels for this specific partition date
            # We pass None for post_uris and True for load_all_labels to load all labels
            labels_for_partition_date: dict[str, dict] = get_all_labels_for_posts(
                post_uris=None,
                partition_dates=[partition_date],
                load_all_labels=True,
                num_days_lookback=0,
            )

            logger.info(
                f"[Baseline analysis] Loaded {len(labels_for_partition_date)} labeled posts for {partition_date}"
            )

            if len(labels_for_partition_date) == 0:
                logger.warning(
                    f"[Baseline analysis] No labels found for {partition_date}"
                )
                baseline_per_day_content_label_metrics[partition_date] = {}
                continue

            # Calculate baseline metrics for this day's labels
            daily_baseline_metrics: dict[str, float | None] = (
                get_daily_baseline_content_metrics(
                    labels_for_content=labels_for_partition_date
                )
            )

            baseline_per_day_content_label_metrics[partition_date] = (
                daily_baseline_metrics
            )

            logger.info(
                f"[Baseline analysis] Calculated {len(daily_baseline_metrics)} baseline metrics for {partition_date}"
            )

            # Clean up memory
            del labels_for_partition_date
            del daily_baseline_metrics

        except Exception as e:
            logger.error(f"[Baseline analysis] Failed to process {partition_date}: {e}")
            # Continue with other dates even if one fails
            baseline_per_day_content_label_metrics[partition_date] = {}
            continue

    return baseline_per_day_content_label_metrics


def do_aggregations_and_export_results(
    baseline_per_day_content_label_metrics: dict[str, dict[str, float | None]],
    date_to_week_mapping: dict[str, int],
    partition_dates: list[str],
):
    """Perform aggregated analyses and export results.

    Aggregations are done at two levels:
    - (1) daily
    - (2) weekly.
    """

    # (1) Daily aggregations - transform to DataFrame format
    logger.info("[Daily analysis] Transforming daily baseline content label metrics...")

    try:
        # Create baseline user structure for the transform function
        baseline_user_df = pd.DataFrame(
            {
                "bluesky_user_did": ["baseline"],
                "bluesky_handle": ["baseline"],
                "condition": ["baseline"],
            }
        )

        # Use existing transformation function with baseline user structure
        daily_baseline_df = transform_daily_content_per_user_metrics(
            user_per_day_content_label_metrics={
                "baseline": baseline_per_day_content_label_metrics
            },
            users=baseline_user_df,
            partition_dates=partition_dates,
        )

        logger.info(
            f"[Daily analysis] Exporting daily baseline content label metrics to {baseline_daily_results_export_fp}..."
        )
        os.makedirs(
            os.path.dirname(baseline_daily_results_export_fp),
            exist_ok=True,
        )
        daily_baseline_df.to_csv(baseline_daily_results_export_fp, index=False)
        logger.info(
            f"[Daily analysis] Exported daily baseline content label metrics to {baseline_daily_results_export_fp}..."
        )

    except Exception as e:
        logger.error(f"Failed to export daily baseline content label metrics: {e}")
        raise

    # (2) Weekly aggregations
    logger.info("[Weekly analysis] Getting weekly baseline content label metrics...")

    try:
        # Calculate weekly baseline metrics using the new helper function
        weekly_baseline_metrics: dict[int, dict[str, float | None]] = (
            get_weekly_baseline_content_metrics(
                baseline_per_day_content_label_metrics=baseline_per_day_content_label_metrics,
                date_to_week_mapping=date_to_week_mapping,
            )
        )

        # Create baseline user structure and week DataFrame for transformation
        baseline_user_df = pd.DataFrame(
            {
                "bluesky_user_did": ["baseline"],
                "bluesky_handle": ["baseline"],
                "condition": ["baseline"],
            }
        )
        baseline_week_df = create_baseline_week_dataframe(date_to_week_mapping)

        # Use existing transformation function with baseline user structure
        weekly_baseline_df = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics={"baseline": weekly_baseline_metrics},
            users=baseline_user_df,
            user_date_to_week_df=baseline_week_df,
        )

        logger.info(
            f"[Weekly analysis] Exporting weekly baseline content label metrics to {baseline_weekly_results_export_fp}..."
        )
        os.makedirs(
            os.path.dirname(baseline_weekly_results_export_fp),
            exist_ok=True,
        )
        weekly_baseline_df.to_csv(baseline_weekly_results_export_fp, index=False)
        logger.info(
            f"[Weekly analysis] Exported weekly baseline content label metrics to {baseline_weekly_results_export_fp}..."
        )

    except Exception as e:
        logger.error(f"Failed to export weekly baseline content label metrics: {e}")
        raise


def main():
    """Execute the steps required for doing baseline analysis of all labeled posts
    during the study, both at the daily and weekly aggregation levels."""

    try:
        setup_objs = do_setup()

        partition_dates = setup_objs["partition_dates"]
        date_to_week_mapping = setup_objs["date_to_week_mapping"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        # Calculate baseline metrics day by day
        baseline_per_day_content_label_metrics = calculate_baseline_metrics_per_day(
            partition_dates=partition_dates
        )
    except Exception as e:
        logger.error(f"Failed to calculate baseline metrics per day: {e}")
        raise

    try:
        do_aggregations_and_export_results(
            baseline_per_day_content_label_metrics=baseline_per_day_content_label_metrics,
            date_to_week_mapping=date_to_week_mapping,
            partition_dates=partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to do aggregations and export results: {e}")
        raise


if __name__ == "__main__":
    main()
