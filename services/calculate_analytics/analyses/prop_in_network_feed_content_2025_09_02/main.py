"""Calculate the proportion of in-network feed content for each user, per day."""

import os
from datetime import datetime

import numpy as np
import pandas as pd

from lib.constants import timestamp_format
from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.analysis.content_analysis import (
    get_weekly_content_per_user_metrics,
    transform_daily_content_per_user_metrics,
    transform_weekly_content_per_user_metrics,
)
from services.calculate_analytics.shared.data_loading.feeds import get_feeds_per_user
from services.calculate_analytics.shared.data_loading.users import load_user_data
from services.fetch_posts_used_in_feeds.helper import load_feed_from_json_str

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()
in_network_daily_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"daily_in_network_feed_content_proportions_{current_datetime_str}.csv",
)
in_network_weekly_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"weekly_in_network_feed_content_proportions_{current_datetime_str}.csv",
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
            exclude_partition_dates=exclude_partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to load user data and/or partition dates: {e}")
        raise

    # load feeds per user
    try:
        generated_feeds_df: pd.DataFrame = get_feeds_per_user(valid_study_users_dids)
    except Exception as e:
        logger.error(f"Failed to get feeds per user: {e}")
        raise

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "valid_study_users_dids": valid_study_users_dids,
        "generated_feeds_df": generated_feeds_df,
        "partition_dates": partition_dates,
    }


def get_daily_in_network_metrics_per_user(
    generated_feeds_df: pd.DataFrame,
) -> dict[str, dict[str, dict[str, float | None]]]:
    """Calculate daily in-network metrics per user.

    Returns a structure compatible with content_analysis functions:
    {
        "<user_did>": {
            "<date>": {
                "feed_average_prop_in_network_posts": 0.165
            }
        }
    }
    """
    user_daily_metrics: dict[str, dict[str, dict[str, float | None]]] = {}

    for row in generated_feeds_df.itertuples():
        user_did = row.bluesky_user_did
        timestamp = row.feed_generation_timestamp
        feed_generation_date = datetime.strptime(timestamp, timestamp_format).strftime(
            "%Y-%m-%d"
        )

        # Load the feed and extract is_in_network values
        feed_json = row.feed
        feed = load_feed_from_json_str(feed_json)

        if not isinstance(feed, list):
            logger.error(f"Feed data for user {user_did} is not a list: {type(feed)}")
            raise ValueError(f"Invalid feed structure for user {user_did}")

        # Calculate proportion of in-network posts for this feed
        in_network_values = [post.get("is_in_network", False) for post in feed]
        if len(in_network_values) == 0:
            logger.warning(f"Empty feed for user {user_did} on {feed_generation_date}")
            continue

        proportion = sum(in_network_values) / len(in_network_values)

        # Add to the structure
        if user_did not in user_daily_metrics:
            user_daily_metrics[user_did] = {}

        if feed_generation_date not in user_daily_metrics[user_did]:
            user_daily_metrics[user_did][feed_generation_date] = {
                "feed_average_prop_in_network_posts": []
            }

        user_daily_metrics[user_did][feed_generation_date][
            "feed_average_prop_in_network_posts"
        ].append(proportion)

    # Convert lists to averages
    for user_did, date_metrics in user_daily_metrics.items():
        for date, metrics in date_metrics.items():
            proportions_list = metrics["feed_average_prop_in_network_posts"]
            clean_values = [p for p in proportions_list if p is not None]
            if clean_values:
                avg_proportion = round(np.mean(clean_values), 3)
                user_daily_metrics[user_did][date][
                    "feed_average_prop_in_network_posts"
                ] = avg_proportion
            else:
                user_daily_metrics[user_did][date][
                    "feed_average_prop_in_network_posts"
                ] = None

    logger.info(
        f"Calculated daily in-network metrics for {len(user_daily_metrics)} users"
    )
    return user_daily_metrics


def do_analysis_and_export_results(
    user_df: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
    generated_feeds_df: pd.DataFrame,
    partition_dates: list[str],
):
    """Perform analysis and export results."""

    # Calculate daily in-network metrics per user
    logger.info("Calculating daily in-network metrics per user...")
    try:
        user_daily_metrics = get_daily_in_network_metrics_per_user(generated_feeds_df)
    except Exception as e:
        logger.error(f"Failed to get daily in-network metrics per user: {e}")
        raise

    # Transform and export daily results using shared function
    logger.info("Transforming and exporting daily results...")
    try:
        daily_df = transform_daily_content_per_user_metrics(
            user_per_day_content_label_metrics=user_daily_metrics,
            users=user_df,
            partition_dates=partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to transform daily in-network metrics: {e}")
        raise

    try:
        logger.info(
            f"Exporting daily in-network metrics to {in_network_daily_results_export_fp}..."
        )
        os.makedirs(os.path.dirname(in_network_daily_results_export_fp), exist_ok=True)
        daily_df.to_csv(in_network_daily_results_export_fp, index=False)
        logger.info(
            f"Exported daily in-network metrics to {in_network_daily_results_export_fp}..."
        )
    except Exception as e:
        logger.error(f"Failed to export daily in-network metrics: {e}")
        raise

    # Calculate weekly metrics using shared function
    logger.info("Calculating weekly in-network metrics per user...")
    try:
        user_weekly_metrics = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics=user_daily_metrics,
            user_date_to_week_df=user_date_to_week_df,
        )
    except Exception as e:
        logger.error(f"Failed to get weekly in-network metrics per user: {e}")
        raise

    # Transform and export weekly results using shared function
    logger.info("Transforming and exporting weekly results...")
    try:
        weekly_df = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics=user_weekly_metrics,
            users=user_df,
            user_date_to_week_df=user_date_to_week_df,
        )
    except Exception as e:
        logger.error(f"Failed to transform weekly in-network metrics: {e}")
        raise

    try:
        logger.info(
            f"Exporting weekly in-network metrics to {in_network_weekly_results_export_fp}..."
        )
        os.makedirs(os.path.dirname(in_network_weekly_results_export_fp), exist_ok=True)
        weekly_df.to_csv(in_network_weekly_results_export_fp, index=False)
        logger.info(
            f"Exported weekly in-network metrics to {in_network_weekly_results_export_fp}..."
        )
    except Exception as e:
        logger.error(f"Failed to export weekly in-network metrics: {e}")
        raise


def main():
    """Execute the analysis of in-network feed content proportions."""
    try:
        setup_objs = do_setup()

        user_df: pd.DataFrame = setup_objs["user_df"]
        user_date_to_week_df: pd.DataFrame = setup_objs["user_date_to_week_df"]
        generated_feeds_df: pd.DataFrame = setup_objs["generated_feeds_df"]
        partition_dates: list[str] = setup_objs["partition_dates"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        do_analysis_and_export_results(
            user_df=user_df,
            user_date_to_week_df=user_date_to_week_df,
            generated_feeds_df=generated_feeds_df,
            partition_dates=partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to do analysis and export results: {e}")
        raise


if __name__ == "__main__":
    main()
