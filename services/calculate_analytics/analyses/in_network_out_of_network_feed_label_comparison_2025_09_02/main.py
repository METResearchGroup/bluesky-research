"""Compare content labels between in-network and out-of-network posts used in feeds.

This analysis compares the content characteristics (labels) of posts that are
in-network versus out-of-network for each user, per day and per week.

Usage:
    python main.py --network_type in_network
    python main.py --network_type out_of_network
"""

import argparse
import os
from datetime import datetime

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
    get_daily_feed_content_per_user_metrics,
    get_weekly_content_per_user_metrics,
    transform_daily_content_per_user_metrics,
    transform_weekly_content_per_user_metrics,
)
from services.calculate_analytics.shared.data_loading.feeds import get_feeds_per_user
from services.calculate_analytics.shared.data_loading.labels import (
    get_all_labels_for_posts,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data
from services.fetch_posts_used_in_feeds.helper import load_feed_from_json_str

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()

logger = get_logger(__file__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare content labels between in-network and out-of-network posts"
    )
    parser.add_argument(
        "--network_type",
        type=str,
        required=True,
        choices=["in_network", "out_of_network"],
        help="Type of network posts to analyze: 'in_network' or 'out_of_network'",
    )
    return parser.parse_args()


def get_network_filtered_post_uris_per_user_per_day(
    valid_study_users_dids: set[str], network_type: str
) -> dict[str, dict[str, set[str]]]:
    """Get post URIs used in feeds filtered by network type (in-network or out-of-network).

    Args:
        valid_study_users_dids: Set of valid study user DIDs
        network_type: Either "in_network" or "out_of_network"

    Returns:
        Dictionary mapping user DIDs to dates to sets of post URIs
    """
    generated_feeds_df: pd.DataFrame = get_feeds_per_user(valid_study_users_dids)

    feeds_per_user: dict[str, dict[str, set[str]]] = {}
    is_in_network_filter = network_type == "in_network"

    # Collect URIs filtered by network type
    for row in generated_feeds_df.itertuples():
        user_did = row.bluesky_user_did
        timestamp = row.feed_generation_timestamp
        feed_generation_date = datetime.strptime(timestamp, timestamp_format).strftime(
            "%Y-%m-%d"
        )

        # Load the feed and extract post URIs based on network type
        feed_json = row.feed
        feed = load_feed_from_json_str(feed_json)

        if not isinstance(feed, list):
            logger.error(f"Feed data for user {user_did} is not a list: {type(feed)}")
            raise ValueError(f"Invalid feed structure for user {user_did}")

        # Filter posts by network type
        filtered_post_uris = []
        for post in feed:
            is_in_network = post.get("is_in_network", False)
            if is_in_network == is_in_network_filter:
                filtered_post_uris.append(post["item"])

        # Add to the structure
        if user_did not in feeds_per_user:
            feeds_per_user[user_did] = {}

        if feed_generation_date not in feeds_per_user[user_did]:
            feeds_per_user[user_did][feed_generation_date] = set()

        feeds_per_user[user_did][feed_generation_date].update(filtered_post_uris)

    logger.info(f"Filtered {network_type} posts for {len(feeds_per_user)} users")
    return feeds_per_user


def do_setup(network_type: str):
    """Setup steps for analysis."""

    # Load users and partition dates
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

    # Load feeds filtered by network type
    try:
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = (
            get_network_filtered_post_uris_per_user_per_day(
                valid_study_users_dids=valid_study_users_dids, network_type=network_type
            )
        )
    except Exception as e:
        logger.error(f"Failed to get {network_type} feeds per user: {e}")
        raise

    # Load labels for feed content
    try:
        from services.calculate_analytics.shared.data_loading.feeds import (
            get_all_post_uris_used_in_feeds,
        )

        feed_content_uris: set[str] = get_all_post_uris_used_in_feeds(
            user_to_content_in_feeds=user_to_content_in_feeds
        )
        labels_for_feed_content: dict[str, dict] = get_all_labels_for_posts(
            post_uris=feed_content_uris, partition_dates=partition_dates
        )
    except Exception as e:
        logger.error(f"Failed to get labels for {network_type} feed content: {e}")
        raise

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "valid_study_users_dids": valid_study_users_dids,
        "user_to_content_in_feeds": user_to_content_in_feeds,
        "labels_for_feed_content": labels_for_feed_content,
        "partition_dates": partition_dates,
    }


def do_analysis_and_export_results(
    user_df: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
    labels_for_feed_content: dict[str, dict],
    partition_dates: list[str],
    network_type: str,
):
    """Perform analysis and export results."""

    # Create timestamped output directory
    timestamp = generate_current_datetime_str()
    output_dir = os.path.join(current_dir, "results", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    # Define output file paths
    daily_results_export_fp = os.path.join(
        output_dir,
        f"daily_{network_type}_feed_content_analysis_{timestamp}.csv",
    )
    weekly_results_export_fp = os.path.join(
        output_dir,
        f"weekly_{network_type}_feed_content_analysis_{timestamp}.csv",
    )

    # (1) Daily aggregations
    logger.info(
        f"[Daily analysis] Getting per-user, per-day {network_type} content label metrics..."
    )

    try:
        user_per_day_content_label_metrics: dict[
            str, dict[str, dict[str, float | None]]
        ] = get_daily_feed_content_per_user_metrics(
            user_to_content_in_feeds=user_to_content_in_feeds,
            labels_for_feed_content=labels_for_feed_content,
        )
    except Exception as e:
        logger.error(
            f"Failed to get daily {network_type} feed content per user metrics: {e}"
        )
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
        logger.error(
            f"Failed to transform daily {network_type} feed content per user metrics: {e}"
        )
        raise

    try:
        logger.info(
            f"[Daily analysis] Exporting per-user, per-day {network_type} content label metrics to {daily_results_export_fp}..."
        )
        transformed_per_user_per_day_content_label_metrics.to_csv(
            daily_results_export_fp, index=False
        )
        logger.info(
            f"[Daily analysis] Exported per-user, per-day {network_type} content label metrics to {daily_results_export_fp}..."
        )
    except Exception as e:
        logger.error(
            f"Failed to export daily {network_type} feed content per user metrics: {e}"
        )
        raise

    # (2) Weekly aggregations
    logger.info(
        f"[Weekly analysis] Getting per-user, per-week {network_type} content label metrics..."
    )

    try:
        user_per_week_content_label_metrics: dict[
            str, dict[str, dict[str, float | None]]
        ] = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics=user_per_day_content_label_metrics,
            user_date_to_week_df=user_date_to_week_df,
        )
    except Exception as e:
        logger.error(
            f"Failed to get weekly {network_type} feed content per user metrics: {e}"
        )
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
        logger.error(
            f"Failed to transform weekly {network_type} feed content per user metrics: {e}"
        )
        raise

    try:
        logger.info(
            f"[Weekly analysis] Exporting per-user, per-week {network_type} content label metrics to {weekly_results_export_fp}..."
        )
        transformed_per_user_per_week_feed_content_metrics.to_csv(
            weekly_results_export_fp, index=False
        )
        logger.info(
            f"[Weekly analysis] Exported per-user, per-week {network_type} content label metrics to {weekly_results_export_fp}..."
        )
    except Exception as e:
        logger.error(
            f"Failed to export weekly {network_type} feed content per user metrics: {e}"
        )
        raise

    logger.info(
        f"Analysis complete for {network_type} posts. Results saved in: {output_dir}"
    )


def main():
    """Execute the analysis comparing in-network vs out-of-network feed content labels."""
    args = parse_arguments()
    network_type = args.network_type

    logger.info(f"Starting {network_type} feed content analysis")

    try:
        setup_objs = do_setup(network_type)

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
        do_analysis_and_export_results(
            user_df=user_df,
            user_date_to_week_df=user_date_to_week_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
            labels_for_feed_content=labels_for_feed_content,
            partition_dates=partition_dates,
            network_type=network_type,
        )
    except Exception as e:
        logger.error(f"Failed to do analysis and export results: {e}")
        raise

    logger.info(f"{network_type} feed content analysis completed successfully")


if __name__ == "__main__":
    main()
