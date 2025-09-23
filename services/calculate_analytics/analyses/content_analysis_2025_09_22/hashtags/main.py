"""Hashtag Analysis for Feed Content

This module extracts and analyzes hashtag usage patterns across experimental
conditions and time periods (pre/post election). It provides insights into
how hashtag adoption and popularity vary across different feed conditions
and temporal periods.

Usage:
    python main.py
"""

import os
import json
from typing import Dict, Set, List

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)

# Import our modular components
from load_data import load_posts_used_in_feeds, load_user_and_feed_data
from hashtag_analysis import (
    create_stratified_hashtag_analysis,
    create_hashtag_dataframe,
    get_hashtag_summary_stats,
    validate_hashtag_data,
    ELECTION_DATE,
    MIN_HASHTAG_FREQUENCY,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()

logger = get_logger(__file__)


def do_setup():
    """Setup steps for hashtag analysis.

    Returns:
        Dictionary containing loaded data objects:
        - user_df: User data with conditions
        - user_date_to_week_df: Date to week mapping
        - valid_study_users_dids: Set of valid user DIDs
        - user_to_content_in_feeds: Posts used in feeds per user per day
        - posts_data: Preprocessed posts data
        - partition_dates: List of partition dates
    """
    logger.info("Starting hashtag analysis setup...")

    # Load users and partition dates
    try:
        from services.calculate_analytics.shared.data_loading.users import (
            load_user_data,
        )

        user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )
        logger.info(
            f"Loaded {len(valid_study_users_dids)} users and {len(partition_dates)} partition dates"
        )
    except Exception as e:
        logger.error(f"Failed to load user data and/or partition dates: {e}")
        raise

    # Load feeds: per-user, per-date, get list of URIs of posts in feeds
    try:
        user_to_content_in_feeds = load_user_and_feed_data(valid_study_users_dids)
        logger.info(f"Loaded feeds for {len(user_to_content_in_feeds)} users")
    except Exception as e:
        logger.error(f"Failed to get feeds per user: {e}")
        raise

    # Load posts data for all posts used in feeds
    try:
        posts_data = load_posts_used_in_feeds(user_to_content_in_feeds, partition_dates)
        logger.info(f"Loaded {len(posts_data)} unique posts total")
    except Exception as e:
        logger.error(f"Failed to load posts data: {e}")
        raise

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "valid_study_users_dids": valid_study_users_dids,
        "user_to_content_in_feeds": user_to_content_in_feeds,
        "posts_data": posts_data,
        "partition_dates": partition_dates,
    }


def do_analysis_and_export_results(
    user_df: pd.DataFrame,
    user_to_content_in_feeds: Dict[str, Dict[str, Set[str]]],
    posts_data: pd.DataFrame,
    partition_dates: List[str],
):
    """Perform hashtag analysis and export results.

    Args:
        user_df: User data with conditions
        user_to_content_in_feeds: Posts used in feeds per user per day
        posts_data: Preprocessed posts data
        partition_dates: List of partition dates
    """
    logger.info("Starting hashtag analysis and export...")

    # Create timestamped output directory
    timestamp = generate_current_datetime_str()
    output_dir = os.path.join(current_dir, "results", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    # Perform stratified analysis
    stratified_results = create_stratified_hashtag_analysis(
        user_df=user_df,
        user_to_content_in_feeds=user_to_content_in_feeds,
        posts_data=posts_data,
    )

    # Create standardized DataFrame
    hashtag_df = create_hashtag_dataframe(stratified_results)

    # Validate data
    if not validate_hashtag_data(hashtag_df):
        logger.error("Hashtag data validation failed")
        raise ValueError("Invalid hashtag data")

    # Export main results
    main_output_path = os.path.join(output_dir, f"hashtag_analysis_{timestamp}.csv")
    hashtag_df.to_csv(main_output_path, index=False)
    logger.info(f"Exported main results to {main_output_path}")

    # Create pre-sliced CSV files for easy visualization loading
    # Overall analysis
    overall_df = hashtag_df.groupby(["hashtag"])["count"].sum().reset_index()
    overall_df["proportion"] = overall_df["count"] / overall_df["count"].sum()
    overall_output_path = os.path.join(output_dir, f"hashtag_overall_{timestamp}.csv")
    overall_df.to_csv(overall_output_path, index=False)

    # By condition
    for condition in hashtag_df["condition"].unique():
        condition_df = hashtag_df[hashtag_df["condition"] == condition]
        condition_output_path = os.path.join(
            output_dir, f"hashtag_condition_{condition}_{timestamp}.csv"
        )
        condition_df.to_csv(condition_output_path, index=False)

    # By election period
    for period in hashtag_df["pre_post_election"].unique():
        period_df = hashtag_df[hashtag_df["pre_post_election"] == period]
        period_output_path = os.path.join(
            output_dir, f"hashtag_period_{period}_{timestamp}.csv"
        )
        period_df.to_csv(period_output_path, index=False)

    # Create visualizations
    # Note: Visualizations are handled separately - not created in main.py

    # Create metadata file
    summary_stats = get_hashtag_summary_stats(hashtag_df)
    metadata = {
        "analysis_type": "hashtag_analysis",
        "timestamp": timestamp,
        "election_date": ELECTION_DATE,
        "min_hashtag_frequency": MIN_HASHTAG_FREQUENCY,
        **summary_stats,
    }

    metadata_path = os.path.join(
        output_dir, f"hashtag_analysis_metadata_{timestamp}.json"
    )
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Hashtag analysis complete. Results saved in: {output_dir}")


def main():
    """Execute the hashtag analysis."""
    logger.info("Starting hashtag analysis")

    try:
        setup_objs = do_setup()

        user_df: pd.DataFrame = setup_objs["user_df"]
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = setup_objs[
            "user_to_content_in_feeds"
        ]
        posts_data: pd.DataFrame = setup_objs["posts_data"]
        partition_dates: list[str] = setup_objs["partition_dates"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        do_analysis_and_export_results(
            user_df=user_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
            posts_data=posts_data,
            partition_dates=partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to do analysis and export results: {e}")
        raise

    logger.info("Hashtag analysis completed successfully")


if __name__ == "__main__":
    main()
