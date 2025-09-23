"""Data loading utilities for Hashtag Analysis.

This module provides data loading functions specifically for hashtag analysis,
leveraging the shared data loading infrastructure where possible.
"""

from typing import Dict, Set, List
import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.shared.data_loading.feeds import (
    get_post_uris_used_in_feeds_per_user_per_day,
    get_all_post_uris_used_in_feeds,
)
from services.calculate_analytics.shared.data_loading.posts import (
    load_preprocessed_posts_by_uris,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data

logger = get_logger(__file__)


def load_posts_used_in_feeds(
    user_to_content_in_feeds: Dict[str, Dict[str, Set[str]]],
    partition_dates: List[str],
) -> pd.DataFrame:
    """Load preprocessed posts data for all posts used in feeds.

    Args:
        user_to_content_in_feeds: Posts used in feeds per user per day
        partition_dates: List of partition dates to load posts for

    Returns:
        DataFrame containing preprocessed posts data
    """
    logger.info("Loading posts used in feeds...")

    # Get all unique post URIs used in feeds
    feed_content_uris: set[str] = get_all_post_uris_used_in_feeds(
        user_to_content_in_feeds=user_to_content_in_feeds
    )
    logger.info(f"Found {len(feed_content_uris)} unique posts used in feeds")

    # Load posts data for all partition dates
    posts_data_list = []
    for partition_date in partition_dates:
        try:
            posts_df = load_preprocessed_posts_by_uris(
                uris=feed_content_uris,
                partition_date=partition_date,
            )
            if not posts_df.empty:
                posts_data_list.append(posts_df)
                logger.info(
                    f"Loaded {len(posts_df)} posts for partition date {partition_date}"
                )
        except Exception as e:
            logger.warning(
                f"Failed to load posts for partition date {partition_date}: {e}"
            )
            continue

    if posts_data_list:
        posts_data = pd.concat(posts_data_list, ignore_index=True)
        posts_data = posts_data.drop_duplicates(subset=["uri"])
        logger.info(f"Loaded {len(posts_data)} unique posts total")
        return posts_data
    else:
        logger.error("No posts data loaded")
        raise ValueError("No posts data available")


def load_user_and_feed_data(
    valid_study_users_dids: Set[str],
) -> Dict[str, Dict[str, Set[str]]]:
    """Load user feed data mapping users to posts used in their feeds.

    Args:
        valid_study_users_dids: Set of valid study user DIDs

    Returns:
        Dictionary mapping user DIDs to dates to sets of post URIs
    """
    logger.info("Loading user and feed data...")

    try:
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = (
            get_post_uris_used_in_feeds_per_user_per_day(
                valid_study_users_dids=valid_study_users_dids
            )
        )
        logger.info(f"Loaded feeds for {len(user_to_content_in_feeds)} users")
        return user_to_content_in_feeds
    except Exception as e:
        logger.error(f"Failed to get feeds per user: {e}")
        raise


def load_all_required_data():
    """Load all required data for hashtag analysis.

    Returns:
        Dictionary containing all loaded data objects
    """
    logger.info("Loading all required data for hashtag analysis...")

    # Load users and partition dates
    try:
        user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
        logger.info(f"Loaded {len(valid_study_users_dids)} users")
    except Exception as e:
        logger.error(f"Failed to load user data: {e}")
        raise

    # Load feed data
    try:
        user_to_content_in_feeds = load_user_and_feed_data(valid_study_users_dids)
    except Exception as e:
        logger.error(f"Failed to load feed data: {e}")
        raise

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "valid_study_users_dids": valid_study_users_dids,
        "user_to_content_in_feeds": user_to_content_in_feeds,
    }
