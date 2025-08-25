"""Shared feed data loading functionality for analytics system.

This module provides unified interfaces for loading feed data and mapping
users to posts used in feeds, eliminating code duplication and
ensuring consistent data handling patterns.
"""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.fetch_posts_used_in_feeds.helper import load_feed_from_json_str

logger = get_logger(__file__)


def get_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Get the feeds generated for a particular partition date.

    Args:
        partition_date: The partition date to load feeds for

    Returns:
        DataFrame containing feeds for the partition date
    """
    feeds_df: pd.DataFrame = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        partition_date=partition_date,
    )
    logger.info(f"Loaded {len(feeds_df)} feeds for partition date {partition_date}")
    return feeds_df


def map_users_to_posts_used_in_feeds(
    partition_date: str,
) -> dict[str, set[str]]:
    """Map users to the posts used in their feeds for a given partition date.

    This function loads feeds for a partition date and creates a mapping
    from user DIDs to the set of post URIs that were used in their feeds.

    Args:
        partition_date: The partition date to load feeds for

    Returns:
        Dictionary mapping user DIDs to sets of post URIs used in their feeds
    """
    feeds_df: pd.DataFrame = get_feeds_for_partition_date(partition_date)
    users_to_posts: dict[str, set[str]] = {}

    for _, row in feeds_df.iterrows():
        user = row["user"]
        feed: list[dict] = load_feed_from_json_str(row["feed"])
        post_uris: list[str] = [post["item"] for post in feed]

        if user not in users_to_posts:
            users_to_posts[user] = set()
        users_to_posts[user].update(post_uris)

    logger.info(
        f"Mapped {len(users_to_posts)} users to posts for partition date {partition_date}"
    )
    return users_to_posts


def get_feeds_with_post_mapping(
    partition_date: str,
) -> tuple[pd.DataFrame, dict[str, set[str]]]:
    """Get feeds and user-to-posts mapping for a partition date.

    This is a convenience function that loads both feeds and creates the
    user-to-posts mapping in a single call.

    Args:
        partition_date: The partition date to load feeds for

    Returns:
        Tuple of (feeds DataFrame, user-to-posts mapping dictionary)
    """
    feeds_df = get_feeds_for_partition_date(partition_date)
    users_to_posts = map_users_to_posts_used_in_feeds(partition_date)

    return feeds_df, users_to_posts
