"""Shared feed data loading functionality for analytics system.

This module provides unified interfaces for loading feed data and mapping
users to posts used in feeds, eliminating code duplication and
ensuring consistent data handling patterns.
"""

from datetime import datetime

import pandas as pd

from lib.constants import timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
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

    for row in feeds_df.itertuples():
        user = row.user
        feed: list[dict] = load_feed_from_json_str(row.feed)
        post_uris: list[str] = [post["item"] for post in feed]

        if user not in users_to_posts:
            users_to_posts[user] = set()
        users_to_posts[user].update(post_uris)

    logger.info(
        f"Mapped {len(users_to_posts)} users to posts for partition date {partition_date}"
    )
    return users_to_posts


def get_feeds_per_user(
    valid_study_users_dids: set[str],
) -> dict[str, dict[str, set[str]]]:
    """Gets the posts used in feeds generated per user per day.

    Returns a map of something like:

    {
        "<did>": {
             "<date>": {"<uri_1>", "<uri_2>", ...}  # Set of URIs
        }
    }

    Where for each user DID + date, we get a deduplicated list of post URIs for
    posts used in feeds from that date.

    Note that this only returns did + date combinations that have feeds. This is
    important to consider for downstream analyses for combinations of date + user
    that may not have feeds. This is generally unlikely, and the only expected
    examples are:
    - Feeds from the 1st week, for users in Wave 2 (since they start in Week 2).
    - Feeds from the 9th week, for users in Wave 1 (since they end in Week 8).
    - Feeds from 2024-10-08 (there was an outage on this date and feed generation
    was broken).
    """
    query = (
        "SELECT bluesky_user_did, feed, feed_generation_timestamp FROM generated_feeds"
    )
    query_metadata = {
        "tables": [
            {
                "name": "generated_feeds",
                "columns": ["bluesky_user_did", "feed", "feed_generation_timestamp"],
            }
        ]
    }
    generated_feeds_df: pd.DataFrame = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata=query_metadata,
        start_partition_date=STUDY_START_DATE,
        end_partition_date=STUDY_END_DATE,
    )

    # filter for only the valid study users.
    generated_feeds_df = generated_feeds_df[
        generated_feeds_df["bluesky_user_did"].isin(valid_study_users_dids)
    ]

    feeds_per_user: dict[str, dict[str, set[str]]] = {}

    # collect URIs in a single pass across generated_feeds_df.
    for row in generated_feeds_df.itertuples():
        # get user DID and the date of the feed generation.
        user_did = row.bluesky_user_did
        timestamp = row.feed_generation_timestamp
        feed_generation_date = datetime.strptime(timestamp, timestamp_format).strftime(
            "%Y-%m-%d"
        )

        # load the feeds and the post URIs from the feed.
        feed_json = row.feed
        feed = load_feed_from_json_str(feed_json)
        if not isinstance(feed, list):
            logger.error(f"Feed data for user {user_did} is not a list: {type(feed)}")
            raise ValueError(f"Invalid feed structure for user {user_did}")
        post_uris = [post["item"] for post in feed]

        # start adding the post URIs to the user + date feed structure.
        if user_did not in feeds_per_user:
            feeds_per_user[user_did] = {}

        if feed_generation_date not in feeds_per_user[user_did]:
            feeds_per_user[user_did][feed_generation_date] = set()

        feeds_per_user[user_did][feed_generation_date].update(post_uris)

    return feeds_per_user


def get_all_post_uris_used_in_feeds(
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
) -> set[str]:
    """Gets the deduplicated list of all post URIs used in feeds.

    Returns:
        List of post URIs used in feeds.
    """
    all_post_uris: set[str] = set()
    for _, content_in_feeds_by_date in user_to_content_in_feeds.items():
        for _, post_uris in content_in_feeds_by_date.items():
            all_post_uris.update(post_uris)
    return all_post_uris
