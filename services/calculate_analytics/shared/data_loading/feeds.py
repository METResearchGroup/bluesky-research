"""Shared feed data loading functionality for analytics system.

This module provides unified interfaces for loading feed data and mapping
users to posts used in feeds, eliminating code duplication and
ensuring consistent data handling patterns.
"""

import datetime
import json

import pandas as pd

from lib.constants import timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.shared.data_loading.posts import (
    get_posts_with_labels_for_partition_date,
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


def get_feed_posts_with_labels_per_user(partition_date: str) -> dict[str, pd.DataFrame]:
    """Get the hydrated posts (posts + labels) for a given partition date and
    map them to the users who posted them.
    """
    posts_df: pd.DataFrame = get_posts_with_labels_for_partition_date(
        partition_date=partition_date
    )
    users_to_posts: dict[str, set[str]] = map_users_to_posts_used_in_feeds(
        partition_date=partition_date
    )
    map_user_to_subset_df: dict[str, pd.DataFrame] = {}
    for user, posts in users_to_posts.items():
        subset_df = posts_df[posts_df["uri"].isin(posts)]
        map_user_to_subset_df[user] = subset_df
    return map_user_to_subset_df


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

    # JSON-load the feeds
    generated_feeds_df["feed"] = generated_feeds_df["feed"].apply(json.loads)

    # get the post URIs for each feed.
    generated_feeds_df["post_uris"] = generated_feeds_df["feed"].apply(
        lambda x: [post["item"] for post in x]
    )

    # get the date of the feed generation.
    generated_feeds_df["feed_generation_date"] = generated_feeds_df[
        "feed_generation_timestamp"
    ].apply(lambda x: datetime.strptime(x, timestamp_format).strftime("%Y-%m-%d"))

    feeds_per_user: dict[str, dict[str, set[str]]] = {}

    # iterate through each combination of user DID + date and get the deduplicated
    # list of URIs of the posts used in their feeds for that date.
    for (user_did, date), group in generated_feeds_df.groupby(
        ["bluesky_user_did", "feed_generation_date"]
    ):
        # initialize nested structure efficiently.
        if user_did not in feeds_per_user:
            feeds_per_user[user_did] = {}

        feeds_per_user[user_did][date] = set()

        # get the deduplicated list of URIs of the posts used in their feeds for that date.
        for uris_list in group["post_uris"]:
            feeds_per_user[user_did][date].update(uris_list)

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
