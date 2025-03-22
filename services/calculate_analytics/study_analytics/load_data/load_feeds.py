"""Loads the feeds for a given partition date."""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.fetch_posts_used_in_feeds.helper import load_feed_from_json_str

logger = get_logger(__file__)


def get_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Get the feeds generated for a particular partition date."""
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
    """Map users to the posts used in their feeds for a given partition
    date.
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
    return users_to_posts
