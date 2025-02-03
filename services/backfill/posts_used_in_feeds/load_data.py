"""Load data for posts used in feeds backfill pipeline."""

from datetime import timedelta

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.backfill.posts.load_data import (
    INTEGRATIONS_LIST,
    load_preprocessed_posts,
    load_service_post_uris,
)

logger = get_logger(__file__)

num_days_lookback = 5
min_lookback_date = "2024-09-28"


def calculate_start_end_date_for_lookback(partition_date: str) -> tuple[str, str]:
    """Calculate the start and end date for the lookback period.

    The lookback period is the period of time before the partition date that
    we want to load posts from. The earliest that it can be is the
    min_lookback_date, and the latest that it can be is the partition date.
    """
    start_date = max(
        min_lookback_date, partition_date - timedelta(days=num_days_lookback)
    )
    end_date = partition_date
    return start_date, end_date


# keys = partition dates
# values = dicts of posts
# NOTE: reason why is we want to do processing incrementally via a rolling
# window, else we can't process all the posts in a single run.
def load_posts_used_in_feeds(partition_date: str) -> list[dict]:
    """Loads the posts that were used in the feeds for a specific date."""
    query = "SELECT uri FROM fetch_posts_used_in_feeds"
    logger.info(f"Loading posts used in feeds for partition date {partition_date}")
    df: pd.DataFrame = load_data_from_local_storage(
        service="fetch_posts_used_in_feeds",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [{"name": "fetch_posts_used_in_feeds", "columns": ["uri"]}]
        },
        start_partition_date=partition_date,
        end_partition_date=partition_date,
    )
    logger.info(
        f"Loaded {len(df)} posts used in feeds for partition date {partition_date}"
    )
    return df


def load_preprocessed_posts_used_in_feeds_for_partition_date(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> list[dict]:
    """Load the preprocessed, hydrated posts for the feeds created on the
    partition date."""
    # load in posts used in feeds for partition date
    # load in base pool of posts
    # match posts used in feeds to base pool of posts
    # load in hydrated versions of posts
    # check if posts have any integration data
    # if not, add to queue
    posts_used_in_feeds: list[dict] = load_posts_used_in_feeds(partition_date)
    logger.info(
        f"Loaded {len(posts_used_in_feeds)} posts used in feeds for {partition_date}."
    )
    logger.info(
        f"Loading base pool posts from {lookback_start_date} to {lookback_end_date} for {partition_date}."
    )
    base_pool_posts: list[dict] = load_preprocessed_posts(
        start_date=lookback_start_date, end_date=lookback_end_date
    )

    logger.info(
        f"Loaded {len(base_pool_posts)} base pool posts from {lookback_start_date} to {lookback_end_date}."
    )

    uris_of_posts_used_in_feeds: set[str] = set(
        post["uri"] for post in posts_used_in_feeds
    )

    res: list[dict] = []

    for post in base_pool_posts:
        if post["uri"] in uris_of_posts_used_in_feeds:
            res.append(post)

    logger.info(f"Found {len(res)} posts used in feeds for {partition_date}.")

    return res


@track_performance
def load_posts_to_backfill(
    partition_date: str,
    integrations: list[str] = INTEGRATIONS_LIST,
) -> dict[str, list[dict]]:
    """Load the posts to be backfilled for the partition date.

    Args:
        partition_date (str): The partition date to backfill for.
        integrations (list[str]): The integrations to backfill for.

    Loads the posts used in feeds (the hydrated, preprocessed versions) and then
    returns the URIs of the posts to be backfilled for each integration.
    """
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date
    )
    posts_used_in_feeds: list[dict] = (
        load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date,
            lookback_start_date=lookback_start_date,
            lookback_end_date=lookback_end_date,
        )
    )
    posts_to_backfill_by_integration: dict[str, list[dict]] = {}
    for integration in integrations:
        integration_post_uris: set[str] = load_service_post_uris(
            service=integration,
            start_date=lookback_start_date,
            end_date=lookback_end_date,
        )
        posts_to_backfill_by_integration[integration] = [
            post
            for post in posts_used_in_feeds
            if post["uri"] not in integration_post_uris
        ]
    return posts_to_backfill_by_integration
