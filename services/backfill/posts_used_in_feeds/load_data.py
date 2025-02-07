"""Load data for posts used in feeds backfill pipeline."""

from datetime import timedelta
from typing import Optional

import pandas as pd

from lib.constants import partition_date_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.backfill.posts.load_data import (
    INTEGRATIONS_LIST,
    load_preprocessed_posts,
    load_service_post_uris,
)

logger = get_logger(__file__)

default_num_days_lookback = 5
default_min_lookback_date = "2024-09-28"


def calculate_start_end_date_for_lookback(
    partition_date: str,
    num_days_lookback: int = default_num_days_lookback,
    min_lookback_date: str = default_min_lookback_date,
) -> tuple[str, str]:
    """Calculate the start and end date for the lookback period.

    The lookback period is the period of time before the partition date that
    we want to load posts from. The earliest that it can be is the
    min_lookback_date, and the latest that it can be is the partition date.
    """
    partition_dt = pd.to_datetime(partition_date)
    lookback_dt = partition_dt - timedelta(days=num_days_lookback)
    lookback_date = lookback_dt.strftime(partition_date_format)
    start_date = max(min_lookback_date, lookback_date)
    end_date = partition_date
    return start_date, end_date


def load_posts_used_in_feeds(partition_date: str) -> pd.DataFrame:
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
        partition_date=partition_date,
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
    partition date.

    Since a feed's posts can be pooled from a base pool of posts from the past
    few days, we load in a day's worth of posts used in feeds and then match
    them to the base pool of posts from the past few days (as defined by
    lookback_start_date and lookback_end_date).
    """
    posts_used_in_feeds_df: pd.DataFrame = load_posts_used_in_feeds(partition_date)
    logger.info(
        f"Loaded {len(posts_used_in_feeds_df)} posts used in feeds for {partition_date}."
    )
    logger.info(
        f"Loading base pool posts from {lookback_start_date} to {lookback_end_date} for {partition_date}."
    )
    base_pool_posts: pd.DataFrame = load_preprocessed_posts(
        start_date=lookback_start_date,
        end_date=lookback_end_date,
        sorted_by_partition_date=True,
        ascending=True,
        table_columns=["uri", "text", "created_at", "partition_date"],
        output_format="df",
        convert_ts_fields=True,
    )

    # convert to list of dicts, for easier downstream iteration + processing.
    base_pool_posts: list[dict] = base_pool_posts.to_dict(orient="records")

    logger.info(
        f"Loaded {len(base_pool_posts)} base pool posts from {lookback_start_date} to {lookback_end_date}."
    )

    uris_of_posts_used_in_feeds: set[str] = set(posts_used_in_feeds_df["uri"].values)
    total_unique_posts_used_in_feeds = len(uris_of_posts_used_in_feeds)

    if len(base_pool_posts) < total_unique_posts_used_in_feeds:
        raise ValueError(
            f"Base pool size ({len(base_pool_posts)}) is less than "
            f"total unique posts used in feeds ({total_unique_posts_used_in_feeds}). "
            "This should never happen as the base pool should not be smaller than "
            "the total number of unique posts used in feeds."
        )

    res: list[dict] = []
    seen_uris = set()

    for post in base_pool_posts:
        if post["uri"] in uris_of_posts_used_in_feeds and post["uri"] not in seen_uris:
            res.append(post)
            seen_uris.add(post["uri"])

    logger.info(f"Found {len(res)} posts used in feeds for {partition_date}.")

    total_posts_used_in_feeds = len(posts_used_in_feeds_df)
    total_posts_in_base_pool = len(base_pool_posts)
    total_hydrated_posts_used_in_feeds = len(res)

    logger.info(
        f"Total posts used in feeds: {total_posts_used_in_feeds}\n"
        f"Total posts in base pool: {total_posts_in_base_pool}\n"
        f"Total unique posts used in feeds: {total_unique_posts_used_in_feeds}\n"
        f"Total hydrated posts used in feeds: {total_hydrated_posts_used_in_feeds}\n"
    )

    if total_hydrated_posts_used_in_feeds < total_unique_posts_used_in_feeds:
        logger.warning(
            f"Found fewer hydrated posts ({total_hydrated_posts_used_in_feeds}) than "
            f"unique posts used in feeds ({total_unique_posts_used_in_feeds}). "
            "This means some posts used in feeds don't have corresponding records."
            "We should investigate this, as this is OK but good to know why."
        )

    return res


@track_performance
def load_posts_to_backfill(
    partition_date: str,
    integrations: Optional[list[str]] = None,
) -> dict[str, list[dict]]:
    """Load the posts to be backfilled for the partition date.

    Args:
        partition_date (str): The partition date to backfill for.
        integrations (list[str]): The integrations to backfill for.

    Loads the posts used in feeds (the hydrated, preprocessed versions) and then
    returns the URIs of the posts to be backfilled for each integration.
    """
    if not integrations:
        integrations = INTEGRATIONS_LIST

    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=default_num_days_lookback,
        min_lookback_date=default_min_lookback_date,
    )
    posts_used_in_feeds: list[dict] = (
        load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date,
            lookback_start_date=lookback_start_date,
            lookback_end_date=lookback_end_date,
        )
    )
    total_posts_used_in_feeds = len(posts_used_in_feeds)
    posts_to_backfill_by_integration: dict[str, list[dict]] = {}
    logger.info(f"Loading posts to backfill for integrations: {integrations}...")
    for integration in integrations:
        logger.info(f"Loading posts to backfill for {integration}...")
        integration_post_uris: set[str] = load_service_post_uris(
            service=integration,
            start_date=lookback_start_date,
            end_date=lookback_end_date,
        )
        total_integration_uris = len(integration_post_uris)
        logger.info(
            f"Loaded {total_integration_uris} URIs for {integration} from {lookback_start_date} to {lookback_end_date}.\n"
            f"Comparing to {total_posts_used_in_feeds} posts used in feeds for {partition_date}."
        )
        integration_posts_to_backfill: list[dict] = [
            post
            for post in posts_used_in_feeds
            if post["uri"] not in integration_post_uris
        ]
        logger.info(
            f"Found {len(integration_posts_to_backfill)} posts to backfill for {integration}."
        )
        posts_to_backfill_by_integration[integration] = integration_posts_to_backfill

    integration_to_backfill_count = {
        integration: len(posts_to_backfill_by_integration[integration])
        for integration in integrations
    }
    logger.info(f"Posts to backfill per integration: {integration_to_backfill_count}")

    return posts_to_backfill_by_integration
