"""Load data for posts used in feeds backfill pipeline."""

from datetime import timedelta
import gc
from typing import Optional

import pandas as pd

from lib.constants import partition_date_format, timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.backfill.posts.load_data import (
    load_preprocessed_posts,
    load_service_post_uris,
)

logger = get_logger(__file__)

default_num_days_lookback = 5
default_min_lookback_date = "2024-09-28"

# we use preprocessing_timestamp now for our timestamps since the "created_at"
# timestamps are not accurate from the Bluesky firehose.
default_preprocessed_posts_columns = [
    "uri",
    "text",
    "preprocessing_timestamp",
    "partition_date",
]


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
    table_columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Load the preprocessed, hydrated posts for the feeds created on the
    partition date.

    Since a feed's posts can be pooled from a base pool of posts from the past
    few days, we load in a day's worth of posts used in feeds and then match
    them to the base pool of posts from the past few days (as defined by
    lookback_start_date and lookback_end_date).

    Returns:
        pd.DataFrame: DataFrame containing the preprocessed posts used in feeds.
            For duplicate URIs, keeps the earliest version based on preprocessing_timestamp.
    """
    posts_used_in_feeds_df: pd.DataFrame = load_posts_used_in_feeds(partition_date)
    logger.info(
        f"Loaded {len(posts_used_in_feeds_df)} posts used in feeds for {partition_date}."
    )
    logger.info(
        f"Loading base pool posts from {lookback_start_date} to {lookback_end_date} for {partition_date}."
    )
    if not table_columns:
        table_columns = default_preprocessed_posts_columns
    base_pool_posts: pd.DataFrame = load_preprocessed_posts(
        start_date=lookback_start_date,
        end_date=lookback_end_date,
        sorted_by_partition_date=True,
        ascending=True,
        table_columns=table_columns,
        output_format="df",
        convert_ts_fields=True,
    )

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

    # Filter to only posts used in feeds and sort by preprocessing_timestamp
    filtered_posts = base_pool_posts[
        base_pool_posts["uri"].isin(uris_of_posts_used_in_feeds)
    ]
    filtered_posts = filtered_posts.sort_values(
        "preprocessing_timestamp", ascending=True
    )

    # Keep earliest version of each post
    result_df = filtered_posts.drop_duplicates(subset=["uri"], keep="first")

    logger.info(f"Found {len(result_df)} posts used in feeds for {partition_date}.")

    total_posts_used_in_feeds = len(posts_used_in_feeds_df)
    total_posts_in_base_pool = len(base_pool_posts)
    total_hydrated_posts_used_in_feeds = len(result_df)

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

    del posts_used_in_feeds_df
    del filtered_posts
    del uris_of_posts_used_in_feeds
    del base_pool_posts
    gc.collect()

    return result_df


@track_performance
def load_posts_to_backfill(
    partition_date: str,
    integrations: list[str],
) -> dict[str, list[dict]]:
    """Load the posts to be backfilled for the partition date.

    Args:
        partition_date (str): The partition date to backfill for.
        integrations (list[str]): The integrations to backfill for.

    Loads the posts used in feeds (the hydrated, preprocessed versions) and then
    returns the URIs of the posts to be backfilled for each integration.
    """
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=default_num_days_lookback,
        min_lookback_date=default_min_lookback_date,
    )
    cols_str = ",".join(default_preprocessed_posts_columns)
    query = f"SELECT {cols_str} FROM preprocessed_posts_used_in_feeds;"

    posts_used_in_feeds_df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts_used_in_feeds",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [
                {
                    "name": "preprocessed_posts_used_in_feeds",
                    "columns": default_preprocessed_posts_columns,
                }
            ]
        },
        partition_date=partition_date,
    )

    total_posts_used_in_feeds = len(posts_used_in_feeds_df)
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

        integration_posts_to_backfill_df: pd.DataFrame = posts_used_in_feeds_df[
            ~posts_used_in_feeds_df["uri"].isin(integration_post_uris)
        ]

        # convert "preprocessing_timestamp" and "partition_date" to from datetime
        # to str, if they aren't already strings.
        if pd.api.types.is_datetime64_any_dtype(
            integration_posts_to_backfill_df["preprocessing_timestamp"]
        ):
            integration_posts_to_backfill_df["preprocessing_timestamp"] = (
                integration_posts_to_backfill_df[
                    "preprocessing_timestamp"
                ].dt.strftime(timestamp_format)
            )
        else:
            integration_posts_to_backfill_df["preprocessing_timestamp"] = (
                integration_posts_to_backfill_df["preprocessing_timestamp"].astype(str)
            )

        if pd.api.types.is_datetime64_any_dtype(
            integration_posts_to_backfill_df["partition_date"]
        ):
            integration_posts_to_backfill_df["partition_date"] = (
                integration_posts_to_backfill_df[
                    "partition_date"
                ].dt.strftime(partition_date_format)
            )
        else:
            integration_posts_to_backfill_df["partition_date"] = (
                integration_posts_to_backfill_df["partition_date"].astype(str)
            )

        logger.info(
            f"Dtypes for integration posts to backfill for {integration}:\n"
            f"{integration_posts_to_backfill_df.dtypes}"
        )

        integration_posts_to_backfill: list[dict] = (
            integration_posts_to_backfill_df.to_dict(orient="records")
        )
        del integration_posts_to_backfill_df
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
