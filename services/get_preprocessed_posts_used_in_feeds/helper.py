"""Helper functions for getting preprocessed posts used in feeds.

This service takes the posts used in feeds and loads their preprocessed versions
from the preprocessed_posts service. This allows us to track which preprocessed
posts were actually used in feeds. The service supports loading data with a lookback
window to ensure we capture all relevant preprocessed posts.
"""

import gc

import pandas as pd

from lib.db.manage_local_data import export_data_to_local_storage
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    load_preprocessed_posts_used_in_feeds_for_partition_date as load_posts_with_lookback,
    calculate_start_end_date_for_lookback,
    default_num_days_lookback,
    default_min_lookback_date,
)

logger = get_logger(__file__)


def load_preprocessed_posts_used_in_feeds_for_partition_date(
    partition_date: str,
) -> pd.DataFrame:
    """Load preprocessed posts used in feeds for a given partition date with lookback.

    Args:
        partition_date (str): The partition date to load data for, in YYYY-MM-DD format.

    Returns:
        pd.DataFrame: DataFrame containing preprocessed posts that were used in feeds.
            The DataFrame includes all columns from the preprocessed_posts service
            for posts that were used in feeds on the given partition date.
            Only returns posts that exist in both the fetch_posts_used_in_feeds
            and preprocessed_posts services.

    Behavior:
        1. Calculates lookback window using default settings (5 days lookback,
           minimum date of 2024-09-28)
        2. Loads preprocessed posts using the lookback window to ensure we capture
           all relevant posts
        3. Logs the number of posts found at each step
    """
    logger.info(f"Processing partition date {partition_date} with lookback...")

    # Calculate lookback window
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=default_num_days_lookback,
        min_lookback_date=default_min_lookback_date,
    )

    # Load posts with lookback
    preprocessed_posts_df = load_posts_with_lookback(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    logger.info(
        f"Found {len(preprocessed_posts_df)} preprocessed posts that were used in feeds."
    )
    return preprocessed_posts_df


def get_and_export_preprocessed_posts_used_in_feeds_for_partition_date(
    partition_date: str,
):
    """Get and export preprocessed posts used in feeds for a given partition date.

    Args:
        partition_date (str): The partition date to process, in YYYY-MM-DD format.

    Behavior:
        1. Loads preprocessed posts used in feeds for the partition date
        2. Exports the loaded data to local storage in parquet format
        3. Logs the number of posts exported

    The partition_date is partitioned on the "preprocessing_timestamp" field
    of the preprocessed_posts. We use this in lieu of the "created_at" field
    from the fetch_posts_used_in_feeds service, since the "created_at" field
    is not accurate from the Bluesky firehose.
    """
    logger.info(
        f"Processing preprocessed posts from feeds with partition date {partition_date}..."
    )

    preprocessed_posts_df = load_preprocessed_posts_used_in_feeds_for_partition_date(
        partition_date=partition_date
    )

    export_data_to_local_storage(
        service="preprocessed_posts_used_in_feeds",
        df=preprocessed_posts_df,
        export_format="parquet",
    )
    logger.info(
        f"Exported {len(preprocessed_posts_df)} preprocessed posts to preprocessed_posts_used_in_feeds "
        f"for partition date {partition_date}."
    )
    del preprocessed_posts_df
    gc.collect()


def get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates(
    start_date: str = "2024-09-28",
    end_date: str = "2025-12-01",
    exclude_partition_dates: list[str] = ["2024-10-08"],
):
    """Get and export preprocessed posts used in feeds for multiple partition dates.

    Args:
        start_date (str): Start date in YYYY-MM-DD format (inclusive). Defaults to "2024-09-28".
        end_date (str): End date in YYYY-MM-DD format (inclusive). Defaults to "2025-12-01".
        exclude_partition_dates (list[str]): List of dates to exclude in YYYY-MM-DD format.
            Defaults to ["2024-10-08"].

    Behavior:
        1. Generates list of partition dates between start_date and end_date,
           excluding specified dates
        2. For each partition date:
            a. Loads preprocessed posts used in feeds
            b. Exports the data to local storage
        3. Logs progress and completion
    """
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    for partition_date in partition_dates:
        get_and_export_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date
        )
    logger.info("Finished exporting preprocessed posts across partition dates.")


if __name__ == "__main__":
    get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates()
