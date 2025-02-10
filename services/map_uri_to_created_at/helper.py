"""Helper functions for mapping URIs to their creation timestamps.

This module provides functions to load preprocessed posts and extract URI to
creation timestamp mappings. The mappings are processed in partition date batches
for efficient memory management and data organization.
"""

import pandas as pd

from lib.db.manage_local_data import (
    export_data_to_local_storage,
)
from lib.helper import get_partition_dates
from lib.log.logger import get_logger
from services.backfill.posts.load_data import load_preprocessed_posts
from lib.constants import convert_bsky_dt_to_pipeline_dt

logger = get_logger(__file__)


def map_uris_to_created_at(partition_date: str) -> pd.DataFrame:
    """Load preprocessed posts and extract URI to creation timestamp mappings.

    Args:
        partition_date (str): The partition date to process in YYYY-MM-DD format.

    Returns:
        pd.DataFrame: DataFrame containing URI to creation timestamp mappings.
            Contains columns:
            - uri (str): The unique identifier for the post
            - created_at (str): The timestamp when the post was created
    """
    logger.info(f"Loading preprocessed posts for partition date {partition_date}...")
    posts_df: pd.DataFrame = load_preprocessed_posts(
        start_date=partition_date,
        end_date=partition_date,
        table_columns=["uri", "created_at", "text"],
        output_format="df",
    )

    if len(posts_df) == 0:
        logger.warning(f"No posts found for partition date {partition_date}")
        return pd.DataFrame(columns=["uri", "created_at"])

    posts_df = posts_df[posts_df["text"].notna() & (posts_df["text"] != "")]

    # Convert Bluesky timestamps to pipeline format
    posts_df["created_at"] = posts_df["created_at"].apply(
        convert_bsky_dt_to_pipeline_dt
    )

    logger.info(f"Found {len(posts_df)} posts for partition date {partition_date}")
    return posts_df[["uri", "created_at"]]


def map_uris_to_created_at_for_partition_date(partition_date: str):
    """Map URIs to creation timestamps for a given partition date and export to storage.

    Args:
        partition_date (str): The partition date to process in YYYY-MM-DD format.
    """
    logger.info(f"Processing URI mappings for partition date {partition_date}...")

    mappings_df = map_uris_to_created_at(partition_date=partition_date)

    export_data_to_local_storage(
        service="uris_to_created_at",
        df=mappings_df,
        export_format="parquet",
    )
    logger.info(
        f"Exported {len(mappings_df)} URI mappings to uris_to_created_at for "
        f"partition date {partition_date}."
    )


def map_uris_to_created_at_for_partition_dates(
    start_date: str = "2024-09-28",
    end_date: str = "2025-12-01",
    exclude_partition_dates: list[str] = ["2024-10-08"],
):
    """Map URIs to creation timestamps for multiple partition dates.

    Args:
        start_date (str): Start date in YYYY-MM-DD format (inclusive).
            Defaults to "2024-09-28".
        end_date (str): End date in YYYY-MM-DD format (inclusive).
            Defaults to "2025-12-01".
        exclude_partition_dates (list[str]): List of dates to exclude in YYYY-MM-DD format.
            Defaults to ["2024-10-08"].
    """
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    for partition_date in partition_dates:
        map_uris_to_created_at_for_partition_date(partition_date=partition_date)
    logger.info("Finished mapping URIs across partition dates.")


if __name__ == "__main__":
    map_uris_to_created_at_for_partition_dates()
