"""Data loading utilities for posts."""

from typing import Literal, Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

logger = get_logger(__file__)


def load_preprocessed_posts(
    lookback_start_date: str,
    lookback_end_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    source_file_format: Literal["jsonl", "parquet"] = "parquet",
) -> pd.DataFrame:
    """Load preprocessed posts data from local storage.

    Args:
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period
        duckdb_query: Optional SQL query for DuckDB format
        query_metadata: Optional metadata for DuckDB query
        source_file_format: Format of the source data files

    Returns:
        DataFrame containing preprocessed posts data
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=["cache"],
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        source_file_format=source_file_format,
    )
    logger.info(
        f"Loaded {len(df)} preprocessed posts for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df


def load_preprocessed_posts_by_uris(
    uris: set[str],
    partition_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    source_file_format: Literal["jsonl", "parquet"] = "parquet",
) -> pd.DataFrame:
    """Load preprocessed posts data filtered by specific URIs and partition date.

    Args:
        uris: Set of post URIs to filter by
        partition_date: The partition date to load posts for
        duckdb_query: Optional SQL query for DuckDB format
        query_metadata: Optional metadata for DuckDB query
        source_file_format: Format of the source data files

    Returns:
        DataFrame containing preprocessed posts data filtered by URIs
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        storage_tiers=["cache"],
        partition_date=partition_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        source_file_format=source_file_format,
    )

    # Filter to only include posts with the specified URIs
    df = df[df["uri"].isin(uris)]

    logger.info(
        f"Loaded {len(df)} preprocessed posts for {len(uris)} URIs on partition date {partition_date}"
    )
    return df
