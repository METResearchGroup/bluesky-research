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
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
):
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        export_format=export_format,
    )
    logger.info(
        f"Loaded {len(df)} preprocessed posts for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df


def load_preprocessed_posts_by_uris(
    uris: pd.DataFrame,
    lookback_start_date: str,
    lookback_end_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
) -> pd.DataFrame:
    """Load preprocessed posts by URIs."""
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        export_format=export_format,
    )

    # filter to only include posts with the given URIs
    df = df[df["uri"].isin(uris["uri"])]
    logger.info(
        f"Filtered to {len(df)} preprocessed posts for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df
