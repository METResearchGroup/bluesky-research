"""Shared label data loading functionality for analytics system.

This module provides unified interfaces for loading various types of ML labels
used across the analytics system, eliminating code duplication and
ensuring consistent data handling patterns.
"""

from typing import Literal, Optional
import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.shared.config.loader import get_config

logger = get_logger(__file__)

# Load configuration
config = get_config()


def get_perspective_api_labels(
    lookback_start_date: str,
    lookback_end_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
):
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        export_format=export_format,
    )
    logger.info(
        f"Loaded {len(df)} Perspective API labels for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df


def get_perspective_api_labels_for_posts(
    posts: pd.DataFrame,
    lookback_start_date: str,
    lookback_end_date: str,
    duckdb_query: Optional[str] = None,
    query_metadata: Optional[dict] = None,
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
) -> pd.DataFrame:
    """Get the Perspective API labels for a list of posts.

    Args:
        posts: DataFrame containing posts with 'uri' column
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing Perspective API labels filtered to the given posts
    """
    df: pd.DataFrame = get_perspective_api_labels(
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        duckdb_query=duckdb_query,
        query_metadata=query_metadata,
        export_format=export_format,
    )

    # Filter to only include labels for the given posts
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} Perspective API labels for lookback period {lookback_start_date} to {lookback_end_date}"
    )
    return df


def get_sociopolitical_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Get the sociopolitical labels for a list of posts.

    Args:
        posts: DataFrame containing posts with 'uri' column
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing sociopolitical labels filtered to the given posts
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_sociopolitical",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    logger.info(
        f"Loaded {len(df)} sociopolitical labels for partition date {partition_date}"
    )

    # Filter to only include labels for the given posts
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} sociopolitical labels for partition date {partition_date}"
    )
    return df


def get_ime_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Get the IME labels for a list of posts.

    Args:
        posts: DataFrame containing posts with 'uri' column
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing IME labels filtered to the given posts
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    logger.info(f"Loaded {len(df)} IME labels for partition date {partition_date}")

    # Filter to only include labels for the given posts
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(f"Filtered to {len(df)} IME labels for partition date {partition_date}")
    return df


def get_valence_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Get the valence labels for a list of posts.

    Args:
        posts: DataFrame containing posts with 'uri' column
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing valence labels filtered to the given posts
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_valence_classifier",
        directory="cache",
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    logger.info(f"Loaded {len(df)} valence labels for partition date {partition_date}")

    # Filter to only include labels for the given posts
    df = df[df["uri"].isin(posts["uri"])]
    logger.info(
        f"Filtered to {len(df)} valence labels for partition date {partition_date}"
    )
    return df


def load_all_labels_for_posts(
    posts: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Load all available labels for posts and merge them together.

    This function loads all types of ML labels (Perspective API, sociopolitical,
    IME, valence) and merges them into a single DataFrame with one row per post.

    Args:
        posts: DataFrame containing posts with 'uri' column
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing all labels merged together, with one row per post
    """
    if posts.empty:
        logger.warning("No posts provided, returning empty labels DataFrame")
        return pd.DataFrame()

    # Load all label types
    label_dfs = []

    try:
        # Perspective API labels
        perspective_df = get_perspective_api_labels_for_posts(
            posts, partition_date, lookback_start_date, lookback_end_date
        )
        if not perspective_df.empty:
            label_dfs.append(perspective_df)
    except Exception as e:
        logger.warning(f"Failed to load Perspective API labels: {e}")

    try:
        # Sociopolitical labels
        sociopolitical_df = get_sociopolitical_labels_for_posts(
            posts, partition_date, lookback_start_date, lookback_end_date
        )
        if not sociopolitical_df.empty:
            label_dfs.append(sociopolitical_df)
    except Exception as e:
        logger.warning(f"Failed to load sociopolitical labels: {e}")

    try:
        # IME labels
        ime_df = get_ime_labels_for_posts(
            posts, partition_date, lookback_start_date, lookback_end_date
        )
        if not ime_df.empty:
            label_dfs.append(ime_df)
    except Exception as e:
        logger.warning(f"Failed to load IME labels: {e}")

    try:
        # Valence labels
        valence_df = get_valence_labels_for_posts(
            posts, partition_date, lookback_start_date, lookback_end_date
        )
        if not valence_df.empty:
            label_dfs.append(valence_df)
    except Exception as e:
        logger.warning(f"Failed to load valence labels: {e}")

    # Merge all labels together
    if not label_dfs:
        logger.warning(f"No labels loaded for partition date {partition_date}")
        return pd.DataFrame()

    # Start with the first label DataFrame
    merged_labels = label_dfs[0]

    # Merge additional label DataFrames
    for label_df in label_dfs[1:]:
        if not label_df.empty:
            # Use outer merge to keep all posts and labels
            merged_labels = merged_labels.merge(
                label_df, on="uri", how="outer", suffixes=("", "_duplicate")
            )

            # Remove duplicate columns (those ending with _duplicate)
            duplicate_cols = [
                col for col in merged_labels.columns if col.endswith("_duplicate")
            ]
            if duplicate_cols:
                merged_labels = merged_labels.drop(columns=duplicate_cols)

    logger.info(
        f"Successfully merged {len(label_dfs)} label types for partition date {partition_date}"
    )

    return merged_labels
