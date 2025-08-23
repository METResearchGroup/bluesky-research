"""Shared post data loading functionality for analytics system.

This module provides unified interfaces for loading and filtering post data
used across the analytics system, eliminating code duplication and
ensuring consistent data handling patterns.
"""

import gc
import os
from typing import Optional

import pandas as pd

from lib.constants import project_home_directory
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
    default_num_days_lookback,
)
from services.calculate_analytics.study_analytics.shared.config.loader import get_config
from services.calculate_analytics.study_analytics.shared.data_loading.labels import (
    load_all_labels_for_posts,
)
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    BSKY_HANDLES_TO_EXCLUDE,
)

logger = get_logger(__file__)

# Load configuration
config = get_config()

# Load invalid author data
invalid_dids = pd.read_csv(
    os.path.join(
        project_home_directory,
        "services/preprocess_raw_data/classify_nsfw_content/dids_to_exclude.csv",
    )
)
invalid_handles = BSKY_HANDLES_TO_EXCLUDE


def load_filtered_preprocessed_posts(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Load preprocessed posts that have been filtered with additional
    custom filters for analysis.

    We can check, for example, what happens if we remove posts from invalid
    authors (e.g., NSFW authors), which was a change that eventually made it
    into production but wasn't present in the beginning.

    Args:
        partition_date: The partition date to load posts for
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing filtered preprocessed posts
    """
    # Get default columns from config
    columns = config.get(
        "data_loading.default_columns",
        ["uri", "text", "preprocessing_timestamp", "author_did", "author_handle"],
    )

    posts_df: pd.DataFrame = load_preprocessed_posts_used_in_feeds_for_partition_date(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        table_columns=columns,
    )
    logger.info(f"Loaded {len(posts_df)} posts for partition date {partition_date}")

    # Filter out invalid authors
    posts_df = posts_df[~posts_df["author_did"].isin(invalid_dids["did"])]
    posts_df = posts_df[~posts_df["author_handle"].isin(invalid_handles)]

    logger.info(
        f"Filtered {len(posts_df)} posts for partition date {partition_date} by removing invalid authors"
    )
    return posts_df


def get_hydrated_posts_for_partition_date(
    partition_date: str,
    posts_df: Optional[pd.DataFrame] = None,
    load_unfiltered_posts: bool = False,
) -> pd.DataFrame:
    """Hydrate each post and create a wide table of post features.

    Args:
        partition_date: The partition date to load posts for
        posts_df: Optional pre-loaded posts DataFrame
        load_unfiltered_posts: Whether to load unfiltered posts

    Returns:
        DataFrame containing hydrated posts with all features
    """
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=default_num_days_lookback,
    )

    if posts_df is not None:
        logger.info(
            f"Using provided posts dataframe for partition date {partition_date}"
        )
    else:
        if load_unfiltered_posts:
            df: pd.DataFrame = load_preprocessed_posts_used_in_feeds_for_partition_date(
                partition_date=partition_date,
                lookback_start_date=lookback_start_date,
                lookback_end_date=lookback_end_date,
            )
        else:
            logger.info(
                f"Loading custom filtered preprocessed posts for partition date {partition_date}"
            )
            df: pd.DataFrame = load_filtered_preprocessed_posts(
                partition_date=partition_date,
                lookback_start_date=lookback_start_date,
                lookback_end_date=lookback_end_date,
            )
        posts_df = df

    # Load all labels for posts
    posts_df = load_posts_with_labels(
        posts_df=posts_df,
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    # Clean up memory
    gc.collect()

    return posts_df


def load_posts_with_labels(
    posts_df: pd.DataFrame,
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Load posts with all available labels and merge them together.

    Args:
        posts_df: Base posts DataFrame
        partition_date: The partition date
        lookback_start_date: Start date for lookback period
        lookback_end_date: End date for lookback period

    Returns:
        DataFrame containing posts with all labels merged
    """
    # Load all labels
    labels_df = load_all_labels_for_posts(
        posts=posts_df,
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    # Merge labels with posts
    if not labels_df.empty:
        posts_df = posts_df.merge(
            labels_df,
            on="uri",
            how="left",
        )
        logger.info(
            f"Merged {len(labels_df)} label records with posts for partition date {partition_date}"
        )
    else:
        logger.warning(f"No labels found for partition date {partition_date}")

    return posts_df
