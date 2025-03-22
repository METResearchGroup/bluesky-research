"""Loads the data for a given partition date."""

import gc
import os
from typing import Literal, Optional

import pandas as pd

from lib.constants import project_home_directory
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
    load_posts_used_in_feeds,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
    default_num_days_lookback,
)
from services.calculate_analytics.study_analytics.load_data.load_feeds import (
    map_users_to_posts_used_in_feeds,
)
from services.calculate_analytics.study_analytics.load_data.load_labels import (
    get_ime_labels_for_posts,
    get_perspective_api_labels_for_posts,
    get_sociopolitical_labels_for_posts,
)
from services.calculate_analytics.study_analytics.load_data.helper import (
    insert_missing_posts_to_backfill_queue,
)
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    BSKY_HANDLES_TO_EXCLUDE,
)

invalid_dids = pd.read_csv(
    os.path.join(
        project_home_directory,
        "services/preprocess_raw_data/classify_nsfw_content/dids_to_exclude.csv",
    )
)
invalid_handles = BSKY_HANDLES_TO_EXCLUDE


logger = get_logger(__file__)


def load_filtered_preprocessed_posts(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Load preprocessed posts that have been filtered with additional
    custom filters for analysis.

    We can check, for example, what happens if we remove posts from invalid
    authors (e.g., NSFW authors), which was a change that eventually made it
    into production but wasn't present in the beginning."""
    columns = ["uri", "text", "preprocessing_timestamp", "author_did", "author_handle"]
    posts_df: pd.DataFrame = load_preprocessed_posts_used_in_feeds_for_partition_date(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        table_columns=columns,
    )
    logger.info(f"Loaded {len(posts_df)} posts for partition date {partition_date}")
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
    """Hydrate each post and create a wide table of post features."""
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

    perspective_api_labels_df: pd.DataFrame = get_perspective_api_labels_for_posts(
        posts=posts_df,
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )
    ime_labels_df: pd.DataFrame = get_ime_labels_for_posts(
        posts=posts_df,
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    sociopolitical_labels_df: pd.DataFrame = get_sociopolitical_labels_for_posts(
        posts=posts_df,
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    # deduping
    posts_df = posts_df.drop_duplicates(subset=["uri"])
    perspective_api_labels_df = perspective_api_labels_df.drop_duplicates(
        subset=["uri"]
    )
    ime_labels_df = ime_labels_df.drop_duplicates(subset=["uri"])
    sociopolitical_labels_df = sociopolitical_labels_df.drop_duplicates(subset=["uri"])

    # Left join each set of labels against the posts dataframe
    # This ensures we keep all posts even if they don't have certain labels
    # Keep only the left dataframe's columns when there are duplicates
    # Get list of duplicate columns between posts_df and perspective_api_labels_df
    duplicate_cols = [
        col
        for col in perspective_api_labels_df.columns
        if col in posts_df.columns and col != "uri"
    ]
    # Drop duplicate columns from perspective_api_labels_df before merging
    perspective_api_labels_df_deduped = perspective_api_labels_df.drop(
        columns=duplicate_cols
    )
    ime_labels_df_deduped = ime_labels_df.drop(columns=duplicate_cols)
    sociopolitical_labels_df_deduped = sociopolitical_labels_df.drop(
        columns=duplicate_cols
    )

    posts_with_perspective = posts_df.merge(
        perspective_api_labels_df_deduped, on="uri", how="left"
    )

    posts_with_ime = posts_with_perspective.merge(
        ime_labels_df_deduped, on="uri", how="left"
    )

    posts_with_all_labels = posts_with_ime.merge(
        sociopolitical_labels_df_deduped, on="uri", how="left"
    )

    # get missing labels.
    missing_perspective_api_labels_df = posts_df[
        ~posts_df["uri"].isin(perspective_api_labels_df["uri"])
    ]

    missing_sociopolitical_labels_df = posts_df[
        ~posts_df["uri"].isin(sociopolitical_labels_df["uri"])
    ]

    missing_ime_labels_df = posts_df[~posts_df["uri"].isin(ime_labels_df["uri"])]

    for integration, missing_df in [
        ("ml_inference_perspective_api", missing_perspective_api_labels_df),
        ("ml_inference_sociopolitical", missing_sociopolitical_labels_df),
        ("ml_inference_ime", missing_ime_labels_df),
    ]:
        if len(missing_df) > 0:
            print(
                f"Found {len(missing_df)} missing {integration} labels for partition date = {partition_date}"
            )
            insert_missing_posts_to_backfill_queue(missing_df, integration)

    del posts_df
    del posts_with_perspective
    del posts_with_ime
    del perspective_api_labels_df
    del ime_labels_df
    del sociopolitical_labels_df
    del missing_perspective_api_labels_df
    del missing_sociopolitical_labels_df
    del missing_ime_labels_df
    gc.collect()

    logger.info(
        f"Created wide table with {len(posts_with_all_labels)} rows for partition date {partition_date}"
    )

    return posts_with_all_labels


def get_hydrated_feed_posts_per_user(
    partition_date: str, load_unfiltered_posts: bool = True
) -> dict[str, pd.DataFrame]:
    """Get the hydrated posts for a given partition date and map them to
    the users who posted them.
    """
    posts_df: pd.DataFrame = get_hydrated_posts_for_partition_date(
        partition_date=partition_date, load_unfiltered_posts=load_unfiltered_posts
    )
    users_to_posts: dict[str, set[str]] = map_users_to_posts_used_in_feeds(
        partition_date=partition_date
    )
    map_user_to_subset_df: dict[str, pd.DataFrame] = {}
    for user, posts in users_to_posts.items():
        subset_df = posts_df[posts_df["uri"].isin(posts)]
        map_user_to_subset_df[user] = subset_df
    return map_user_to_subset_df


def load_preprocessed_posts_by_source(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
    source: Literal["firehose", "most_liked"],
) -> pd.DataFrame:
    """Load posts by source."""
    table_columns = [
        "uri",
        "text",
        "preprocessing_timestamp",
        "partition_date",
        "source",
    ]
    table_columns_str = ", ".join(table_columns)
    query = f"""
        SELECT {table_columns_str}
        FROM preprocessed_posts
        WHERE text IS NOT NULL
        AND text != ''
        AND partition_date = '{partition_date}'
        AND source = '{source}'
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [{"name": "preprocessed_posts", "columns": table_columns}]
        },
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
        source_types=[source],
    )
    logger.info(
        f"Loaded {len(df)} posts from cache for partition date {partition_date} and source {source}"
    )
    return df


def load_posts_used_in_feeds_by_source(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
    source: Literal["firehose", "most_liked"],
) -> pd.DataFrame:
    """Load posts used in feeds by source."""
    posts_used_in_feeds_df: pd.DataFrame = load_posts_used_in_feeds(partition_date)
    base_pool_posts: pd.DataFrame = load_preprocessed_posts_by_source(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        source=source,
    )

    uris_of_posts_used_in_feeds: set[str] = set(posts_used_in_feeds_df["uri"].values)
    total_unique_posts_used_in_feeds = len(uris_of_posts_used_in_feeds)
    # Filter to only posts used in feeds and sort by preprocessing_timestamp
    filtered_posts = base_pool_posts[
        base_pool_posts["uri"].isin(uris_of_posts_used_in_feeds)
    ]
    filtered_posts = filtered_posts.sort_values(
        "preprocessing_timestamp", ascending=True
    )

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
