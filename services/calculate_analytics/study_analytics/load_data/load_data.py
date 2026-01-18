"""Loads the data for a given partition date."""

import gc
from typing import Literal, Optional

import pandas as pd

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
    get_valence_labels_for_posts,
)
from services.calculate_analytics.study_analytics.load_data.helper import (
    insert_missing_posts_to_backfill_queue,
)

logger = get_logger(__file__)


def load_filtered_preprocessed_posts(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
) -> pd.DataFrame:
    """Load preprocessed posts for analysis.

    Note: Author filtering has been removed for testing purposes.
    This can be re-added later when needed.
    """
    columns = ["uri", "text", "preprocessing_timestamp", "author_did", "author_handle"]
    posts_df: pd.DataFrame = load_preprocessed_posts_used_in_feeds_for_partition_date(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        table_columns=columns,
    )
    logger.info(f"Loaded {len(posts_df)} posts for partition date {partition_date}")
    # Note: Author filtering removed for testing - can be re-added later
    logger.info(
        f"Using all posts for partition date {partition_date} (no filtering applied)"
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

    valence_labels_df: pd.DataFrame = get_valence_labels_for_posts(
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
    valence_labels_df = valence_labels_df.drop_duplicates(subset=["uri"])

    # Left join each set of labels against the posts dataframe
    # This ensures we keep all posts even if they don't have certain labels
    # Keep only the left dataframe's columns when there are duplicates
    # Get list of duplicate columns between posts_df and each of the integrations.
    # These are columns like "text' and whatnot.
    duplicate_cols = [
        col
        for col in perspective_api_labels_df.columns
        if col in posts_df.columns and col != "uri"
    ]

    duplicate_cols = []

    for label_df in [
        perspective_api_labels_df,
        sociopolitical_labels_df,
        ime_labels_df,
        valence_labels_df,
    ]:
        for col in label_df.columns:
            if col in posts_df.columns and col != "uri":
                duplicate_cols.append(col)

    # Drop duplicate columns from each integration df before merging.
    perspective_api_labels_df_deduped = perspective_api_labels_df.drop(
        columns=duplicate_cols
    )  # noqa
    ime_labels_df_deduped = ime_labels_df.drop(columns=duplicate_cols)
    sociopolitical_labels_df_deduped = sociopolitical_labels_df.drop(
        columns=duplicate_cols
    )  # noqa
    valence_labels_df_deduped = valence_labels_df.drop(columns=duplicate_cols)  # noqa

    # merge the dfs together.
    posts_with_all_labels = posts_df

    for label_df in [
        perspective_api_labels_df_deduped,
        sociopolitical_labels_df_deduped,
        ime_labels_df_deduped,
        valence_labels_df_deduped,
    ]:
        # Merge with suffixes to handle duplicate columns
        posts_with_all_labels = posts_with_all_labels.merge(
            label_df, on="uri", how="left", suffixes=("", "_drop")
        )
        # Drop columns with '_drop' suffix
        cols_to_drop = [
            col for col in posts_with_all_labels.columns if col.endswith("_drop")
        ]
        if cols_to_drop:
            posts_with_all_labels = posts_with_all_labels.drop(columns=cols_to_drop)

    # get missing labels.
    missing_perspective_api_labels_df = posts_df[
        ~posts_df["uri"].isin(perspective_api_labels_df["uri"])
    ]
    missing_sociopolitical_labels_df = posts_df[
        ~posts_df["uri"].isin(sociopolitical_labels_df["uri"])
    ]
    missing_ime_labels_df = posts_df[~posts_df["uri"].isin(ime_labels_df["uri"])]
    missing_valence_labels_df = posts_df[
        ~posts_df["uri"].isin(valence_labels_df["uri"])
    ]

    for integration, missing_df in [
        ("ml_inference_perspective_api", missing_perspective_api_labels_df),
        ("ml_inference_sociopolitical", missing_sociopolitical_labels_df),
        ("ml_inference_ime", missing_ime_labels_df),
        ("ml_inference_valence_classifier", missing_valence_labels_df),
    ]:
        if len(missing_df) > 0:
            print(
                f"Found {len(missing_df)} missing {integration} labels for partition date = {partition_date}"
            )
            insert_missing_posts_to_backfill_queue(missing_df, integration)

    del posts_df
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
        storage_tiers=["cache"],
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
