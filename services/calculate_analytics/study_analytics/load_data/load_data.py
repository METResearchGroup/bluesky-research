"""Loads the data for a given partition date."""

import gc

import pandas as pd

from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
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


logger = get_logger(__file__)


def get_hydrated_posts_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Hydrate each post and create a wide table of post features."""
    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=default_num_days_lookback,
    )
    posts_df: pd.DataFrame = load_preprocessed_posts_used_in_feeds_for_partition_date(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )
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


def get_hydrated_feed_posts_per_user(partition_date: str) -> dict[str, pd.DataFrame]:
    """Get the hydrated posts for a given partition date and map them to
    the users who posted them.
    """
    posts_df: pd.DataFrame = get_hydrated_posts_for_partition_date(partition_date)
    users_to_posts: dict[str, set[str]] = map_users_to_posts_used_in_feeds(
        partition_date=partition_date
    )
    map_user_to_subset_df: dict[str, pd.DataFrame] = {}
    for user, posts in users_to_posts.items():
        subset_df = posts_df[posts_df["uri"].isin(posts)]
        map_user_to_subset_df[user] = subset_df
    return map_user_to_subset_df
