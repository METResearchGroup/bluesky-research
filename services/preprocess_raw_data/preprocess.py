"""Preprocesses raw data before filtering."""

import pandas as pd

from services.preprocess_raw_data.filters import filter_posts


def prepare_posts_for_preprocessing(latest_posts: pd.DataFrame) -> pd.DataFrame:
    """Prepares posts for preprocessing."""
    latest_posts["text"] = latest_posts["text"].str.replace("\n", " ").str.strip()
    return latest_posts


def preprocess_latest_posts(latest_posts: pd.DataFrame) -> tuple[pd.DataFrame, dict]:  # noqa
    """Preprocesses and filters posts."""
    df: pd.DataFrame = prepare_posts_for_preprocessing(latest_posts=latest_posts)
    filtered_posts_df, posts_metadata = filter_posts(df)
    return filtered_posts_df, posts_metadata


def preprocess_latest_likes(latest_likes):
    return [], {"num_likes": len(latest_likes)}


def preprocess_latest_follows(latest_follows):
    return [], {"num_follows": len(latest_follows)}
