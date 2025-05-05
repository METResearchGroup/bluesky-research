"""Preprocesses raw data before filtering."""

import pandas as pd

from lib.helper import track_performance
from lib.log.logger import get_logger
from services.preprocess_raw_data.filters import filter_posts
from services.preprocess_raw_data.export_data import write_posts_to_cache


logger = get_logger(__name__)


def prepare_posts_for_preprocessing(latest_posts: pd.DataFrame) -> pd.DataFrame:
    """Prepares posts for preprocessing."""
    latest_posts["text"] = latest_posts["text"].str.replace("\n", " ").str.strip()
    return latest_posts


@track_performance
def preprocess_latest_posts(posts: list[dict]) -> dict:
    """Preprocesses and filters posts."""
    logger.info(f"Preprocessing {len(posts)} posts...")
    posts_df: pd.DataFrame = prepare_posts_for_preprocessing(posts=posts)
    transformed_posts_df: pd.DataFrame = prepare_posts_for_preprocessing(posts=posts_df)
    preprocessed_posts_df, posts_metadata = filter_posts(transformed_posts_df)
    preprocessed_posts = preprocessed_posts_df.to_dict(orient="records")
    logger.info(
        f"Finished preprocessing {len(preprocessed_posts)} posts. Now writing to cache..."
    )
    write_posts_to_cache(posts=preprocessed_posts)
    logger.info(
        f"Finished writing {len(preprocessed_posts)} posts to cache. Returning metadata..."
    )
    return posts_metadata
