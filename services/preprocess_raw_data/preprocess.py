"""Preprocesses raw data before filtering."""

import pandas as pd

from lib.helper import track_performance
from lib.log.logger import get_logger
from services.preprocess_raw_data.filters import filter_posts
from services.preprocess_raw_data.export_data import write_posts_to_cache
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


logger = get_logger(__name__)


def prepare_posts_for_preprocessing(latest_posts: pd.DataFrame) -> pd.DataFrame:
    """Prepares posts for preprocessing."""
    latest_posts["text"] = latest_posts["text"].str.replace("\n", " ").str.strip()

    if "author" in latest_posts.columns:
        latest_posts = latest_posts.rename(columns={"author": "author_did"})

    if "author_handle" not in latest_posts.columns:
        latest_posts["author_handle"] = None

    latest_posts["source"] = None

    return latest_posts


def postprocess_posts(posts: pd.DataFrame) -> pd.DataFrame:
    """Postprocesses posts.

    Makes sure that it conforms to expected data model."""
    post_dicts = posts.to_dict(orient="records")
    records = []
    try:
        for post_dict in post_dicts:
            # Extract only the fields required by FilteredPreprocessedPostModel
            filtered_dict = {
                field: post_dict[field]
                for field in FilteredPreprocessedPostModel.__annotations__.keys()
                if field in post_dict
            }
            record = FilteredPreprocessedPostModel(**filtered_dict)
            records.append(record.model_dump())
    except Exception as e:
        logger.error(f"Error postprocessing posts in preprocessing pipeline: {e}")
        raise e
    return records


@track_performance
def preprocess_latest_posts(posts: list[dict]) -> dict:
    """Preprocesses and filters posts."""
    batch_ids = [post["batch_id"] for post in posts]
    logger.info(f"Preprocessing {len(posts)} posts...")
    posts_df = pd.DataFrame(posts)
    transformed_posts_df: pd.DataFrame = prepare_posts_for_preprocessing(
        latest_posts=posts_df
    )
    preprocessed_posts_df, posts_metadata = filter_posts(transformed_posts_df)
    preprocessed_posts: list[dict] = postprocess_posts(preprocessed_posts_df)
    logger.info(
        f"Finished preprocessing {len(preprocessed_posts)} posts. Now writing to cache..."
    )
    write_posts_to_cache(posts=preprocessed_posts, batch_ids=batch_ids)
    logger.info(
        f"Finished writing {len(preprocessed_posts)} posts to cache. Returning metadata..."
    )
    return posts_metadata
