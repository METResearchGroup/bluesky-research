"""Helper code for running filters on raw data."""
import sys

from lib.db.sql.preprocessing_database import batch_create_filtered_posts
from lib.helper import track_performance
from lib.log.logger import Logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.filters import filter_posts
from services.preprocess_raw_data.load_data import load_latest_raw_posts
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

DEFAULT_BATCH_SIZE = 100000

logger = Logger(__name__)


@track_performance
def filter_latest_raw_data() -> list[FilteredPreprocessedPostModel]:  # noqa
    """Filters the latest raw data.

    Loads the latest posts, filters them, and writes the filtered data to the
    database. Writes all posts and their filtered status, so we can track
    which posts passed the filters and which ones didn't and so we don't
    duplicate filtering in the future.
    """
    latest_raw_posts: list[ConsolidatedPostRecordModel] = load_latest_raw_posts()  # noqa
    if len(latest_raw_posts) == 0:
        logger.warning("No new raw posts to filter and preprocess.")
        sys.exit(1)
    num_posts: int = len(latest_raw_posts)
    logger.info(f"Loaded {num_posts} posts for filtering.")
    filtered_posts: list[FilteredPreprocessedPostModel] = filter_posts(
        posts=latest_raw_posts
    )
    total_raw_posts = len(latest_raw_posts)
    num_posts_passed_filters = sum(
        post.passed_filters for post in filtered_posts
    )
    logger.info(f"After filtering, {num_posts_passed_filters} posts passed the filters (out of {total_raw_posts} original posts).")  # noqa
    return filtered_posts


def preprocess_raw_data() -> None:
    """Preprocesses raw data.

    We'll preprocess using the following steps:
    1. Filter the raw data.
    2. Preprocess the filtered data.
    3. Validate the preprocessed data.
    4. Write the filtered, preprocessed, validated data to the database.
    """
    filtered_posts = filter_latest_raw_data()
    batch_create_filtered_posts(filtered_posts)
    logger.info("Filtered data written to DB.")
