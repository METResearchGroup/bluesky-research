"""Helper code for running filters on raw data."""
from typing import Optional

from lib.db.sql.preprocessing_database import batch_create_filtered_posts
from lib.helper import track_performance
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.filters import filter_posts, save_filtered_posts_to_db  # noqa
from services.preprocess_raw_data.load_data import load_latest_raw_posts
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

DEFAULT_BATCH_SIZE = 100000


@track_performance
def filter_latest_raw_data() -> tuple[list[FilteredPreprocessedPostModel], int, int]:  # noqa
    """Filters the latest raw data.

    Loads the latest posts, filters them, and writes the filtered data to the
    database. Writes all posts and their filtered status, so we can track
    which posts passed the filters and which ones didn't and so we don't
    duplicate filtering in the future.
    """
    latest_raw_posts: list[ConsolidatedPostRecordModel] = load_latest_raw_posts()  # noqa
    num_posts: int = len(latest_raw_posts)
    print(f"Loaded {num_posts} posts for filtering.")
    filtered_posts: list[FilteredPreprocessedPostModel] = filter_posts(
        posts=latest_raw_posts
    )
    total_raw_posts = len(latest_raw_posts)
    num_posts_passed_filters = sum(
        post.passed_filters for post in filtered_posts
    )
    return filtered_posts, total_raw_posts, num_posts_passed_filters


def preprocess_posts(posts: list[dict]) -> list[dict]:
    """Performs any preprocessing that needs to be done on the filtered posts.

    Takes as input all posts, including their filter status, but operates
    on only posts that pass filtering.
    """
    return posts


def preprocess_raw_data(
    max_num_raw_posts: Optional[int] = DEFAULT_BATCH_SIZE
) -> None:
    """Preprocesses raw data.

    We'll preprocess using the following steps:
    1. Filter the raw data.
    2. Preprocess the filtered data.
    3. Validate the preprocessed data.
    4. Write the filtered, preprocessed, validated data to the database.
    """
    filtered_posts, total_raw_posts,  num_posts_passed_filters = (
        filter_latest_raw_data(max_num_raw_posts=max_num_raw_posts)
    )
    preprocessed_posts = preprocess_posts(filtered_posts)
    batch_create_filtered_posts(preprocessed_posts)
    print(f"Filtered data written to DB. After filtering, {num_posts_passed_filters} posts passed the filters (out of {total_raw_posts} original posts).")  # noqa
