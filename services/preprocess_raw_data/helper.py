"""Helper code for running filters on raw data."""
from datetime import datetime, timedelta, timezone
import sys

from lib.constants import current_datetime_str
from lib.helper import track_performance
from lib.log.logger import Logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.filters import filter_posts
from services.preprocess_raw_data.load_data import (
    load_latest_posts, load_latest_likes, load_latest_follows,
    load_previous_session_data
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel
from services.preprocess_raw_data.preprocess import (
    preprocess_latest_posts, preprocess_latest_likes, preprocess_latest_follows
)

DEFAULT_BATCH_SIZE = 100000
num_days_lookback = 1
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
).strftime("%Y-%m-%d")

logger = Logger(__name__)


def init_session_data(previous_timestamp: str) -> dict:
    """Initializes the session data for the current preprocessing run."""
    return {
        "previous_preprocessing_timestamp": previous_timestamp,
        "current_preprocessing_timestamp": current_datetime_str,
        "num_raw_records": {
            "posts": 0,
            "likes": 0,
            "follows": 0
        },
        "num_records_per_filter": {
            "posts": {
                "passed": 0,
                "failed_total": 0,
                "failed_breakdown": {
                    "not_english": 0,
                    "has_spam": 0,
                    "has_hate_speech": 0,
                    "has_nsfw": 0,
                    "not_written_by_bot": 0
                }
            }
        }
    }


def export_session_data(session_data: dict):
    """Exports the session data to DynamoDB."""
    pass


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


def preprocess_latest_raw_data():
    """Preprocesses the latest raw data."""
    print(f"Preprocessing the latest raw data at {current_datetime_str}.")
    previous_session_data: dict = load_previous_session_data()
    if previous_session_data:
        previous_timestamp = previous_session_data["current_preprocessing_timestamp"]  # noqa
    else:
        previous_timestamp = None

    session_data: dict = init_session_data(previous_timestamp=previous_timestamp)  # noqa

    if not previous_timestamp:
        previous_timestamp = default_latest_timestamp

    latest_posts = load_latest_posts(
        source="s3", latest_preprocessing_timestamp=previous_timestamp
    )
    latest_likes = load_latest_likes(
        source="s3", latest_preprocessing_timestamp=previous_timestamp
    )
    latest_follows = load_latest_follows(
        source="s3", latest_preprocessing_timestamp=previous_timestamp
    )

    preprocess_latest_posts(latest_posts=latest_posts)
    preprocess_latest_likes(latest_likes=latest_likes)
    preprocess_latest_follows(latest_follows=latest_follows)

    export_session_data(session_data)
    print(f"Preprocessing completed at {current_datetime_str}.")
