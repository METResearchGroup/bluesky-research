"""Helper code for running filters on raw data."""
from datetime import datetime, timedelta, timezone

from lib.constants import current_datetime_str, timestamp_format
from lib.helper import track_performance
from lib.log.logger import Logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.export_data import (
    export_latest_follows, export_latest_likes,
    export_latest_preprocessed_posts, export_session_metadata
)
from services.preprocess_raw_data.load_data import (
    load_latest_posts, load_latest_likes, load_latest_follows,
    load_previous_session_metadata
)
from services.preprocess_raw_data.preprocess import (
    preprocess_latest_posts, preprocess_latest_likes, preprocess_latest_follows
)

DEFAULT_BATCH_SIZE = 100000
num_days_lookback = 1
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
).strftime(timestamp_format)

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
        "num_records_after_filtering": {
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


@track_performance
def preprocess_latest_raw_data():
    """Preprocesses the latest raw data."""
    print(f"Preprocessing the latest raw data at {current_datetime_str}.")
    previous_session_metadata: dict = load_previous_session_metadata()
    if previous_session_metadata:
        previous_timestamp = previous_session_metadata["current_preprocessing_timestamp"]  # noqa
        # we don't want to process data too far back, we really care most
        # about the most recent data. Can be changed later if needed, to
        # support processing data further back.
        if previous_timestamp < default_latest_timestamp:
            print(f"Previous timestamp is too far back ({previous_timestamp}), setting to default latest timestamp: {default_latest_timestamp}")  # noqa
            print(f"Number of default lookback days: {num_days_lookback}")
            previous_timestamp = default_latest_timestamp
    else:
        previous_timestamp = None

    session_metadata: dict = init_session_data(previous_timestamp=previous_timestamp)  # noqa

    print(f"Previous session metadata: {previous_session_metadata}")
    print(f"Current session metadata: {session_metadata}")

    if not previous_timestamp:
        previous_timestamp = default_latest_timestamp

    latest_posts: list[ConsolidatedPostRecordModel] = load_latest_posts(
        source="s3", latest_preprocessing_timestamp=previous_timestamp
    )
    print(f"Loaded {len(latest_posts)} posts from S3.")
    latest_likes = load_latest_likes(
        source="s3", latest_preprocessing_timestamp=previous_timestamp
    )
    print(f"Loaded {len(latest_likes)} likes from S3.")
    latest_follows = load_latest_follows(
        source="s3", latest_preprocessing_timestamp=previous_timestamp
    )
    print(f"Loaded {len(latest_follows)} follows from S3.")
    # TODO: preprocess study posts

    breakpoint()

    # we export only the posts that have passed preprocessing
    passed_posts, posts_metadata = (
        preprocess_latest_posts(latest_posts=latest_posts)  # noqa
    )
    preprocessed_likes, likes_metadata = preprocess_latest_likes(latest_likes=latest_likes)  # noqa
    preprocessed_follows, follows_metadata = preprocess_latest_follows(latest_follows=latest_follows)  # noqa

    session_metadata["num_raw_records"]["posts"] = posts_metadata["num_posts"]
    session_metadata["num_records_after_filtering"]["posts"] = posts_metadata["num_records_after_filtering"]["posts"]  # noqa
    session_metadata["num_raw_records"]["likes"] = likes_metadata["num_likes"]
    session_metadata["num_raw_records"]["follows"] = follows_metadata["num_follows"]

    print(f"Final session metadata: {session_metadata}")

    export_latest_preprocessed_posts(
        latest_posts=passed_posts,
        session_metadata=session_metadata,
        external_stores=["s3"]
    )
    export_latest_likes(preprocessed_likes)
    export_latest_follows(preprocessed_follows)

    breakpoint()

    export_session_metadata(session_metadata)
    print(f"Preprocessing completed at {current_datetime_str}.")
