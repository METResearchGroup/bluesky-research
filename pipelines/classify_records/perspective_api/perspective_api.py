"""Base file for classifying posts in batch using the Perspective API."""

from datetime import datetime, timedelta, timezone

from lib.aws.s3 import S3
from lib.constants import current_datetime_str, timestamp_format
from lib.log.logger import get_logger
from services.ml_inference.perspective_api.export_data import export_classified_posts  # noqa
from ml_tooling.perspective_api.model import run_batch_classification
from services.ml_inference.helper import get_posts_to_classify, insert_labeling_session  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


s3 = S3()
logger = get_logger(__name__)

# by default, process the posts from the past day, if we don't have a
# previous timestamp to work with.
num_days_lookback = 1
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
).strftime(timestamp_format)


def init_session_metadata(source_feeds: list[str]):
    """Initializes the session metadata for the classification session."""
    res = {}
    res["inference_type"] = "perspective_api"
    res["inference_timestamp"] = current_datetime_str
    for feed in source_feeds:
        res[feed] = {
            "classification_type": "perspective_api",
            "num_posts_loaded": 0,
            "num_valid_posts_for_classification": 0,
            "num_invalid_posts_for_classification": 0,
            "num_posts_classified_with_api": 0,
        }
    return res


def classify_latest_posts():
    """Classifies the latest preprocessed posts using the Perspective API."""
    labeling_session = init_session_metadata()  # TODO: update the fields here.
    posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(  # noqa
        inference_type="perspective_api"
    )
    logger.info(
        f"Classifying {len(posts_to_classify)} posts with the Perspective API..."
    )  # noqa
    firehose_posts = [post for post in posts_to_classify if post.source == "firehose"]
    most_liked_posts = [
        post for post in posts_to_classify if post.source == "most_liked"
    ]

    source_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa
    for source, posts in source_to_posts_tuples:
        # labels stored in local storage, and then loaded
        # later. This format is done to make it more
        # robust to errors and to the script failing (though
        # tbh I could probably just return the posts directly
        # and then write to S3).
        run_batch_classification(posts=posts, source_feed=source)
    results = export_classified_posts(
        current_timestamp=current_datetime_str, external_stores=["s3"]
    )
    labeling_session = {
        "inference_type": "perspective_api",
        "inference_timestamp": current_datetime_str,
        "total_classified_posts": results["total_classified_posts"],
        "total_classified_posts_by_source": results["total_classified_posts_by_source"],  # noqa
    }
    insert_labeling_session(labeling_session)


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
