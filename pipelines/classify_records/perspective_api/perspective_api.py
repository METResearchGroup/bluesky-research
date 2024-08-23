"""Base file for classifying posts in batch using the Perspective API."""

from datetime import datetime, timedelta, timezone

from lib.aws.s3 import S3
from lib.constants import current_datetime_str, timestamp_format
from lib.log.logger import get_logger
from services.ml_inference.helper import get_posts_to_classify, insert_labeling_session  # noqa
from services.ml_inference.perspective_api.helper import filter_posts_already_in_cache  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


s3 = S3()
logger = get_logger(__name__)

# by default, process the posts from the past day, if we don't have a
# previous timestamp to work with.
num_days_lookback = 1
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
).strftime(timestamp_format)


def init_session_metadata(previous_timestamp: str, source_feeds: list[str]):
    """Initializes the session metadata for the classification session."""
    res = {}
    res["source_feeds"] = source_feeds
    res["previous_classification_timestamp"] = previous_timestamp
    res["current_classification_timestamp"] = current_datetime_str
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
    posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(
        inference_type="perspective_api"
    )
    logger.info(
        f"Classifying {len(posts_to_classify)} posts with the Perspective API..."
    )  # noqa
    # TODO: classify the posts with the Perspective API.
    labels = []
    # TODO: export the results to S3.
    # TODO: add partitioning here as well and trigger Glue crawler?
    # TODO: need to think of the S3 path that I want to use here.
    s3_key = ""
    s3.write_dicts_jsonl_to_s3(data=labels, key=s3_key)
    insert_labeling_session(labeling_session)


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
