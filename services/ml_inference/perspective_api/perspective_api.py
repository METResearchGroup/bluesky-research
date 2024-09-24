"""Base file for classifying posts in batch using the Perspective API."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from lib.aws.s3 import S3
from lib.constants import timestamp_format
from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from services.ml_inference.perspective_api.export_data import export_results
from ml_tooling.perspective_api.model import run_batch_classification
from services.ml_inference.helper import get_posts_to_classify, insert_labeling_session  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


s3 = S3()
logger = get_logger(__name__)


@track_performance
def classify_latest_posts(
    backfill_period: Optional[str] = None, backfill_duration: Optional[int] = None
):
    """Classifies the latest preprocessed posts using the Perspective API."""
    if backfill_duration is not None and backfill_period in ["days", "hours"]:
        current_time = datetime.now(timezone.utc)
        if backfill_period == "days":
            backfill_time = current_time - timedelta(days=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} days of data.")
        elif backfill_period == "hours":
            backfill_time = current_time - timedelta(hours=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} hours of data.")
    else:
        backfill_time = None
    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = None
    posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(  # noqa
        inference_type="perspective_api", timestamp=timestamp
    )
    logger.info(
        f"Classifying {len(posts_to_classify)} posts with the Perspective API..."
    )  # noqa
    if len(posts_to_classify) == 0:
        logger.warning("No posts to classify with Perspective API. Exiting...")
        return
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
    timestamp = generate_current_datetime_str()
    results = export_results(current_timestamp=timestamp, external_stores=["s3"])
    labeling_session = {
        "inference_type": "perspective_api",
        "inference_timestamp": timestamp,
        "total_classified_posts": results["total_classified_posts"],
        "total_classified_posts_by_source": results["total_classified_posts_by_source"],  # noqa
    }
    insert_labeling_session(labeling_session)


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
