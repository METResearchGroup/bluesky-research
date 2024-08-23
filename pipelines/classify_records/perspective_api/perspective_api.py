"""Base file for classifying posts in batch using the Perspective API."""

from lib.aws.s3 import S3
from lib.constants import current_datetime_str
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.ml_inference.perspective_api.export_data import export_results
from ml_tooling.perspective_api.model import run_batch_classification
from services.ml_inference.helper import get_posts_to_classify, insert_labeling_session  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


s3 = S3()
logger = get_logger(__name__)


@track_performance
def classify_latest_posts():
    """Classifies the latest preprocessed posts using the Perspective API."""
    labeling_session = {
        "inference_type": "perspective_api",
        "inference_timestamp": current_datetime_str,
    }
    posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(  # noqa
        inference_type="perspective_api"
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
    results = export_results(
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
