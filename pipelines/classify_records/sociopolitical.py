"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from lib.constants import current_datetime_str
from lib.db.sql.ml_inference_database import (
    batch_insert_metadata, batch_insert_sociopolitical_labels,
    get_existing_sociopolitical_uris
)
from lib.db.sql.preprocessing_database import get_filtered_posts
from ml_tooling.llm.model import run_batch_classification
from pipelines.classify_records.helper import (
    get_post_metadata_for_classification, validate_posts
)
from services.ml_inference.models import (
    RecordClassificationMetadataModel, SociopoliticalLabelsModel
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


def load_posts_to_classify(
    num_days_lookback: Optional[int] = 1,
    feed_source: Optional[Literal["firehose", "most_liked"]] = None
) -> list[FilteredPreprocessedPostModel]:
    """Load posts for Perspective API classification. Load only the posts that
    haven't been classified yet.
    """
    if num_days_lookback:
        latest_preprocessing_timestamp = (
            datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
        ).strftime("%Y-%m-%d")
    else:
        latest_preprocessing_timestamp = None
    preprocessed_posts: list[FilteredPreprocessedPostModel] = get_filtered_posts(  # noqa
        latest_preprocessing_timestamp=latest_preprocessing_timestamp,
        feed_source=feed_source
    )
    existing_uris: set[str] = get_existing_sociopolitical_uris(
        latest_preprocessing_timestamp=latest_preprocessing_timestamp,
        feed_source=feed_source
    )
    if not existing_uris:
        return preprocessed_posts
    posts = [
        post for post in preprocessed_posts if post.uri not in existing_uris
    ]
    # sort by synctimestamp ascending so the oldest posts are first.
    sorted_posts = sorted(posts, key=lambda x: x.synctimestamp, reverse=False)
    return sorted_posts


def classify_latest_posts():
    # load posts
    posts: list[FilteredPreprocessedPostModel] = load_posts_to_classify()
    print(f"Number of posts loaded for sociopolitical classification: {len(posts)}")  # noqa

    # fetch metadata of posts to be classified. Insert the metadata to DBs
    # (if not already present)
    posts_to_classify: list[RecordClassificationMetadataModel] = (
        get_post_metadata_for_classification(posts)
    )
    batch_insert_metadata(posts_to_classify)

    # validate posts
    valid_posts, invalid_posts = validate_posts(posts_to_classify)
    print(f"Number of valid posts: {len(valid_posts)}")
    print(f"Number of invalid posts: {len(invalid_posts)}")
    print(f"Classifying {len(valid_posts)} posts for sociopolitical characteristics, via LLMs...")  # noqa
    print(f"Defaulting {len(invalid_posts)} posts to failed label...")

    # insert invalid posts into DB first, before running LLM sociopolitical
    # classification
    invalid_posts_models = []
    for post in invalid_posts:
        invalid_posts_models.append(
            SociopoliticalLabelsModel(
                uri=post["uri"],
                text=post["text"],
                was_successfully_labeled=False,
                reason="text_too_short",
                label_timestamp=current_datetime_str,
            )
        )

    print(f"Inserting {len(invalid_posts_models)} invalid posts into the DB.")
    batch_insert_sociopolitical_labels(invalid_posts_models)
    print(f"Completed inserting {len(invalid_posts_models)} invalid posts into the DB.")  # noqa

    # run inference on valid posts
    print(f"Running batch classification on {len(valid_posts)} valid posts.")
    run_batch_classification(posts=valid_posts)
    print("Completed batch classification.")
