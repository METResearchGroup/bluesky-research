"""Base file for classifying posts in batch using the Perspective API."""
from datetime import datetime, timedelta, timezone

from lib.constants import current_datetime_str, timestamp_format
from ml_tooling.perspective_api.model import run_batch_classification
from pipelines.classify_records.helper import validate_posts
from services.ml_inference.models import PerspectiveApiLabelsModel
from services.ml_inference.perspective_api.export_data import (
    export_results, write_post_to_cache
)
from services.ml_inference.perspective_api.helper import filter_posts_already_in_cache  # noqa
from services.ml_inference.perspective_api.load_data import (
    load_posts_to_classify, load_previous_session_metadata,
    load_previously_classified_post_uris
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


# by default, process the posts from the past day, if we don't have a
# previous timestamp to work with.
num_days_lookback = 1
default_latest_timestamp = (
    datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
).strftime(timestamp_format)


def init_session_metadata(
    previous_timestamp: str, source_feeds: list[str]
):
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
    source_feeds = ["firehose", "most_liked"]
    previous_classified_post_uris: set[str] = (
        load_previously_classified_post_uris(source="s3")
    )
    # assumes that I'll classify both firehose and most liked posts in the
    # same session, otherwise this causes complications since I'll have to
    # manage the session metadata separately for each type.
    previous_session_metadata: dict = load_previous_session_metadata()
    previous_timestamp = (
        previous_session_metadata["current_classification_timestamp"]
        if previous_session_metadata
        else None
    )
    session_metadata: dict = init_session_metadata(
        previous_timestamp=previous_timestamp,
        source_feeds=source_feeds
    )
    if not previous_timestamp:
        previous_timestamp = default_latest_timestamp
    for source_feed in source_feeds:
        print(f"Loading posts from {source_feed}...")
        posts: list[FilteredPreprocessedPostModel] = load_posts_to_classify(
            source="local", source_feed=source_feed,
            previous_timestamp=previous_timestamp,
        )
        # validate posts
        valid_posts, invalid_posts = validate_posts(posts)  # TODO: needs update
        print(f"Number of valid posts: {len(valid_posts)}")
        print(f"Number of invalid posts: {len(invalid_posts)}")
        print(f"Classifying {len(valid_posts)} posts via Perspective API...")
        print(f"Defaulting {len(invalid_posts)} posts to failed label...")

        session_metadata[source_feed]["num_posts_loaded"] = len(posts)
        session_metadata[source_feed]["num_valid_posts_for_classification"] = len(valid_posts)  # noqa
        session_metadata[source_feed]["num_invalid_posts_for_classification"] = len(invalid_posts)  # nqoa
        # workaround and decently good assumption that the number of posts
        # classified with the API is the number of valid posts.
        session_metadata[source_feed]["num_posts_classified_with_api"] = len(valid_posts)  # noqa

        posts_after_cache_removal: dict = filter_posts_already_in_cache(
            valid_posts=valid_posts,
            invalid_posts=invalid_posts,
            source_feed=source_feed
        )

        valid_posts: list[FilteredPreprocessedPostModel] = posts_after_cache_removal["valid_posts"]  # noqa
        invalid_posts: list[FilteredPreprocessedPostModel] = posts_after_cache_removal["invalid_posts"]  # noqa

        num_posts_removed = len(posts) - len(valid_posts) - len(invalid_posts)
        if num_posts_removed > 0:
            print(f"Removed {num_posts_removed} posts that were already in cache.")  # noqa
            print(f"Number of valid posts after cache removal: {len(valid_posts) + len(invalid_posts)}")  # noqa

        if len(valid_posts) + len(invalid_posts) == 0:
            print(f"No posts to classify for {source_feed} feed. Skipping...")
            continue

        # process and classify invalid posts first.
        invalid_posts_models = []
        for post in invalid_posts:
            invalid_posts_models.append(
                PerspectiveApiLabelsModel(
                    uri=post.uri,
                    text=post.text,
                    was_successfully_labeled=False,
                    reason="text_too_short",
                    label_timestamp=current_datetime_str,
                )
            )

        for classified_post in invalid_posts_models:
            write_post_to_cache(
                classified_post=classified_post,
                source_feed=source_feed,
                classification_type="invalid"
            )

        run_batch_classification(
            posts=valid_posts, source_feed=source_feed
        )

    export_results(
        previous_classified_post_uris=previous_classified_post_uris,
        session_metadata=session_metadata,
        external_stores=["local", "s3"]
    )
    print("Classification complete.")


if __name__ == "__main__":
    classify_latest_posts()
    print("Perspective API classification complete.")
