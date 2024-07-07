"""Base file for classifying posts in batch using the Perspective API."""
from lib.constants import current_datetime_str
from ml_tooling.perspective_api.model import run_batch_classification
from pipelines.classify_records.helper import validate_posts
from services.ml_inference.models import (
    PerspectiveApiLabelsModel
)
from services.ml_inference.perspective_api.export_data import (
    export_results, write_post_to_cache
)
from services.ml_inference.perspective_api.helper import filter_posts_already_in_cache  # noqa
from services.ml_inference.perspective_api.load_data import (
    load_posts_to_classify, load_previous_session_metadata
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


def init_session_metadata(previous_timestamp: str):
    """Initializes the session metadata for the classification session."""
    return {
        "source_feeds": ["firehose", "most_liked"],
        "current_classification_timestamp": current_datetime_str,
        "firehose": {
            "classification_type": "perspective_api",
            "previous_classification_timestamp": previous_timestamp,
            "current_classification_timestamp": current_datetime_str,
            "num_posts_loaded": 0,
            "num_valid_posts_for_classification": 0,
            "num_invalid_posts_for_classification": 0,
            "num_posts_classified": 0,
        },
        "most_liked": {
            "classification_type": "perspective_api",
            "previous_classification_timestamp": previous_timestamp,
            "current_classification_timestamp": current_datetime_str,
            "num_posts_loaded": 0,
            "num_valid_posts_for_classification": 0,
            "num_invalid_posts_for_classification": 0,
            "num_posts_classified": 0,
        }
    }


def classify_latest_posts():
    source_feeds = ["firehose", "most_liked"]
    previous_classified_post_uris = set()  # TODO: import
    for source_feed in source_feeds:
        print(f"Loading posts from {source_feed}...")
        # TODO: load posts from either s3 or local
        # TODO: load either firehose or most liked posts
        posts: list[FilteredPreprocessedPostModel] = load_posts_to_classify()
        previous_session_metadata: dict = load_previous_session_metadata()
        session_metadata: dict = init_session_metadata(
            previous_timestamp=previous_session_metadata["current_classification_timestamp"]  # noqa
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

        posts_after_cache_removal: dict = filter_posts_already_in_cache(
            valid_posts=valid_posts,
            invalid_posts=invalid_posts,
            source_feed=source_feed
        )

        if len(posts) != len(posts_after_cache_removal):
            num_posts_removed = len(posts) - len(posts_after_cache_removal)
            print(f"Removed {num_posts_removed} posts that were already in cache.")  # noqa
            print(f"Number of valid posts after cache removal: {len(posts_after_cache_removal)}")  # noqa

        valid_posts = posts_after_cache_removal["valid_posts"]
        invalid_posts = posts_after_cache_removal["invalid_posts"]

        # process and classify invalid posts first.
        invalid_posts_models = []
        for post in invalid_posts:
            invalid_posts_models.append(
                PerspectiveApiLabelsModel(
                    uri=post["uri"],
                    text=post["text"],
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

        # TODO: I should test up until this point and verify that at least
        # the correct data is being loaded. Then and only then should I then
        # continue and actually do inference.

        # TODO: update to just write to cache. Also need to make sure that
        # the classifed URIs set keeps getting updated (and that that is
        # also exported.
        run_batch_classification(posts=valid_posts)

    export_results(
        previous_classified_post_uris=previous_classified_post_uris,
        session_metadata=session_metadata,
        external_stores=["local", "s3"]
    )
    print("Classification complete.")
