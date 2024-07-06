"""Preprocesses raw data before filtering."""
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_language.helper import preprocess_text_for_filtering  # noqa
from services.preprocess_raw_data.filters import filter_posts
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


def preprocess_post(
    post: ConsolidatedPostRecordModel
) -> ConsolidatedPostRecordModel:
    """Preprocesses a single post as necessary, before filtering."""
    # preprocessing needed for language classifier. Specifically, removes any
    # newline chars, which the classifier doesn't like.
    processed_text = preprocess_text_for_filtering(post.text)
    post.text = processed_text
    return post


def preprocess_latest_posts(
    latest_posts: list[ConsolidatedPostRecordModel]
) -> tuple[list[FilteredPreprocessedPostModel], dict]:
    """Preprocesses and filters posts."""
    lst: list[ConsolidatedPostRecordModel] = [
        preprocess_post(post) for post in latest_posts
    ]
    passed_posts, updated_posts_metadata = filter_posts(lst)
    return passed_posts,  updated_posts_metadata


def preprocess_latest_likes(latest_likes):
    return [], {"num_likes": len(latest_likes)}


def preprocess_latest_follows(latest_follows):
    return [], {"num_follows": len(latest_follows)}
