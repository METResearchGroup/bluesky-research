"""Preprocesses raw data before filtering."""
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_language.helper import preprocess_text_for_filtering  # noqa
from services.preprocess_raw_data.filters import filter_posts


def preprocess_post(
    post: ConsolidatedPostRecordModel
) -> ConsolidatedPostRecordModel:
    """Preprocesses a single post as necessary, before filtering."""
    # preprocessing needed for language classifier. Specifically, removes any
    # newline chars, which the classifier doesn't like.
    post_text = post.record.text
    processed_text = preprocess_text_for_filtering(post_text)
    post.record.text = processed_text
    return post


def preprocess_latest_posts(
    latest_posts: list[ConsolidatedPostRecordModel]
) -> tuple[list[ConsolidatedPostRecordModel], dict]:
    """Preprocesses and filters posts."""
    res: list[ConsolidatedPostRecordModel] = [
        preprocess_post(post) for post in latest_posts
    ]
    filtered_posts, updated_posts_metadata = filter_posts(res)
    return filtered_posts, updated_posts_metadata


def preprocess_latest_likes(latest_likes):
    return [], {"num_likes": len(latest_likes)}


def preprocess_latest_follows(latest_follows):
    return [], {"num_follows": len(latest_follows)}
