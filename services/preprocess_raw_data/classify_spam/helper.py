"""Classifies whether a post contains possible spam."""
from typing import Optional

from ml_tooling.inference_helpers import classify_posts
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_spam.model import classify

DEFAULT_BATCH_SIZE = None  # we can do inference without batching


def classify_single_post(post: ConsolidatedPostRecordModel) -> dict:
    """Classifies a single post as having spam."""
    has_spam: bool = classify(post=post)
    return {"uri": post.uri, "has_no_spam": not has_spam}


def classify_if_posts_have_no_spam(
    posts: list[ConsolidatedPostRecordModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies if posts have spam."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post,
        batch_size=batch_size, rate_limit_per_minute=None
    )
