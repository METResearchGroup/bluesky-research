"""Classifies whether a post contains hate speech."""
from typing import Optional

from ml_tooling.inference_helpers import classify_posts
from services.preprocess_raw_data.classify_hate_speech.model import classify

DEFAULT_BATCH_SIZE = None # we can do inference without batching


def classify_single_post(post: dict) -> dict:
    """Classifies a single post as having spam."""
    has_hate_speech: bool = classify(post=post)
    return {"uri": post["uri"], "has_no_hate_speech": not has_hate_speech}


def classify_if_posts_have_no_hate_speech(
    posts: list[dict], batch_size: Optional[int]=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies if posts have hate speech."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post,
        batch_size=batch_size, rate_limit_per_minute=None
    )
