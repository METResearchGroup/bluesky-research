"""Classifies posts as written by a possible bot."""
from typing import Optional

from ml_tooling.inference_helpers import classify_posts
from services.filter_raw_data.classify_bots.model import classify

DEFAULT_BATCH_SIZE = None # we can do inference without batching


def classify_single_post(post: dict) -> dict:
    """Classifies a single post as written by a possible bot."""
    is_from_possible_bot_account: bool = classify(post=post)
    return {
        "uid": post["uid"],
        "is_from_possible_bot_account": is_from_possible_bot_account
    }


def classify_if_posts_from_bot_accounts(
    posts: list[dict], batch_size: Optional[int]=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies the language of multiple posts."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post,
        batch_size=batch_size, rate_limit_per_minute=None
    )