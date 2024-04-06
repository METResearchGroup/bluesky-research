"""Generic helpers related to inference."""
import time
from typing import Optional


def generate_batches_of_posts(
    posts: list[dict], batch_size: int
) -> list[list[dict]]:
    """Generates batches of posts."""
    batches: list[list[dict]] = []
    for i in range(0, len(posts), batch_size):
        batches.append(posts[i:i + batch_size])
    return batches


def batch_classify_posts(
    batch: list[dict],
    clf_func: callable,
    rate_limit_per_minute: Optional[int] = 15
) -> list[dict]:
    """Classifies posts in batches.

    Respects appropriate rate limits per call.
    """
    seconds_per_request = 60 / rate_limit_per_minute
    classified_posts: list[dict] = []
    for post in batch:
        classified_posts.append(clf_func(post))
        time.sleep(seconds_per_request)
    return classified_posts


def classify_posts(
    posts: list[dict], clf_func: callable, batch_size: int
) -> list[dict]:
    """Classifies posts."""
    batches: list[list[dict]] = generate_batches_of_posts(
        posts=posts, batch_size=batch_size
    )
    classified_posts: list[dict] = []
    for batch in batches:
        classified_posts.extend(
            batch_classify_posts(batch=batch, clf_func=clf_func)
        )
    return classified_posts