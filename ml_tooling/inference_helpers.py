"""Generic helpers related to inference."""
import multiprocessing
import time
from typing import Optional

from lib.helper import track_performance


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
    rate_limit_per_minute: Optional[int]
) -> list[dict]:
    """Classifies posts in batches.

    Respects appropriate rate limits per call, if applicable.
    """
    seconds_per_request = (
        60 / rate_limit_per_minute if rate_limit_per_minute else None
    )
    classified_posts: list[dict] = []
    for post in batch:
        classified_posts.append(clf_func(post))
        if seconds_per_request:
            time.sleep(seconds_per_request)
    return classified_posts


def batch_classify_posts_multiprocessing(
    batch: list[dict],
    clf_func: callable,
    rate_limit_per_minute: Optional[int]=None
) -> list[dict]:
    """Classifies posts in batches using multiprocessing.

    Respects appropriate rate limits per call, if applicable.
    """
    seconds_per_request = (
        60 / rate_limit_per_minute if rate_limit_per_minute else None
    )
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        classified_posts: list[dict] = pool.map(lambda post: clf_func(post), batch)
        if seconds_per_request:
            time.sleep(seconds_per_request)
    return classified_posts


@track_performance
def classify_posts(
    posts: list[dict],
    clf_func: callable,
    batch_size: Optional[int]=None,
    rate_limit_per_minute: Optional[int]=None,
    multiprocessing: Optional[bool]=False
) -> list[dict]:
    """Classifies posts."""
    if batch_size:
        batches: list[list[dict]] = generate_batches_of_posts(
            posts=posts, batch_size=batch_size
        )
    else:
        batches: list[list[dict]] = [posts]

    classified_posts: list[dict] = []
    for batch in batches:
        if multiprocessing:
            classified_posts.extend(
                batch_classify_posts_multiprocessing(
                    batch=batch, clf_func=clf_func,
                    rate_limit_per_minute=rate_limit_per_minute
                )
            )
        else:
            classified_posts.extend(
                batch_classify_posts(
                    batch=batch, clf_func=clf_func,
                    rate_limit_per_minute=rate_limit_per_minute
                )
            )
    return classified_posts
