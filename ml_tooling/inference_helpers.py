"""Generic helpers related to inference."""

import multiprocessing
import time
from typing import Callable, Optional, Union

from lib.helper import create_batches, track_performance
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa


def batch_classify_posts(
    batch: list[Union[dict, ConsolidatedPostRecordModel]],
    clf_func: Callable,
    rate_limit_per_minute: Optional[int],
) -> list[dict]:
    """Classifies posts in batches.

    Respects appropriate rate limits per call, if applicable.
    """
    seconds_per_request = 60 / rate_limit_per_minute if rate_limit_per_minute else None
    classified_posts: list[dict] = []
    for post in batch:
        classified_posts.append(clf_func(post))
        if seconds_per_request:
            time.sleep(seconds_per_request)
    return classified_posts


def batch_classify_posts_multiprocessing(
    batch: list[Union[dict, ConsolidatedPostRecordModel]],
    clf_func: Callable,
    rate_limit_per_minute: Optional[int] = None,
) -> list[dict]:
    """Classifies posts in batches using multiprocessing.

    Respects appropriate rate limits per call, if applicable.
    """
    seconds_per_request = 60 / rate_limit_per_minute if rate_limit_per_minute else None
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        classified_posts: list[dict] = pool.map(lambda post: clf_func(post), batch)
        if seconds_per_request:
            time.sleep(seconds_per_request)
    return classified_posts


@track_performance
def classify_posts(
    posts: list[Union[dict, ConsolidatedPostRecordModel]],
    clf_func: Callable,
    batch_size: Optional[int] = None,
    rate_limit_per_minute: Optional[int] = None,
    multiprocessing: Optional[bool] = False,
) -> list[dict]:
    """Classifies posts."""
    if batch_size:
        batches: list[list] = create_batches(batch_list=posts, batch_size=batch_size)
    else:
        batches = [posts]
    classified_posts: list[dict] = []
    for batch in batches:
        if multiprocessing:
            classified_posts.extend(
                batch_classify_posts_multiprocessing(
                    batch=batch,
                    clf_func=clf_func,
                    rate_limit_per_minute=rate_limit_per_minute,
                )
            )
        else:
            classified_posts.extend(
                batch_classify_posts(
                    batch=batch,
                    clf_func=clf_func,
                    rate_limit_per_minute=rate_limit_per_minute,
                )
            )
    return classified_posts
