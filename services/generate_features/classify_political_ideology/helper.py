"""Classifies the political ideology of posts."""
from typing import Optional

from services.generate_features.classify_political_ideology.model import classify # noqa

DEFAULT_BATCH_SIZE = 100


def generate_batches_of_posts(
    posts: list[dict], batch_size: int
) -> list[list[dict]]:
    """Generates batches of posts."""
    batches: list[list[dict]] = []
    for i in range(0, len(posts), batch_size):
        batches.append(posts[i:i + batch_size])
    return batches


def classify_single_post(post: dict) -> dict:
    """Classifies a single post."""
    prob_and_label_dict: dict = classify(post)
    return {
        **post,
        **prob_and_label_dict
    }


def batch_classify_political_ideology_of_posts(
    posts: list[dict]
) -> list[dict]:
    """Classifies posts in batches."""
    classified_posts: list[dict] = []
    for post in posts:
        classified_posts.append(classify_single_post(post))
    return classified_posts


def classify_political_ideology_of_posts(
    posts: list[dict], batch_size: Optional[int]=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies the political ideology of posts."""
    batches: list[list[dict]] = generate_batches_of_posts(
        posts=posts, batch_size=batch_size
    )
    classified_posts: list[dict] = []
    for batch in batches:
        classified_posts.extend(
            batch_classify_political_ideology_of_posts(batch)
        )
    return classified_posts
