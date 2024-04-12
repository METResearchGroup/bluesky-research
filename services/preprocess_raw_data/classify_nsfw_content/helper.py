"""Classifying NSFW content."""
from ml_tooling.inference_helpers import classify_posts
from services.preprocess_raw_data.classify_nsfw_content.model import classify


# our model is fast enough to handle all posts at once without special batching
DEFAULT_BATCH_SIZE = None


def classify_single_post(post: dict) -> dict:
    """Classifies post as NSFW or not."""
    post_is_nsfw: bool = classify(post=post)
    return {"uri": post["uri"], "has_no_nsfw_content": not post_is_nsfw}


def classify_if_posts_have_no_nsfw_content(
    posts: list[dict], batch_size: int=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies multiple posts."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post,
        batch_size=batch_size, rate_limit_per_minute=None
    )
