"""Classifies posts using the Perspective API."""
from ml_tooling.inference_helpers import classify_posts
from services.generate_features.classify_perspective_api.model import classify


DEFAULT_BATCH_SIZE = 10


def classify_single_post(post: dict) -> dict:
    """Classifies a single post.

    Returns multiple classifications from the Perspective API, depending on
    what combination of features we decide to use.
    """
    probs_and_labels_dict: dict = classify(post)
    return {
        **{"uid": post["uid"]},
        **probs_and_labels_dict
    }


def classify_posts_using_perspective_api(
    posts: list[dict], batch_size: int = DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies posts using the Perspective API from Google Jigsaw."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post, batch_size=batch_size
    )
