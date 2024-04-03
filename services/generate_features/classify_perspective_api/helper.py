"""Classifies posts using the Perspective API."""
from services.generate_features.helper import classify_posts
from services.generate_features.classify_perspective_api.model import classify


DEFAULT_BATCH_SIZE = 10

def classify_single_post(post: dict) -> dict:
    """Classifies a single post."""
    prob_and_label_dict: dict = classify(post)
    return {
        "uid": post["uid"],
        "prob": prob_and_label_dict["prob"],
        "label": prob_and_label_dict["label"]
    }


def classify_posts_using_perspective_api(
    posts: list[dict], batch_size: int=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies posts using the Perspective API from Google Jigsaw."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post, batch_size=batch_size
    )
