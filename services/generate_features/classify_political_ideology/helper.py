"""Classifies the political ideology of posts."""
from ml_tooling.inference_helpers import classify_posts
from services.generate_features.classify_political_ideology.model import classify # noqa

DEFAULT_BATCH_SIZE = 100


def classify_single_post(post: dict) -> dict:
    """Classifies a single post."""
    prob_and_label_dict: dict = classify(post)
    return {
        "uid": post["uid"],
        "prob": prob_and_label_dict["prob"],
        "label": prob_and_label_dict["label"]
    }


def classify_political_ideology_of_posts(
    posts: list[dict], batch_size: int=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies the political ideology of posts."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post, batch_size=batch_size
    )
