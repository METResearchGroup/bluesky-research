"""Classifies the language of a post."""
from ml_tooling.inference_helpers import classify_posts
from services.filter_raw_data.classify_language.model import classify


# our model is fast enough to handle all posts at once without special batching
DEFAULT_BATCH_SIZE = None


def classify_single_post(post: dict) -> dict:
    """Classifies the language of a single post.
    
    If we have metadata for the language of the post via the Bluesky firehose,
    we'll use that. Otherwise, we'll use our model to classify the language.
    """
    langs = post.get("langs", None)
    if langs:
        langs: list[str] = langs.split(",")
        return {
            "uid": post["uid"],
            "is_english": True if "en" in langs else False
        }
    text_is_english: bool = classify(post["text"])
    return {"uid": post["uid"], "is_english": text_is_english}


def classify_language_of_posts(
    posts: list[dict], batch_size: int=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies the language of multiple posts."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post,
        batch_size=batch_size, rate_limit_per_minute=None
    )
