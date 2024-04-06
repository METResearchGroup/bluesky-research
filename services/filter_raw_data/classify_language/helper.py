"""Classifies the language of a post."""
from ml_tooling.inference_helpers import classify_posts
from services.filter_raw_data.classify_language.model import classify


DEFAULT_BATCH_SIZE = 100 # TODO: check behavior of our language model.


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
    lang = classify(post["text"])
    return {
        "uid": post["uid"],
        "is_english": True if lang == "en" else False
    }


def classify_language_of_posts(
    posts: list[dict], batch_size: int=DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies the language of multiple posts."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post, batch_size=batch_size
    )
