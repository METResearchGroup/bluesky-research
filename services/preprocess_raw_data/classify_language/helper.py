"""Classifies the language of a post."""
from lib.db.bluesky_models.transformations import TransformedRecordModel
from ml_tooling.inference_helpers import classify_posts
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_language.model import classify

# our model is fast enough to handle all posts at once without special batching
DEFAULT_BATCH_SIZE = None


def classify_single_post(post: ConsolidatedPostRecordModel) -> dict:
    """Classifies the language of a single post.

    If we have metadata for the language of the post via the Bluesky firehose,
    we'll use that. Otherwise, we'll use our model to classify the language.
    """
    langs = post.langs
    if langs:
        langs: list[str] = langs.split(",")
        text_is_english = True if "en" in langs else False
    else:
        text_is_english: bool = classify(post.text)
    return {"uri": post.uri, "is_english": text_is_english}


def preprocess_text_for_filtering(text: str) -> str:
    return text.replace("\n", " ").strip()


def text_is_english(text: str) -> bool:
    preprocessed_text = preprocess_text_for_filtering(text)
    return classify(preprocessed_text)


def record_is_english(record: TransformedRecordModel) -> bool:
    langs: str = record.langs
    if langs:
        return "en" in langs
    else:
        print(f"Classifying language for record with text: {record.text}")
    return text_is_english(record.text)


def classify_language_of_posts(
    posts: list[ConsolidatedPostRecordModel],
    batch_size: int = DEFAULT_BATCH_SIZE
) -> list[dict]:
    """Classifies the language of multiple posts."""
    return classify_posts(
        posts=posts, clf_func=classify_single_post,
        batch_size=batch_size, rate_limit_per_minute=None
    )
