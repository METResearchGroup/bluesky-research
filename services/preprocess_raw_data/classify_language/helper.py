"""Classifies the language of a post."""

import pandas as pd

from lib.helper import track_performance
from lib.db.bluesky_models.transformations import TransformedRecordModel
from services.preprocess_raw_data.classify_language.model import classify


def classify_single_post(post) -> dict:
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


@track_performance
def filter_text_is_english(texts: pd.Series) -> pd.Series:
    return texts.apply(classify)
