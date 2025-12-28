"""Classifies the language of a post."""

from multiprocessing import Pool, cpu_count
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd

from services.preprocess_raw_data.classify_language.model import classify

if TYPE_CHECKING:  # pragma: no cover
    from lib.db.bluesky_models.transformations import TransformedRecordModel
else:  # pragma: no cover
    TransformedRecordModel = Any

try:
    # `lib.helper` pulls in optional heavy deps (atproto, etc). This service can
    # still function without them, so treat the decorator as optional.
    from lib.helper import track_performance  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    def track_performance(func):  # type: ignore
        return func


def classify_single_post(post) -> dict:
    """Classifies the language of a single post.

    NOTE: We intentionally do *not* trust upstream `langs` labels (e.g. "en")
    because we've observed false positives. We always run our FastText-based
    classifier and treat it as authoritative.
    """
    is_english: bool = text_is_english(post.text)
    return {"uri": post.uri, "is_english": is_english}


def preprocess_text_for_filtering(text: str) -> str:
    return text.replace("\n", " ").strip()


def text_is_english(text: str) -> bool:
    preprocessed_text = preprocess_text_for_filtering(text)
    return classify(preprocessed_text)


def record_is_english(record: TransformedRecordModel) -> bool:
    return text_is_english(record.text)


@track_performance
def filter_text_is_english_parallel(texts: pd.Series) -> pd.Series:
    """Experiments with parallelizing the language classification.

    It does greatly reduce runtime, depending on number of cores, but it also
    takes more memory (presumably having to duplicate the data and/or model?
    Unsure, but something worth considering.
    """
    num_cores = cpu_count()

    print(f"Filtering {len(texts)} texts with {num_cores} cores")

    with Pool(num_cores) as pool:
        results = pool.map(classify, texts)

    return pd.Series(results, index=texts.index)


@track_performance
def filter_text_is_english_serial(texts: pd.Series) -> pd.Series:
    return texts.apply(classify)


@track_performance
def filter_text_is_english(
    texts: pd.Series, mode: Literal["serial", "parallel"] = "serial"
) -> pd.Series:
    """Default function for filtering text is english.

    Can also be used to run the serial or parallel version.
    """
    if mode == "serial":
        return filter_text_is_english_serial(texts)
    elif mode == "parallel":
        return filter_text_is_english_parallel(texts)
    else:
        raise ValueError(
            f"Invalid mode: {mode}. Please specify 'serial' or 'parallel'."
        )
