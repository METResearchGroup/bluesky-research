"""Inference for the IME classification model."""

from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

default_batch_size = 32


def batch_classify_posts(
    posts: list[FilteredPreprocessedPostModel], batch_size: int = default_batch_size
):
    pass


def run_batch_classification():
    pass
