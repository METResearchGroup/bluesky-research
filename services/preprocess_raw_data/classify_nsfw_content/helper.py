"""Classify if a post has any NSFW content."""

import pandas as pd

from lib.helper import track_performance
from services.preprocess_raw_data.classify_nsfw_content.constants import (
    LABELS_TO_FILTER,
)
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    load_users_to_exclude,
)

users_to_exclude = load_users_to_exclude()
bsky_handles_to_exclude = users_to_exclude["bsky_handles_to_exclude"]
bsky_dids_to_exclude = users_to_exclude["bsky_dids_to_exclude"]


@track_performance
def filter_post_content_nsfw(texts: pd.Series, labels: pd.Series) -> pd.Series:
    """Classifies if posts have any NSFW content."""
    labels_check = labels.isin(LABELS_TO_FILTER)
    texts_check = texts.isin(LABELS_TO_FILTER)
    return labels_check | texts_check


@track_performance
def filter_post_author_nsfw(
    author_dids: pd.Series, author_handles: pd.Series
) -> pd.Series:
    """Classifies if posts come from NSFW authors."""
    return author_dids.isin(bsky_dids_to_exclude) | author_handles.isin(
        bsky_handles_to_exclude
    )


if __name__ == "__main__":
    pass
