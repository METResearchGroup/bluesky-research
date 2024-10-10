"""Classify if a post has any NSFW content."""

import pandas as pd

from services.preprocess_raw_data.classify_nsfw_content.constants import (
    LABELS_TO_FILTER,
)
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    load_users_to_exclude,
)

users_to_exclude = load_users_to_exclude()
bsky_handles_to_exclude = users_to_exclude["bsky_handles_to_exclude"]
bsky_dids_to_exclude = users_to_exclude["bsky_dids_to_exclude"]


# NOTE: check the vectorization on this.
def filter_post_content_nsfw(texts: pd.Series, labels: pd.Series) -> pd.Series:
    labels_check = labels.apply(
        lambda x: any(
            label_to_filter in x.split(",") for label_to_filter in LABELS_TO_FILTER
        )
        if x
        else False
    )
    texts_check = texts.apply(
        lambda x: any(label_to_filter in x for label_to_filter in LABELS_TO_FILTER)
    )
    return labels_check | texts_check


def filter_post_author_nsfw(
    author_dids: pd.Series, author_handles: pd.Series
) -> pd.Series:
    return author_dids.isin(bsky_dids_to_exclude) | author_handles.isin(
        bsky_handles_to_exclude
    )


if __name__ == "__main__":
    pass
