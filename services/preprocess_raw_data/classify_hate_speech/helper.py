"""Classifies whether a post contains hate speech."""

import pandas as pd

from lib.helper import track_performance


@track_performance
def filter_posts_have_hate_speech(texts: pd.Series) -> pd.Series:
    """Filters posts that have hate speech."""
    # TODO: implement more fully.
    return False
