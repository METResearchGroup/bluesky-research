"""Classifies posts as written by a possible bot."""

from lib.helper import track_performance

import pandas as pd

from services.preprocess_raw_data.classify_bots.model import (
    bot_user_dids,
    bot_user_handles,
)


@track_performance
def filter_posts_written_by_bot_accounts(
    author_dids: pd.Series,
    author_handles: pd.Series,
) -> pd.Series:
    """Classifies if posts come from bots."""
    is_bot_did = author_dids.isin(bot_user_dids)
    is_bot_handle = author_handles.isin(bot_user_handles)
    return is_bot_did | is_bot_handle
