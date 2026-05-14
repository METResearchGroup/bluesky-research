"""Service-level defaults for feed-day ↔ preprocessed joins."""

from lib.constants import FEED_LOOKBACK_DAYS_DURING_STUDY
from services.calculate_analytics.shared.constants import default_min_lookback_date

default_num_days_lookback = FEED_LOOKBACK_DAYS_DURING_STUDY

__all__ = [
    "default_min_lookback_date",
    "default_num_days_lookback",
]
