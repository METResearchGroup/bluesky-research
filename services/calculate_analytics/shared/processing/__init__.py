"""Shared processing modules for analytics system.

This module provides unified interfaces for processing various types of data
used across the analytics system, eliminating code duplication and
ensuring consistent processing patterns.
"""

from .features import (
    calculate_feature_averages,
    calculate_feature_proportions,
    calculate_political_averages,
    calculate_valence_averages,
)
from .thresholds import (
    map_date_to_static_week,
    map_date_to_dynamic_week,
    get_week_thresholds_per_user_static,
    get_week_thresholds_per_user_dynamic,
    get_latest_survey_timestamp_within_period,
)
from .engagement import (
    get_num_records_per_user_per_day,
    aggregate_metrics_per_user_per_day,
)
from .utils import (
    calculate_probability_threshold_proportions,
    calculate_label_proportions,
    calculate_political_proportions,
)

__all__ = [
    # Features
    "calculate_feature_averages",
    "calculate_feature_proportions",
    "calculate_political_averages",
    "calculate_valence_averages",
    # Thresholds
    "map_date_to_static_week",
    "map_date_to_dynamic_week",
    "get_week_thresholds_per_user_static",
    "get_week_thresholds_per_user_dynamic",
    "get_latest_survey_timestamp_within_period",
    # Engagement
    "get_num_records_per_user_per_day",
    "aggregate_metrics_per_user_per_day",
    # Utils
    "calculate_probability_threshold_proportions",
    "calculate_label_proportions",
    "calculate_political_proportions",
]
