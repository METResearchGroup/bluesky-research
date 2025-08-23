"""Common processing utilities for analytics processing.

This module provides shared utility functions used across multiple processing
modules to avoid code duplication and ensure consistent behavior.
"""

import pandas as pd

from lib.log.logger import get_logger

logger = get_logger(__file__)


def calculate_probability_threshold_proportions(
    probability_series: pd.Series, threshold: float = 0.5
) -> float:
    """Calculate the proportion of values above a threshold in a probability series.

    Args:
        probability_series: Series containing probability values
        threshold: Probability threshold for binary classification (default: 0.5)

    Returns:
        Proportion of values above the threshold
    """
    if probability_series.empty:
        return 0.0

    # Filter out NaN values
    valid_probs = probability_series.dropna()

    if valid_probs.empty:
        return 0.0

    # Calculate proportion above threshold
    above_threshold = (valid_probs >= threshold).sum()
    total_valid = len(valid_probs)

    return above_threshold / total_valid if total_valid > 0 else 0.0


def calculate_label_proportions(label_series: pd.Series, label_values: list) -> dict:
    """Calculate proportions for categorical label values.

    Args:
        label_series: Series containing categorical labels
        label_values: List of label values to calculate proportions for

    Returns:
        Dictionary mapping label values to their proportions
    """
    if label_series.empty:
        return {f"prop_is_{label}": 0.0 for label in label_values}

    total_rows = len(label_series)
    proportions = {}

    for label in label_values:
        count = label_series.eq(label).sum()
        proportions[f"prop_is_{label}"] = count / total_rows

    return proportions


def calculate_political_proportions(
    political_series: pd.Series, total_rows: int
) -> dict:
    """Calculate political ideology proportions from a series.

    Args:
        political_series: Series containing political ideology labels
        total_rows: Total number of rows for proportion calculation

    Returns:
        Dictionary containing political proportions
    """
    proportions = {}

    # Calculate proportions for each political category
    for ideology in ["left", "right", "moderate", "unclear"]:
        count = (political_series.fillna("").eq(ideology)).sum()
        proportions[f"prop_is_political_{ideology}"] = count / total_rows

    return proportions


def safe_mean(series: pd.Series, default: float = 0.0) -> float:
    """Safely calculate the mean of a series, handling empty/NaN cases.

    Args:
        series: Series to calculate mean for
        default: Default value to return if calculation fails

    Returns:
        Mean value or default if calculation fails
    """
    try:
        if series.empty:
            return default

        valid_series = series.dropna()
        if valid_series.empty:
            return default

        return float(valid_series.mean())
    except Exception as e:
        logger.warning(f"Error calculating mean: {e}")
        return default


def safe_sum(series: pd.Series, default: float = 0.0) -> float:
    """Safely calculate the sum of a series, handling empty/NaN cases.

    Args:
        series: Series to calculate sum for
        default: Default value to return if calculation fails

    Returns:
        Sum value or default if calculation fails
    """
    try:
        if series.empty:
            return default

        valid_series = series.dropna()
        if valid_series.empty:
            return default

        return float(valid_series.sum())
    except Exception as e:
        logger.warning(f"Error calculating sum: {e}")
        return default


def validate_probability_series(series: pd.Series) -> bool:
    """Validate that a series contains valid probability values.

    Args:
        series: Series to validate

    Returns:
        True if series contains valid probabilities, False otherwise
    """
    if series.empty:
        return False

    # Check if all values are between 0 and 1
    valid_range = (series >= 0) & (series <= 1)
    if not valid_range.all():
        return False

    # Check if values are numeric
    if not pd.api.types.is_numeric_dtype(series):
        return False

    return True


def normalize_probabilities(series: pd.Series, method: str = "minmax") -> pd.Series:
    """Normalize probability values using specified method.

    Args:
        series: Series containing probability values
        method: Normalization method ('minmax', 'zscore', 'robust')

    Returns:
        Normalized series
    """
    if series.empty:
        return series

    if method == "minmax":
        min_val = series.min()
        max_val = series.max()
        if max_val > min_val:
            return (series - min_val) / (max_val - min_val)
        return series

    elif method == "zscore":
        mean_val = series.mean()
        std_val = series.std()
        if std_val > 0:
            return (series - mean_val) / std_val
        return series

    elif method == "robust":
        median_val = series.median()
        mad_val = (series - median_val).abs().median()
        if mad_val > 0:
            return (series - median_val) / mad_val
        return series

    else:
        logger.warning(f"Unknown normalization method: {method}")
        return series


def calculate_percentile_threshold(series: pd.Series, percentile: float) -> float:
    """Calculate a threshold value at a specific percentile.

    Args:
        series: Series to calculate percentile for
        percentile: Percentile value (0-100)

    Returns:
        Threshold value at the specified percentile
    """
    if series.empty:
        return 0.0

    try:
        return float(series.quantile(percentile / 100))
    except Exception as e:
        logger.warning(f"Error calculating percentile threshold: {e}")
        return 0.0


def filter_by_threshold(
    series: pd.Series, threshold: float, operator: str = ">="
) -> pd.Series:
    """Filter a series by a threshold using specified operator.

    Args:
        series: Series to filter
        threshold: Threshold value
        operator: Comparison operator ('>', '>=', '<', '<=', '==', '!=')

    Returns:
        Filtered series
    """
    if series.empty:
        return series

    if operator == ">":
        return series[series > threshold]
    elif operator == ">=":
        return series[series >= threshold]
    elif operator == "<":
        return series[series < threshold]
    elif operator == "<=":
        return series[series <= threshold]
    elif operator == "==":
        return series[series == threshold]
    elif operator == "!=":
        return series[series != threshold]
    else:
        logger.warning(f"Unknown operator: {operator}")
        return series
