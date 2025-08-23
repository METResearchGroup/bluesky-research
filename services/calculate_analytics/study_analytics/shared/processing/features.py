"""Feature calculation functions for analytics processing.

This module provides reusable functions for calculating various types of features
from post data, including toxicity, IME, political, and valence features.
"""

from typing import Dict

import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.shared.config import get_config
from services.calculate_analytics.study_analytics.shared.processing.utils import (
    calculate_probability_threshold_proportions,
)

logger = get_logger(__file__)


def calculate_feature_averages(posts_df: pd.DataFrame, user: str) -> Dict[str, float]:
    """Calculate average feature values for a user's posts.

    Args:
        posts_df: DataFrame containing posts with feature columns
        user: User identifier

    Returns:
        Dictionary containing average values for all features
    """
    config = get_config()
    feature_config = config.features

    # Calculate averages for each feature
    averages = {
        "user": user,
        "user_did": user,
    }

    # Add toxicity features
    for feature in feature_config.toxicity_features:
        col_name = f"prob_{feature}"
        if col_name in posts_df.columns:
            averages[f"avg_prob_{feature}"] = posts_df[col_name].dropna().mean()

    # Add IME features
    for feature in feature_config.ime_features:
        col_name = f"prob_{feature}"
        if col_name in posts_df.columns:
            averages[f"avg_prob_{feature}"] = posts_df[col_name].dropna().mean()

    # Add political and valence averages
    political_averages = calculate_political_averages(posts_df)
    valence_averages = calculate_valence_averages(posts_df)

    averages.update(political_averages)
    averages.update(valence_averages)

    return averages


def calculate_feature_proportions(
    posts_df: pd.DataFrame, user: str, threshold: float = 0.5
) -> Dict[str, float]:
    """Calculate feature proportions for a user's posts using threshold.

    Args:
        posts_df: DataFrame containing posts with feature columns
        user: User identifier
        threshold: Probability threshold for binary classification

    Returns:
        Dictionary containing proportions for all features
    """
    config = get_config()
    feature_config = config.features

    proportions = {
        "user": user,
        "user_did": user,
    }

    # Add toxicity feature proportions
    for feature in feature_config.toxicity_features:
        col_name = f"prob_{feature}"
        if col_name in posts_df.columns:
            proportions[f"prop_{feature}_posts"] = (
                calculate_probability_threshold_proportions(
                    posts_df[col_name], threshold
                )
            )

    # Add IME feature proportions
    for feature in feature_config.ime_features:
        col_name = f"prob_{feature}"
        if col_name in posts_df.columns:
            proportions[f"prop_{feature}_posts"] = (
                calculate_probability_threshold_proportions(
                    posts_df[col_name], threshold
                )
            )

    # Add political and valence proportions
    political_proportions = calculate_political_proportions(posts_df)
    valence_proportions = calculate_valence_averages(posts_df)

    proportions.update(political_proportions)
    proportions.update(valence_proportions)

    return proportions


def calculate_political_averages(posts_df: pd.DataFrame) -> Dict[str, float]:
    """Calculate political ideology averages from posts.

    Args:
        posts_df: DataFrame containing posts with political columns

    Returns:
        Dictionary containing political averages
    """
    total_rows = len(posts_df)
    avg_is_political = posts_df["is_sociopolitical"].dropna().mean()

    political_averages = {
        "avg_is_political": avg_is_political,
        "avg_is_not_political": 1 - avg_is_political,
        "avg_is_political_left": (
            posts_df["political_ideology_label"].fillna("").eq("left")
        ).sum()
        / total_rows,
        "avg_is_political_right": (
            posts_df["political_ideology_label"].fillna("").eq("right")
        ).sum()
        / total_rows,
        "avg_is_political_moderate": (
            posts_df["political_ideology_label"].fillna("").eq("moderate")
        ).sum()
        / total_rows,
        "avg_is_political_unclear": (
            posts_df["political_ideology_label"].fillna("").eq("unclear")
        ).sum()
        / total_rows,
    }

    return political_averages


def calculate_political_proportions(posts_df: pd.DataFrame) -> Dict[str, float]:
    """Calculate political ideology proportions from posts.

    Args:
        posts_df: DataFrame containing posts with political columns

    Returns:
        Dictionary containing political proportions
    """
    total_rows = len(posts_df)
    avg_is_political = posts_df["is_sociopolitical"].dropna().mean()

    political_proportions = {
        "prop_is_political": avg_is_political,
        "prop_is_not_political": 1 - avg_is_political,
        "prop_is_political_left": (
            posts_df["political_ideology_label"].fillna("").eq("left")
        ).sum()
        / total_rows,
        "prop_is_political_right": (
            posts_df["political_ideology_label"].fillna("").eq("right")
        ).sum()
        / total_rows,
        "prop_is_political_moderate": (
            posts_df["political_ideology_label"].fillna("").eq("moderate")
        ).sum()
        / total_rows,
        "prop_is_political_unclear": (
            posts_df["political_ideology_label"].fillna("").eq("unclear")
        ).sum()
        / total_rows,
    }

    return political_proportions


def calculate_valence_averages(posts_df: pd.DataFrame) -> Dict[str, float]:
    """Calculate valence averages from posts.

    Args:
        posts_df: DataFrame containing posts with valence columns

    Returns:
        Dictionary containing valence averages
    """
    total_rows = len(posts_df)

    valence_averages = {
        "avg_is_positive": posts_df["valence_label"].eq("positive").sum() / total_rows,
        "avg_is_neutral": posts_df["valence_label"].eq("neutral").sum() / total_rows,
        "avg_is_negative": posts_df["valence_label"].eq("negative").sum() / total_rows,
    }

    return valence_averages


def calculate_valence_proportions(posts_df: pd.DataFrame) -> Dict[str, float]:
    """Calculate valence proportions from posts.

    Args:
        posts_df: DataFrame containing posts with valence columns

    Returns:
        Dictionary containing valence proportions
    """
    total_rows = len(posts_df)

    valence_proportions = {
        "prop_is_positive": posts_df["valence_label"].eq("positive").sum() / total_rows,
        "prop_is_neutral": posts_df["valence_label"].eq("neutral").sum() / total_rows,
        "prop_is_negative": posts_df["valence_label"].eq("negative").sum() / total_rows,
    }

    return valence_proportions
