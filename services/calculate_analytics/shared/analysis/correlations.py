"""
Correlation analysis functions.
"""

import pandas as pd

from lib.log.logger import get_logger

logger = get_logger(__name__)


def calculate_pearson_correlation(df: pd.DataFrame, col1: str, col2: str) -> float:
    """
    Calculate Pearson correlation coefficient between two columns.

    Args:
        df: DataFrame containing posts with two columns
        col1: First column name
        col2: Second column name

    Returns:
        Pearson correlation coefficient
    """
    col1_scores = df[col1]
    col2_scores = df[col2]
    logger.info("Calculating Pearson correlation")
    corr = col1_scores.corr(col2_scores, method="pearson")
    logger.info(f"Pearson correlation: {corr}")
    return corr


def calculate_spearman_correlation(df: pd.DataFrame, col1: str, col2: str) -> float:
    """
    Calculate Spearman correlation coefficient between two columns.

    Args:
        df: DataFrame containing posts with two columns
        col1: First column name
        col2: Second column name

    Returns:
        Spearman correlation coefficient
    """
    col1_scores = df[col1]
    col2_scores = df[col2]
    logger.info("Calculating Spearman correlation")
    corr = col1_scores.corr(col2_scores, method="spearman")
    logger.info(f"Spearman correlation: {corr}")
    return corr
