"""
Correlation analysis functions.
"""

import os
import json
from pathlib import Path
from typing import Dict

import pandas as pd

from lib.log.logger import get_logger

logger = get_logger(__name__)


def calculate_correlations(df: pd.DataFrame, col1: str, col2: str) -> Dict[str, float]:
    """
    Calculate correlation coefficients for the given DataFrame.

    Args:
        df: DataFrame containing posts with toxicity and constructiveness scores
        col1: First column name (e.g., "prob_toxic")
        col2: Second column name (e.g., "prob_constructive")

    Returns:
        Dictionary with correlation results and metadata
    """
    logger.info(f"Calculating correlations for {len(df)} posts")

    # Drop rows with missing values in the specified columns
    df_clean = df.dropna(subset=[col1, col2])
    if len(df_clean) == 0:
        logger.warning("No data to calculate correlations")
        return {
            "pearson_correlation": 0.0,
            "spearman_correlation": 0.0,
            "sample_size": 0,
            "toxicity_mean": 0.0,
            "constructiveness_mean": 0.0,
        }

    pearson_correlation = calculate_pearson_correlation(df_clean, col1, col2)
    spearman_correlation = calculate_spearman_correlation(df_clean, col1, col2)

    return {
        "pearson_correlation": pearson_correlation,
        "spearman_correlation": spearman_correlation,
        "sample_size": len(df_clean),
        "toxicity_mean": df_clean[col1].mean(),
        "constructiveness_mean": df_clean[col2].mean(),
    }


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


def write_correlation_results(
    results: dict[str, float],
    output_dir: str,
    filename: str,
):
    """
    Write correlation results to CSV files.

    Args:
        results: List of daily correlation results
        output_dir: Directory to save output files
    """
    logger.info(f"Writing {len(results)} correlation results to {output_dir}")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    if results:
        output_path = os.path.join(output_dir, filename)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Saved daily correlations to {output_path}")
