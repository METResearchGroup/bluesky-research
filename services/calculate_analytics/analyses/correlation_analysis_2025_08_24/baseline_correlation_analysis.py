"""
Baseline Correlation Analysis for Toxicity-Constructiveness Study

This script implements the baseline correlation analysis across all Bluesky posts (20-30M posts)
to investigate the relationship between toxicity and constructiveness scores. The analysis
aims to determine whether observed correlations are real data patterns that replicate across
a wide sample of Bluesky posts, or if they are artifacts of data selection or processing.

Research Question: "Look at the correlation of toxicity x constructiveness on all posts we have,
to see if this is a trend that replicates across a wide sample of Bluesky posts."

The analysis will:
- Process all available posts using Slurm for large-scale data handling
- Calculate both Pearson and Spearman correlation coefficients
- Implement daily batch processing with garbage collection for memory management
- Generate CSV output with correlation results and statistical summaries
- Provide baseline understanding of toxicity-constructiveness relationships

This baseline analysis serves as Phase 1 of the correlation investigation project and will
be compared against feed selection bias analysis and calculation logic review in subsequent phases.
"""

import json
import os

from pathlib import Path
from typing import Dict, List

import pandas as pd

from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_perspective_api_labels,
)

output_dir = os.path.join(os.path.dirname(__file__), "results")
logger = get_logger(__name__)


def calculate_pearson_correlation(df: pd.DataFrame) -> float:
    """
    Calculate Pearson correlation coefficient between toxicity and constructiveness.

    Args:
        df: DataFrame containing posts with toxicity and constructiveness scores

    Returns:
        Pearson correlation coefficient
    """
    toxicity_scores = df["prob_toxic"]
    constructiveness_scores = df["prob_constructive"]
    logger.info("Calculating Pearson correlation")
    corr = toxicity_scores.corr(constructiveness_scores, method="pearson")
    logger.info(f"Pearson correlation: {corr}")
    return corr


def calculate_spearman_correlation(df: pd.DataFrame) -> float:
    """
    Calculate Spearman correlation coefficient between toxicity and constructiveness.

    Args:
        df: DataFrame containing posts with toxicity and constructiveness scores

    Returns:
        Spearman correlation coefficient
    """
    toxicity_scores = df["prob_toxic"]
    constructiveness_scores = df["prob_constructive"]
    logger.info("Calculating Spearman correlation")
    corr = toxicity_scores.corr(constructiveness_scores, method="spearman")
    logger.info(f"Spearman correlation: {corr}")
    return corr


def calculate_correlations(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate correlation coefficients for a single day's data.

    Args:
        df: DataFrame containing posts with toxicity and constructiveness scores

    Returns:
        Dictionary with correlation results and metadata
    """
    logger.info(
        f"Calculating correlations for {df['partition_date'].min()} to {df['partition_date'].max()}"
    )
    df = df.dropna(subset=["prob_toxic", "prob_constructive"])
    if len(df) == 0:
        logger.warning("No data to calculate correlations")
        return {
            "date": df["partition_date"].min(),
            "pearson_correlation": 0.0,
            "spearman_correlation": 0.0,
            "sample_size": 0,
            "toxicity_mean": 0.0,
            "constructiveness_mean": 0.0,
        }

    pearson_correlation = calculate_pearson_correlation(df)
    spearman_correlation = calculate_spearman_correlation(df)

    return {
        "pearson_correlation": pearson_correlation,
        "spearman_correlation": spearman_correlation,
        "sample_size": len(df),
        "toxicity_mean": df["prob_toxic"].mean(),
        "constructiveness_mean": df["prob_constructive"].mean(),
    }


def write_correlation_results(results: List[Dict[str, float]], output_dir: str):
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
        output_path = os.path.join(
            output_dir, f"baseline_correlations_{generate_current_datetime_str()}.json"
        )
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Saved daily correlations to {output_path}")


def main():
    """
    Main execution function for baseline correlation analysis.
    """
    logger.info("Starting baseline correlation analysis")

    table_columns = ["uri", "partition_date", "prob_toxic", "prob_constructive"]
    table_columns_str = ", ".join(table_columns)
    query = (
        f"SELECT {table_columns_str} "
        f"FROM ml_inference_perspective_api "
        f"WHERE prob_toxic IS NOT NULL "
        f"AND prob_constructive IS NOT NULL "
    ).strip()

    df: pd.DataFrame = get_perspective_api_labels(
        lookback_start_date=STUDY_START_DATE,
        lookback_end_date=STUDY_END_DATE,
        duckdb_query=query,
        query_metadata={
            "tables": [
                {"name": "ml_inference_perspective_api", "columns": table_columns}
            ]
        },
        export_format="duckdb",
    )

    # Generate summary and write results
    results = calculate_correlations(df)
    write_correlation_results(results, output_dir)
    logger.info("Baseline correlation analysis complete")


if __name__ == "__main__":
    main()
