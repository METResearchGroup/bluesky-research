"""
Script to summarize baseline measures across all days in the study.

This script takes the daily baseline content label metrics CSV file and calculates
the overall average across all days, providing a single summary of baseline measures
for the entire study period.
"""

import os
import glob
from typing import Optional

import pandas as pd
import numpy as np

from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(__file__)


def find_latest_daily_baseline_file() -> Optional[str]:
    """Find the most recent daily baseline content label metrics CSV file.

    Returns:
        Path to the latest daily baseline CSV file, or None if none found
    """
    results_dir = os.path.join(current_dir, "results")
    pattern = os.path.join(results_dir, "daily_baseline_content_label_metrics_*.csv")

    csv_files = glob.glob(pattern)

    if not csv_files:
        logger.warning("No daily baseline content label metrics CSV files found")
        return None

    # Sort by modification time and return the most recent
    latest_file = max(csv_files, key=os.path.getmtime)
    logger.info(f"Found latest daily baseline file: {latest_file}")

    return latest_file


def load_daily_baseline_data(csv_file_path: str) -> pd.DataFrame:
    """Load the daily baseline content label metrics CSV file.

    Args:
        csv_file_path: Path to the daily baseline CSV file

    Returns:
        DataFrame containing the daily baseline metrics
    """
    try:
        df = pd.read_csv(csv_file_path)
        logger.info(
            f"Loaded daily baseline data with {len(df)} rows and {len(df.columns)} columns"
        )

        # Log column information for debugging
        logger.info(f"Columns in CSV: {list(df.columns)}")
        baseline_columns = [col for col in df.columns if col.startswith("baseline_")]
        logger.info(f"Baseline metric columns found: {baseline_columns}")

        return df
    except Exception as e:
        logger.error(f"Failed to load daily baseline data from {csv_file_path}: {e}")
        raise


def calculate_total_average_metrics(df: pd.DataFrame) -> dict[str, float]:
    """Calculate the total average across all days for each baseline metric.

    Args:
        df: DataFrame containing daily baseline metrics

    Returns:
        Dictionary mapping metric names to their total averages
    """
    # Identify baseline metric columns by looking for columns that start with 'baseline_'
    # This is more robust than excluding hardcoded metadata columns
    baseline_metric_columns = [col for col in df.columns if col.startswith("baseline_")]

    logger.info(
        f"Found {len(baseline_metric_columns)} baseline metric columns to average"
    )

    total_averages = {}

    for metric_col in baseline_metric_columns:
        try:
            # Convert to numeric, coercing errors to NaN
            numeric_values = pd.to_numeric(df[metric_col], errors="coerce")

            # Calculate mean, ignoring NaN values
            metric_values = numeric_values.dropna()

            if len(metric_values) == 0:
                logger.warning(f"No valid numeric values found for metric {metric_col}")
                total_averages[metric_col] = np.nan
            else:
                # Round to 3 decimal places for consistency with other analyses
                total_averages[metric_col] = round(metric_values.mean(), 3)
                logger.debug(
                    f"{metric_col}: {len(metric_values)} days, average = {total_averages[metric_col]}"
                )

        except Exception as e:
            logger.error(f"Failed to process metric {metric_col}: {e}")
            total_averages[metric_col] = np.nan

    logger.info(f"Calculated total averages for {len(total_averages)} metrics")
    return total_averages


def create_summary_dataframe(total_averages: dict[str, float]) -> pd.DataFrame:
    """Create a summary DataFrame with the total average metrics.

    Args:
        total_averages: Dictionary mapping metric names to their total averages

    Returns:
        DataFrame with a single row containing the total average metrics
    """
    # Create a single row with all the total averages
    summary_data = {
        "analysis_type": "total_average_baseline",
        "study_period": "full_study",
        "total_days_analyzed": len(
            [v for v in total_averages.values() if not pd.isna(v)]
        ),
        **total_averages,
    }

    summary_df = pd.DataFrame([summary_data])

    logger.info(f"Created summary DataFrame with {len(summary_df.columns)} columns")
    return summary_df


def export_summary_results(summary_df: pd.DataFrame) -> str:
    """Export the summary results to a CSV file.

    Args:
        summary_df: DataFrame containing the summary results

    Returns:
        Path to the exported CSV file
    """
    current_datetime_str = generate_current_datetime_str()
    output_filename = (
        f"total_average_baseline_content_label_metrics_{current_datetime_str}.csv"
    )
    output_path = os.path.join(current_dir, "results", output_filename)

    # Ensure results directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        summary_df.to_csv(output_path, index=False)
        logger.info(f"Exported total average baseline metrics to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to export summary results to {output_path}: {e}")
        raise


def main():
    """Execute the summarization of baseline measures across all study days."""

    try:
        # Find the latest daily baseline file
        daily_file = find_latest_daily_baseline_file()
        if daily_file is None:
            logger.error(
                "No daily baseline file found. Please run the main analysis first."
            )
            return

        # Load the daily baseline data
        daily_df = load_daily_baseline_data(daily_file)

        # Calculate total averages across all days
        total_averages = calculate_total_average_metrics(daily_df)

        # Create summary DataFrame
        summary_df = create_summary_dataframe(total_averages)

        # Export results
        output_path = export_summary_results(summary_df)

        logger.info(
            f"Successfully completed summarization. Results saved to: {output_path}"
        )

        # Print summary statistics
        num_metrics = len([v for v in total_averages.values() if not pd.isna(v)])
        num_days = len(daily_df)
        logger.info(f"Summary: Averaged {num_metrics} metrics across {num_days} days")

    except Exception as e:
        logger.error(f"Failed to summarize baseline measures: {e}")
        raise


if __name__ == "__main__":
    main()
