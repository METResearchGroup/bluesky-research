"""
Histogram Analysis for Feed and Engagement Data

This script generates histograms for various metrics from both feed and engagement data.
It creates separate histograms for:
- Feed data: daily/weekly, average/proportion metrics
- Engagement data: daily/weekly, average/proportion metrics

Each analysis generates 5 histogram files:
- constructive.png
- intergroup.png
- moral.png
- moral_outrage.png
- sociopolitical.png

Directory structure:
histogram_analyses_2025_09_01/results/<timestamp>/
├── feed_daily_average/
├── feed_daily_proportion/
├── feed_weekly_average/
├── feed_weekly_proportion/
├── engagement_daily_average/
├── engagement_daily_proportion/
├── engagement_weekly_average/
└── engagement_weekly_proportion/
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Optional
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str

logger = get_logger(__name__)

# Define the metrics to analyze (excluding toxic as it's not in the list)
METRICS = ["constructive", "intergroup", "moral", "moral_outrage", "sociopolitical"]

# File paths
FEED_DAILY_FILE = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv"
FEED_WEEKLY_FILE = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/weekly_feed_content_aggregated_results_per_user.csv"
ENGAGEMENT_DAILY_FILE = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_engagement_analysis_2025_06_16/results/daily_content_label_proportions_per_user.csv"
ENGAGEMENT_WEEKLY_FILE = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_engagement_analysis_2025_06_16/results/weekly_content_label_proportions_per_user.csv"

BASE_OUTPUT_DIR = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/histogram_analyses_2025_09_01/results"


def create_timestamped_output_dir(timestamp: str) -> str:
    """
    Create a timestamped output directory for this analysis run.

    Args:
        timestamp: Timestamp string for the directory name

    Returns:
        Path to the created output directory
    """
    # Clean timestamp for directory name (replace colons with dashes)
    clean_timestamp = timestamp.replace(":", "-")
    output_dir = os.path.join(BASE_OUTPUT_DIR, clean_timestamp)

    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")

    return output_dir


def load_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load CSV data and validate it exists.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame if successful, None otherwise
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {os.path.basename(file_path)}")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


def get_metric_columns(
    df: pd.DataFrame, metric_type: str, metric_name: str
) -> List[str]:
    """
    Get the column names for a specific metric type and name.

    Args:
        df: DataFrame to search in
        metric_type: Either 'average' or 'proportion'
        metric_name: The metric name (e.g., 'constructive', 'intergroup')

    Returns:
        List of matching column names
    """
    if metric_type == "average":
        prefix = (
            "feed_average_"
            if "feed" in df.columns[0] or "feed" in str(df.columns)
            else "engagement_average_"
        )
    else:  # proportion
        prefix = (
            "feed_proportion_"
            if "feed" in df.columns[0] or "feed" in str(df.columns)
            else "engagement_proportion_"
        )

    # Look for columns that match the exact pattern
    # Handle special cases for precise matching
    if metric_name == "moral":
        # Only match "moral" but not "moral_outrage"
        matching_columns = [
            col
            for col in df.columns
            if col.startswith(prefix)
            and col.endswith("_moral")
            and not col.endswith("_moral_outrage")
        ]
    elif metric_name == "moral_outrage":
        # Only match "moral_outrage"
        matching_columns = [
            col
            for col in df.columns
            if col.startswith(prefix) and col.endswith("_moral_outrage")
        ]
    elif metric_name == "sociopolitical":
        # Only match "is_sociopolitical"
        matching_columns = [
            col
            for col in df.columns
            if col.startswith(prefix) and col.endswith("_is_sociopolitical")
        ]
    else:
        # For other metrics, use exact matching
        matching_columns = [
            col
            for col in df.columns
            if col.startswith(prefix) and col.endswith(f"_{metric_name}")
        ]

    return matching_columns


def create_histogram(
    df: pd.DataFrame,
    metric_type: str,
    metric_name: str,
    output_path: str,
    title_suffix: str = "",
):
    """
    Create a histogram for a specific metric.

    Args:
        df: DataFrame containing the data
        metric_type: Either 'average' or 'proportion'
        metric_name: The metric name (e.g., 'constructive', 'intergroup')
        output_path: Path to save the histogram
        title_suffix: Additional text for the title
    """
    # Get the column names for this metric
    columns = get_metric_columns(df, metric_type, metric_name)

    if not columns:
        logger.warning(f"No columns found for {metric_type} {metric_name}")
        return

    # Set up the plot
    plt.figure(figsize=(12, 8))

    # Create subplots for each column
    n_cols = len(columns)
    if n_cols == 1:
        fig, ax = plt.subplots(figsize=(10, 6))
        axes = [ax]
    else:
        fig, axes = plt.subplots(1, n_cols, figsize=(5 * n_cols, 6))
        if n_cols == 1:
            axes = [axes]

    for i, col in enumerate(columns):
        ax = axes[i] if n_cols > 1 else axes[0]

        # Get data, removing NaN values
        data = df[col].dropna()

        if len(data) == 0:
            ax.text(
                0.5, 0.5, "No Data", ha="center", va="center", transform=ax.transAxes
            )
            ax.set_title(f"{col}\n(No Data)")
            continue

        # Create histogram
        ax.hist(data, bins=30, alpha=0.7, edgecolor="black", linewidth=0.5)

        # Add statistics
        mean_val = data.mean()
        median_val = data.median()
        std_val = data.std()

        ax.axvline(
            mean_val,
            color="red",
            linestyle="--",
            linewidth=2,
            label=f"Mean: {mean_val:.3f}",
        )
        ax.axvline(
            median_val,
            color="green",
            linestyle="--",
            linewidth=2,
            label=f"Median: {median_val:.3f}",
        )

        ax.set_title(f"{col}\n(n={len(data)}, σ={std_val:.3f})")
        ax.set_xlabel("Value")
        ax.set_ylabel("Frequency")
        ax.legend()
        ax.grid(True, alpha=0.3)

    # Overall title
    title = f"{metric_name.title()} {metric_type.title()} Distribution{title_suffix}"
    fig.suptitle(title, fontsize=16, fontweight="bold")

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Histogram saved to {output_path}")


def analyze_dataset(
    df: pd.DataFrame, dataset_name: str, metric_type: str, output_dir: str
):
    """
    Analyze a dataset and create histograms for all metrics.

    Args:
        df: DataFrame containing the data
        dataset_name: Name of the dataset (e.g., 'feed_daily', 'engagement_weekly')
        metric_type: Either 'average' or 'proportion'
        output_dir: Base output directory
    """
    logger.info(f"Analyzing {dataset_name} {metric_type} data...")

    # Create subdirectory for this analysis
    subdir = os.path.join(output_dir, f"{dataset_name}_{metric_type}")
    os.makedirs(subdir, exist_ok=True)

    # Create histograms for each metric
    for metric in METRICS:
        output_path = os.path.join(subdir, f"{metric}.png")
        title_suffix = f" - {dataset_name.replace('_', ' ').title()}"
        create_histogram(df, metric_type, metric, output_path, title_suffix)


def main():
    """
    Main execution function for histogram analysis.
    """
    logger.info("Starting histogram analysis")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Load all datasets
    datasets = {
        "feed_daily": load_data(FEED_DAILY_FILE),
        "feed_weekly": load_data(FEED_WEEKLY_FILE),
        "engagement_daily": load_data(ENGAGEMENT_DAILY_FILE),
        "engagement_weekly": load_data(ENGAGEMENT_WEEKLY_FILE),
    }

    # Analyze each dataset
    for dataset_name, df in datasets.items():
        if df is not None:
            # Analyze average metrics
            analyze_dataset(df, dataset_name, "average", output_dir)

            # Analyze proportion metrics
            analyze_dataset(df, dataset_name, "proportion", output_dir)
        else:
            logger.error(f"Skipping {dataset_name} due to loading error")

    # Print summary
    logger.info("=== HISTOGRAM ANALYSIS SUMMARY ===")
    logger.info(f"Output directory: {output_dir}")

    # Count generated files
    total_files = 0
    for root, dirs, files in os.walk(output_dir):
        png_files = [f for f in files if f.endswith(".png")]
        if png_files:
            logger.info(
                f"Directory {os.path.basename(root)}: {len(png_files)} PNG files"
            )
            total_files += len(png_files)

    logger.info(f"Total histogram files generated: {total_files}")
    logger.info(f"All analysis assets saved in: {output_dir}")
    logger.info("Histogram analysis complete")


if __name__ == "__main__":
    main()
