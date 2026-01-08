"""
Engagement Data Correlation Analysis for User Engagement Analysis Results

This script analyzes correlations between various engagement metrics from the user engagement analysis results.
It processes the latest daily and weekly CSV files from the results directory and calculates
correlations between different engagement metrics across study conditions.

The analysis includes:
- Correlation analysis for average columns (engagement_average_toxic, engagement_average_constructive, etc.)
- Correlation analysis for proportion columns (engagement_proportion_toxic, engagement_proportion_constructive, etc.)
- Analysis across all study conditions
- Automatic detection of the latest CSV files

Metrics analyzed:
- engagement_average_toxic
- engagement_average_constructive
- engagement_average_intergroup
- engagement_average_moral
- engagement_average_moral_outrage
- engagement_average_is_sociopolitical
"""

import os
import glob
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import json
import matplotlib.pyplot as plt
import seaborn as sns

from lib.log.logger import get_logger
from lib.datetime_utils import generate_current_datetime_str

logger = get_logger(__name__)

# Define the metrics to analyze
# Note: Using posted_posts metrics as the primary engagement measure
AVERAGE_METRICS = [
    "engagement_average_posted_posts_toxic",
    "engagement_average_posted_posts_constructive",
    "engagement_average_posted_posts_intergroup",
    "engagement_average_posted_posts_moral",
    "engagement_average_posted_posts_moral_outrage",
    "engagement_average_posted_posts_is_sociopolitical",
]

PROPORTION_METRICS = [
    "engagement_proportion_posted_posts_toxic",
    "engagement_proportion_posted_posts_constructive",
    "engagement_proportion_posted_posts_intergroup",
    "engagement_proportion_posted_posts_moral",
    "engagement_proportion_posted_posts_moral_outrage",
    "engagement_proportion_posted_posts_is_sociopolitical",
]

# Results directory path
RESULTS_DIR = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_engagement_analysis_2025_06_16/results"
BASE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results", "engagement")


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
    output_dir = os.path.join(
        BASE_OUTPUT_DIR, f"engagement_correlation_analysis_{clean_timestamp}"
    )

    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")

    return output_dir


def find_latest_csv_files() -> Tuple[Optional[str], Optional[str]]:
    """
    Find the specific daily and weekly CSV files from the results directory.

    Returns:
        Tuple of (daily_file_path, weekly_file_path) or (None, None) if not found
    """
    if not os.path.exists(RESULTS_DIR):
        logger.error(f"Results directory not found: {RESULTS_DIR}")
        return None, None

    # Hard-coded filenames for engagement data
    daily_filename = "daily_content_label_proportions_per_user.csv"
    weekly_filename = "weekly_content_label_proportions_per_user.csv"

    daily_file = os.path.join(RESULTS_DIR, daily_filename)
    weekly_file = os.path.join(RESULTS_DIR, weekly_filename)

    # Check if files exist
    daily_exists = os.path.exists(daily_file)
    weekly_exists = os.path.exists(weekly_file)

    if not daily_exists and not weekly_exists:
        logger.error(f"No CSV files found in {RESULTS_DIR}")
        logger.error(f"Looking for: {daily_filename} and {weekly_filename}")
        return None, None

    if daily_exists:
        logger.info(f"Found daily file: {daily_filename}")
    else:
        logger.warning(f"Daily file not found: {daily_filename}")
        daily_file = None

    if weekly_exists:
        logger.info(f"Found weekly file: {weekly_filename}")
    else:
        logger.warning(f"Weekly file not found: {weekly_filename}")
        weekly_file = None

    return daily_file, weekly_file


def load_and_validate_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load CSV data and validate required columns exist.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame if successful, None otherwise
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {os.path.basename(file_path)}")

        # Check for required columns
        required_columns = (
            ["user_id", "condition"] + AVERAGE_METRICS + PROPORTION_METRICS
        )
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logger.warning(
                f"Missing columns in {os.path.basename(file_path)}: {missing_columns}"
            )
            # Check which metrics are actually available
            available_avg = [col for col in AVERAGE_METRICS if col in df.columns]
            available_prop = [col for col in PROPORTION_METRICS if col in df.columns]
            logger.info(f"Available average metrics: {available_avg}")
            logger.info(f"Available proportion metrics: {available_prop}")

        return df

    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


def calculate_correlation_matrix(
    df: pd.DataFrame, metrics: List[str], condition: Optional[str] = None
) -> Dict:
    """
    Calculate correlation matrix for specified metrics.

    Args:
        df: DataFrame containing the data
        metrics: List of metric column names
        condition: Optional condition to filter by

    Returns:
        Dictionary containing correlation results
    """
    # Filter by condition if specified
    if condition:
        df_filtered = df[df["condition"] == condition].copy()
        logger.info(f"Filtering for condition '{condition}': {len(df_filtered)} users")
    else:
        df_filtered = df.copy()
        logger.info(f"Using all data: {len(df_filtered)} users")

    # Select only the metrics that exist in the dataframe
    available_metrics = [col for col in metrics if col in df_filtered.columns]

    if len(available_metrics) < 2:
        logger.warning(
            f"Not enough metrics available for correlation: {available_metrics}"
        )
        return {"error": "Not enough metrics available"}

    # Remove rows with any NaN values in the metrics
    df_clean = df_filtered[available_metrics].dropna()
    logger.info(f"After removing NaN values: {len(df_clean)} users")

    if len(df_clean) < 2:
        logger.warning("Not enough data points after cleaning")
        return {"error": "Not enough data points after cleaning"}

    # Calculate correlation matrix
    corr_matrix = df_clean.corr()

    # Calculate pairwise correlations
    correlations = []
    for i, metric1 in enumerate(available_metrics):
        for j, metric2 in enumerate(available_metrics):
            if i < j:  # Only upper triangle to avoid duplicates
                corr_value = corr_matrix.loc[metric1, metric2]
                correlations.append(
                    {
                        "metric1": metric1,
                        "metric2": metric2,
                        "correlation": float(corr_value),
                        "abs_correlation": float(abs(corr_value)),
                    }
                )

    # Sort by absolute correlation value
    correlations.sort(key=lambda x: x["abs_correlation"], reverse=True)

    return {
        "condition": condition,
        "n_users": len(df_clean),
        "metrics": available_metrics,
        "correlation_matrix": corr_matrix.to_dict(),
        "pairwise_correlations": correlations,
        "summary_stats": {
            "mean_correlation": float(
                corr_matrix.values[np.triu_indices_from(corr_matrix.values, k=1)].mean()
            ),
            "max_correlation": float(
                corr_matrix.values[np.triu_indices_from(corr_matrix.values, k=1)].max()
            ),
            "min_correlation": float(
                corr_matrix.values[np.triu_indices_from(corr_matrix.values, k=1)].min()
            ),
        },
    }


def analyze_file(
    file_path: str, file_type: str, timestamp: str, output_dir: str
) -> Dict:
    """
    Perform complete correlation analysis on a single file.

    Args:
        file_path: Path to the CSV file
        file_type: Type of file ("daily" or "weekly")
        timestamp: Timestamp for output files

    Returns:
        Dictionary containing all analysis results
    """
    logger.info(f"Analyzing {file_type} file: {os.path.basename(file_path)}")

    # Load data
    df = load_and_validate_data(file_path)
    if df is None:
        return {"error": f"Failed to load {file_type} file"}

    results = {
        "file_type": file_type,
        "file_name": os.path.basename(file_path),
        "analysis_timestamp": timestamp,
        "total_users": len(df),
        "conditions": df["condition"].unique().tolist()
        if "condition" in df.columns
        else [],
    }

    # Analyze average metrics
    logger.info("Analyzing average metrics...")
    avg_results = {}

    # All conditions combined
    avg_results["all_conditions"] = calculate_correlation_matrix(df, AVERAGE_METRICS)

    # Per condition
    if "condition" in df.columns:
        for condition in df["condition"].unique():
            avg_results[f"condition_{condition}"] = calculate_correlation_matrix(
                df, AVERAGE_METRICS, condition
            )

    results["average_metrics"] = avg_results

    # Analyze proportion metrics
    logger.info("Analyzing proportion metrics...")
    prop_results = {}

    # All conditions combined
    prop_results["all_conditions"] = calculate_correlation_matrix(
        df, PROPORTION_METRICS
    )

    # Per condition
    if "condition" in df.columns:
        for condition in df["condition"].unique():
            prop_results[f"condition_{condition}"] = calculate_correlation_matrix(
                df, PROPORTION_METRICS, condition
            )

    results["proportion_metrics"] = prop_results

    # Generate heatmaps
    logger.info("Generating correlation heatmaps...")
    generate_heatmaps(df, file_type, timestamp, output_dir, avg_results, prop_results)

    return results


def generate_heatmaps(
    df: pd.DataFrame,
    file_type: str,
    timestamp: str,
    output_dir: str,
    avg_results: Dict,
    prop_results: Dict,
):
    """
    Generate correlation heatmaps for all analysis results.

    Args:
        df: Original DataFrame
        file_type: Type of file ("daily" or "weekly")
        timestamp: Timestamp for output files
        avg_results: Average metrics correlation results
        prop_results: Proportion metrics correlation results
    """

    # Helper function to create heatmap if correlation matrix exists
    def create_heatmap_if_valid(
        result_dict: Dict, metric_type: str, condition_name: str
    ):
        if "error" not in result_dict and "correlation_matrix" in result_dict:
            # Convert correlation matrix dict back to DataFrame
            corr_dict = result_dict["correlation_matrix"]
            corr_df = pd.DataFrame(corr_dict)

            # Create title
            title = f"{file_type.title()} Engagement {metric_type.title()} Correlations"
            if condition_name != "all_conditions":
                condition_display = (
                    condition_name.replace("condition_", "").replace("_", " ").title()
                )
                title += f" - {condition_display}"

            # Create output path
            safe_condition = condition_name.replace("condition_", "").replace("_", "_")
            output_path = os.path.join(
                output_dir,
                f"{file_type}_{metric_type}_{safe_condition}_correlations_{timestamp}.png",
            )

            create_correlation_heatmap(corr_df, title, output_path)

    # Generate heatmaps for average metrics
    for condition_key, result in avg_results.items():
        create_heatmap_if_valid(result, "average", condition_key)

    # Generate heatmaps for proportion metrics
    for condition_key, result in prop_results.items():
        create_heatmap_if_valid(result, "proportion", condition_key)


def create_correlation_heatmap(
    corr_matrix: pd.DataFrame,
    title: str,
    output_path: str,
    figsize: Tuple[int, int] = (10, 8),
):
    """
    Create and save a correlation matrix heatmap.

    Args:
        corr_matrix: Correlation matrix DataFrame
        title: Title for the plot
        output_path: Path to save the PNG file
        figsize: Figure size tuple (width, height)
    """
    # Set up the plot style
    plt.style.use("default")
    sns.set_palette("RdBu_r")

    # Create the figure
    fig, ax = plt.subplots(figsize=figsize)

    # Create the heatmap
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))  # Mask upper triangle
    sns.heatmap(
        corr_matrix,
        mask=mask,
        annot=True,
        cmap="RdBu_r",
        center=0,
        square=True,
        fmt=".2f",
        cbar_kws={"shrink": 0.8, "label": "Correlation"},
        ax=ax,
    )

    # Customize the plot
    ax.set_title(title, fontsize=14, fontweight="bold", pad=20)
    ax.set_xlabel("", fontsize=12)
    ax.set_ylabel("", fontsize=12)

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Correlation heatmap saved to {output_path}")


def save_results(results: Dict, output_file: str):
    """
    Save analysis results to JSON file.

    Args:
        results: Analysis results dictionary
        output_file: Output file path
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"Results saved to {output_file}")


def main():
    """
    Main execution function for engagement data correlation analysis.
    """
    logger.info("Starting engagement data correlation analysis")

    # Find latest CSV files
    daily_file, weekly_file = find_latest_csv_files()

    if not daily_file and not weekly_file:
        logger.error("No CSV files found to analyze")
        return

    timestamp = generate_current_datetime_str()

    # Create timestamped output directory
    output_dir = create_timestamped_output_dir(timestamp)

    all_results = {
        "analysis_timestamp": timestamp,
        "output_directory": output_dir,
        "files_analyzed": [],
    }

    # Analyze daily file if available
    if daily_file:
        daily_results = analyze_file(daily_file, "daily", timestamp, output_dir)
        all_results["files_analyzed"].append(daily_results)

        # Save individual daily results
        daily_output = os.path.join(
            output_dir, f"daily_engagement_correlations_{timestamp}.json"
        )
        save_results(daily_results, daily_output)

    # Analyze weekly file if available
    if weekly_file:
        weekly_results = analyze_file(weekly_file, "weekly", timestamp, output_dir)
        all_results["files_analyzed"].append(weekly_results)

        # Save individual weekly results
        weekly_output = os.path.join(
            output_dir, f"weekly_engagement_correlations_{timestamp}.json"
        )
        save_results(weekly_results, weekly_output)

    # Save combined results
    combined_output = os.path.join(
        output_dir, f"engagement_correlation_analysis_{timestamp}.json"
    )
    save_results(all_results, combined_output)

    # Print summary
    logger.info("=== ENGAGEMENT CORRELATION ANALYSIS SUMMARY ===")
    for file_result in all_results["files_analyzed"]:
        if "error" in file_result:
            logger.error(f"{file_result['file_type']}: {file_result['error']}")
            continue

        logger.info(
            f"\n{file_result['file_type'].upper()} FILE: {file_result['file_name']}"
        )
        logger.info(f"Total users: {file_result['total_users']}")
        logger.info(f"Conditions: {file_result['conditions']}")

        # Average metrics summary
        if "average_metrics" in file_result:
            avg_all = file_result["average_metrics"].get("all_conditions", {})
            if "summary_stats" in avg_all:
                stats = avg_all["summary_stats"]
                logger.info(
                    f"Average metrics - Mean correlation: {stats['mean_correlation']:.3f}, "
                    f"Max: {stats['max_correlation']:.3f}, Min: {stats['min_correlation']:.3f}"
                )

        # Proportion metrics summary
        if "proportion_metrics" in file_result:
            prop_all = file_result["proportion_metrics"].get("all_conditions", {})
            if "summary_stats" in prop_all:
                stats = prop_all["summary_stats"]
                logger.info(
                    f"Proportion metrics - Mean correlation: {stats['mean_correlation']:.3f}, "
                    f"Max: {stats['max_correlation']:.3f}, Min: {stats['min_correlation']:.3f}"
                )

    # Print summary of generated files
    logger.info("\n=== GENERATED FILES ===")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Combined JSON results: {combined_output}")
    if daily_file:
        logger.info(
            f"Daily JSON: {os.path.join(output_dir, f'daily_engagement_correlations_{timestamp}.json')}"
        )
    if weekly_file:
        logger.info(
            f"Weekly JSON: {os.path.join(output_dir, f'weekly_engagement_correlations_{timestamp}.json')}"
        )

    # Count PNG files generated
    png_files = glob.glob(os.path.join(output_dir, f"*_correlations_{timestamp}.png"))
    logger.info(f"Correlation heatmaps: {len(png_files)} PNG files generated")
    for png_file in png_files:
        logger.info(f"  - {os.path.basename(png_file)}")

    logger.info(f"\nAll analysis assets saved in: {output_dir}")

    logger.info("Engagement data correlation analysis complete")


if __name__ == "__main__":
    main()
