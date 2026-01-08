"""
Visualization for Topic Modeling Analysis

This script generates visualizations for topic modeling results, including:
- Topic distribution plots by condition
- Topic evolution over time
- Topic quality metrics visualization
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import json
from typing import Optional
from lib.log.logger import get_logger
from lib.datetime_utils import generate_current_datetime_str

logger = get_logger(__name__)

# Base output directory
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(current_dir, "results")

# Expected file patterns
TOPICS_FILE_PATTERN = "topic_modeling_results_*.csv"
STRATIFIED_FILE_PATTERN = "topic_distributions_combined_*.csv"
QUALITY_METRICS_FILE_PATTERN = "topic_quality_metrics_*.json"


def create_timestamped_output_dir(timestamp: str) -> str:
    """
    Create a timestamped output directory for this visualization run.

    Args:
        timestamp: Timestamp string for the directory name

    Returns:
        Path to the created output directory
    """
    # Clean timestamp for directory name (replace colons with dashes)
    clean_timestamp = timestamp.replace(":", "-")
    output_dir = os.path.join(BASE_OUTPUT_DIR, clean_timestamp)

    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created visualization output directory: {output_dir}")

    return output_dir


def find_latest_file(pattern: str, base_dir: str) -> Optional[str]:
    """
    Find the most recent file matching the pattern in the base directory.

    Args:
        pattern: File pattern to match (e.g., "topic_*.csv")
        base_dir: Base directory to search in

    Returns:
        Path to the most recent matching file, or None if not found
    """
    import glob

    # Search for files matching the pattern
    search_pattern = os.path.join(base_dir, pattern)
    matching_files = glob.glob(search_pattern)

    if not matching_files:
        logger.error(f"No files found matching pattern: {search_pattern}")
        return None

    # Sort by modification time and return the most recent
    latest_file = max(matching_files, key=os.path.getmtime)
    logger.info(f"Found latest file: {os.path.basename(latest_file)}")
    return latest_file


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


def load_json_data(file_path: str) -> Optional[dict]:
    """
    Load JSON data and validate it exists.

    Args:
        file_path: Path to the JSON file

    Returns:
        Dictionary if successful, None otherwise
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        logger.info(f"Loaded JSON data from {os.path.basename(file_path)}")
        return data
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


def create_topic_distribution_plot(
    topics_df: pd.DataFrame, output_path: str, title: str = "Topic Distribution by Size"
):
    """
    Create a bar plot showing topic distribution by size.

    Args:
        topics_df: DataFrame with topic information
        output_path: Path to save the plot
        title: Title for the plot
    """
    plt.figure(figsize=(14, 8))

    # Sort topics by size (Count column)
    topics_sorted = topics_df.sort_values("Count", ascending=True)

    # Create horizontal bar plot
    plt.barh(range(len(topics_sorted)), topics_sorted["Count"])
    plt.yticks(
        range(len(topics_sorted)), [f"Topic {t}" for t in topics_sorted["Topic"]]
    )
    plt.xlabel("Number of Documents")
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Topic distribution plot saved to {output_path}")


def create_topic_evolution_plot(
    stratified_df: pd.DataFrame,
    output_path: str,
    title: str = "Topic Evolution Over Time",
):
    """
    Create a plot showing how topics evolve over time.

    Args:
        stratified_df: DataFrame with stratified topic analysis
        output_path: Path to save the plot
        title: Title for the plot
    """
    plt.figure(figsize=(16, 10))

    # Filter for weekly data
    weekly_data = stratified_df[stratified_df["slice_type"] == "weekly"].copy()

    if weekly_data.empty:
        logger.warning("No weekly data found for topic evolution plot")
        return

    # Convert week to datetime for plotting
    weekly_data["week_date"] = pd.to_datetime(weekly_data["slice_value"].astype(str))

    # Get top 10 topics by total count
    top_topics = weekly_data.groupby("topic_id")["count"].sum().nlargest(10).index

    # Plot evolution for top topics
    for topic_id in top_topics:
        topic_data = weekly_data[weekly_data["topic_id"] == topic_id].sort_values(
            "week_date"
        )
        if not topic_data.empty:
            plt.plot(
                topic_data["week_date"],
                topic_data["proportion"],
                marker="o",
                label=f"Topic {topic_id}",
                linewidth=2,
            )

    plt.xlabel("Week")
    plt.ylabel("Topic Proportion")
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Topic evolution plot saved to {output_path}")


def create_condition_comparison_plot(
    stratified_df: pd.DataFrame,
    output_path: str,
    title: str = "Topic Distribution by Condition",
):
    """
    Create a plot comparing topic distributions across conditions.

    Args:
        stratified_df: DataFrame with stratified topic analysis
        output_path: Path to save the plot
        title: Title for the plot
    """
    plt.figure(figsize=(14, 8))

    # Filter for condition data
    condition_data = stratified_df[stratified_df["slice_type"] == "condition"].copy()

    if condition_data.empty:
        logger.warning("No condition data found for condition comparison plot")
        return

    # Get top 10 topics by total count
    top_topics = condition_data.groupby("topic_id")["count"].sum().nlargest(10).index

    # Create grouped bar plot
    conditions = condition_data["slice_value"].unique()
    x = range(len(top_topics))
    width = 0.25

    for i, condition in enumerate(conditions):
        condition_topic_data = condition_data[
            (condition_data["slice_value"] == condition)
            & (condition_data["topic_id"].isin(top_topics))
        ].set_index("topic_id")["proportion"]

        # Align data with top_topics order
        proportions = [condition_topic_data.get(topic, 0) for topic in top_topics]

        plt.bar(
            [pos + width * i for pos in x],
            proportions,
            width,
            label=condition,
            alpha=0.8,
        )

    plt.xlabel("Topic ID")
    plt.ylabel("Topic Proportion")
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xticks([pos + width for pos in x], [f"Topic {t}" for t in top_topics])
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Condition comparison plot saved to {output_path}")


def create_quality_metrics_plot(
    quality_metrics: dict, output_path: str, title: str = "Topic Model Quality Metrics"
):
    """
    Create a plot showing topic model quality metrics.

    Args:
        quality_metrics: Dictionary with quality metrics
        output_path: Path to save the plot
        title: Title for the plot
    """
    plt.figure(figsize=(10, 6))

    # Extract relevant metrics
    metrics = {}
    if "c_v_mean" in quality_metrics:
        metrics["C_V Coherence"] = quality_metrics["c_v_mean"]
    if "c_npmi_mean" in quality_metrics:
        metrics["C_NPMI Coherence"] = quality_metrics["c_npmi_mean"]
    if "training_time" in quality_metrics:
        metrics["Training Time (s)"] = quality_metrics["training_time"]

    if not metrics:
        logger.warning("No quality metrics found for quality metrics plot")
        return

    # Create bar plot
    metric_names = list(metrics.keys())
    metric_values = list(metrics.values())

    bars = plt.bar(metric_names, metric_values, alpha=0.7)

    # Add value labels on bars
    for bar, value in zip(bars, metric_values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.01,
            f"{value:.3f}",
            ha="center",
            va="bottom",
        )

    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.ylabel("Metric Value")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Quality metrics plot saved to {output_path}")


def create_visualizations():
    """
    Create all visualizations for the topic modeling analysis.
    """
    logger.info("Starting topic modeling visualization")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Find the latest data files
    topics_file = find_latest_file(TOPICS_FILE_PATTERN, BASE_OUTPUT_DIR)
    stratified_file = find_latest_file(STRATIFIED_FILE_PATTERN, BASE_OUTPUT_DIR)
    quality_metrics_file = find_latest_file(
        QUALITY_METRICS_FILE_PATTERN, BASE_OUTPUT_DIR
    )

    if not topics_file and not stratified_file and not quality_metrics_file:
        logger.error("No data files found for visualization")
        return

    # Process topic distribution plot
    if topics_file:
        logger.info("Creating topic distribution plot...")
        topics_df = load_data(topics_file)
        if topics_df is not None:
            topic_dist_output_path = os.path.join(
                output_dir, "topic_distribution_by_size.png"
            )
            create_topic_distribution_plot(
                topics_df,
                topic_dist_output_path,
                "Topic Distribution by Document Count",
            )

    # Process topic evolution plot
    if stratified_file:
        logger.info("Creating topic evolution plot...")
        stratified_df = load_data(stratified_file)
        if stratified_df is not None:
            evolution_output_path = os.path.join(
                output_dir, "topic_evolution_over_time.png"
            )
            create_topic_evolution_plot(
                stratified_df,
                evolution_output_path,
                "Topic Evolution Over Time (Top 10 Topics)",
            )

            # Process condition comparison plot
            logger.info("Creating condition comparison plot...")
            condition_output_path = os.path.join(
                output_dir, "topic_distribution_by_condition.png"
            )
            create_condition_comparison_plot(
                stratified_df,
                condition_output_path,
                "Topic Distribution by Experimental Condition",
            )

    # Process quality metrics plot
    if quality_metrics_file:
        logger.info("Creating quality metrics plot...")
        quality_metrics = load_json_data(quality_metrics_file)
        if quality_metrics is not None:
            quality_output_path = os.path.join(
                output_dir, "topic_model_quality_metrics.png"
            )
            create_quality_metrics_plot(
                quality_metrics, quality_output_path, "Topic Model Quality Metrics"
            )

    # Print summary
    logger.info("=== VISUALIZATION SUMMARY ===")
    logger.info(f"Output directory: {output_dir}")

    # Count generated files
    png_files = [f for f in os.listdir(output_dir) if f.endswith(".png")]
    logger.info(f"Generated {len(png_files)} visualization files:")
    for file in png_files:
        logger.info(f"  - {file}")

    logger.info(f"All visualization assets saved in: {output_dir}")
    logger.info("Topic modeling visualization complete")


def main():
    """
    Main execution function for visualization.
    """
    try:
        create_visualizations()
    except Exception as e:
        logger.error(f"Error in visualization: {e}")
        raise


if __name__ == "__main__":
    main()
