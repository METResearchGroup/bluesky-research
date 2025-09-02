"""
Visualization for Baseline Measures Across All Posts

This script generates time series visualizations for baseline content label metrics
across all labeled posts in the dataset. It creates time series plots showing
the evolution of content characteristics over time.

The visualization shows:
- Daily time series for each baseline metric
- Weekly time series for each baseline metric
- Separate plots for each content label (toxicity, constructive, political, etc.)
- Organized by metric type (average vs proportion)

Output files:
- Multiple PNG files, one for each label and time aggregation (daily/weekly)
- Files organized in: results/visualizations/<timestamp>/<daily/weekly>/<average/proportion>/<trait>.png
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import glob
from typing import Optional, List
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str

logger = get_logger(__name__)

# Base output directory
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(current_dir, "results")
VISUALIZATIONS_BASE_DIR = os.path.join(current_dir, "results", "visualizations")

# Expected file patterns
DAILY_PATTERN = "daily_baseline_content_label_metrics_*.csv"
WEEKLY_PATTERN = "weekly_baseline_content_label_metrics_*.csv"


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
    output_dir = os.path.join(VISUALIZATIONS_BASE_DIR, clean_timestamp)

    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created visualization output directory: {output_dir}")

    return output_dir


def find_latest_files_by_pattern(pattern: str, base_dir: str) -> List[str]:
    """
    Find all files matching the pattern in the base directory, sorted by modification time.

    Args:
        pattern: File pattern to match (e.g., "daily_*.csv")
        base_dir: Base directory to search in

    Returns:
        List of paths to matching files, sorted by modification time (newest first)
    """
    # Search for files matching the pattern
    search_pattern = os.path.join(base_dir, "**", pattern)
    matching_files = glob.glob(search_pattern, recursive=True)

    if not matching_files:
        logger.warning(f"No files found matching pattern: {search_pattern}")
        return []

    # Sort by modification time and return (newest first)
    matching_files.sort(key=os.path.getmtime, reverse=True)
    logger.info(f"Found {len(matching_files)} files matching pattern: {pattern}")
    return matching_files


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


def get_label_columns(df: pd.DataFrame) -> List[str]:
    """
    Extract label columns from the dataframe, excluding metadata columns.

    Args:
        df: DataFrame with the data

    Returns:
        List of label column names
    """
    # Exclude metadata columns
    metadata_columns = {"bluesky_user_did", "condition", "date", "week", "handle"}

    # Get all columns that are not metadata
    label_columns = [col for col in df.columns if col not in metadata_columns]

    logger.info(f"Found {len(label_columns)} label columns: {label_columns[:5]}...")
    return label_columns


def prepare_data_for_plotting(
    df: pd.DataFrame,
    time_col: str,
    label_col: str,
) -> pd.DataFrame:
    """
    Prepare data for time series plotting by aggregating by time.

    Args:
        df: DataFrame with the data
        time_col: Name of the time column ('date' or 'week')
        label_col: Name of the label column to analyze

    Returns:
        Aggregated DataFrame ready for plotting
    """
    # Convert time column to datetime if it's 'date'
    if time_col == "date":
        df[time_col] = pd.to_datetime(df[time_col])

    # Group by time and calculate mean, std, and count
    agg_data = (
        df.groupby(time_col)[label_col].agg(["mean", "std", "count"]).reset_index()
    )
    agg_data["std"] = agg_data["std"].fillna(0)

    return agg_data


def create_time_series_plot(
    agg_data: pd.DataFrame,
    time_col: str,
    label_col: str,
    output_path: str,
    title: str,
    ylabel: str,
):
    """
    Create a time series plot for a specific baseline metric.

    Args:
        agg_data: Aggregated data ready for plotting
        time_col: Name of the time column
        label_col: Name of the label column
        output_path: Path to save the plot
        title: Title for the plot
        ylabel: Y-axis label
    """
    plt.figure(figsize=(14, 8))

    # Sort data by time
    agg_data = agg_data.sort_values(time_col)

    # Plot the main line
    if len(agg_data) > 0:
        plt.plot(
            agg_data[time_col],
            agg_data["mean"],
            color="blue",
            linewidth=2.5,
            marker="o",
            markersize=4,
            linestyle="-",
        )

        # Add error bars if we have standard deviation
        if agg_data["std"].sum() > 0:
            plt.errorbar(
                agg_data[time_col],
                agg_data["mean"],
                yerr=agg_data["std"],
                color="blue",
                alpha=0.3,
                capsize=3,
            )

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Date" if time_col == "date" else "Week", fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.grid(True, alpha=0.3)

    # Format x-axis
    if time_col == "date":
        plt.xticks(rotation=45)
        # Set x-axis limits to show full range
        if len(agg_data) > 0:
            plt.xlim(agg_data[time_col].min(), agg_data[time_col].max())

    # Set y-axis limits based on data range
    if len(agg_data) > 0 and agg_data["mean"].notna().any():
        min_val = agg_data["mean"].min()
        max_val = agg_data["mean"].max()
        # Add some padding
        padding = (max_val - min_val) * 0.1
        plt.ylim(max(0, min_val - padding), min(1, max_val + padding))

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Time series plot saved to {output_path}")


def categorize_label(label_col: str) -> str:
    """
    Categorize a label column as 'average' or 'proportion'.

    Args:
        label_col: Name of the label column

    Returns:
        Category string ('average' or 'proportion')
    """
    if "average" in label_col.lower():
        return "average"
    elif "proportion" in label_col.lower():
        return "proportion"
    else:
        # Default to average for unknown types
        return "average"


def extract_trait_name(label_col: str) -> str:
    """
    Extract the trait name from a label column.

    Args:
        label_col: Name of the label column (e.g., 'baseline_average_toxic')

    Returns:
        Trait name (e.g., 'toxic')
    """
    # Remove common prefixes
    trait = label_col.replace("baseline_average_", "").replace(
        "baseline_proportion_", ""
    )
    return trait


def create_directory_structure(
    base_dir: str, time_type: str, category: str, trait: str
) -> str:
    """
    Create the directory structure for saving plots.

    Args:
        base_dir: Base output directory
        time_type: 'daily' or 'weekly'
        category: 'average' or 'proportion'
        trait: Trait name (e.g., 'toxic')

    Returns:
        Path to the created directory
    """
    dir_path = os.path.join(base_dir, time_type, category, trait)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def create_visualizations():
    """
    Create all visualizations for baseline content label metrics.
    """
    logger.info("Starting baseline content label metrics visualization")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Find the latest result files
    daily_files = find_latest_files_by_pattern(DAILY_PATTERN, BASE_OUTPUT_DIR)
    weekly_files = find_latest_files_by_pattern(WEEKLY_PATTERN, BASE_OUTPUT_DIR)

    if not daily_files and not weekly_files:
        logger.error(
            "No baseline result files found. Please run the main analysis first."
        )
        return

    # Process daily data
    if daily_files:
        logger.info("Processing daily data...")
        daily_file = daily_files[0]  # Use the most recent file
        daily_df = load_data(daily_file)

        if daily_df is not None:
            # Get label columns
            label_columns = get_label_columns(daily_df)

            # Create visualizations for each label
            for label_col in label_columns:
                logger.info(f"Creating daily visualization for {label_col}...")

                # Categorize the label and extract trait name
                category = categorize_label(label_col)
                trait = extract_trait_name(label_col)

                # Create directory structure
                trait_dir = create_directory_structure(
                    output_dir, "daily", category, trait
                )

                # Prepare data for plotting
                agg_data = prepare_data_for_plotting(
                    daily_df,
                    "date",
                    label_col,
                )

                # Create visualization
                output_path = os.path.join(
                    trait_dir,
                    f"daily_{trait}_baseline.png",
                )
                title = f"Daily {trait.replace('_', ' ').title()} - Baseline Across All Posts"
                ylabel = f"{category.title()} {trait.replace('_', ' ').title()}"

                create_time_series_plot(
                    agg_data,
                    "date",
                    label_col,
                    output_path,
                    title,
                    ylabel,
                )
        else:
            logger.error("Failed to load daily data")
    else:
        logger.warning("No daily files found")

    # Process weekly data
    if weekly_files:
        logger.info("Processing weekly data...")
        weekly_file = weekly_files[0]  # Use the most recent file
        weekly_df = load_data(weekly_file)

        if weekly_df is not None:
            # Get label columns
            label_columns = get_label_columns(weekly_df)

            # Create visualizations for each label
            for label_col in label_columns:
                logger.info(f"Creating weekly visualization for {label_col}...")

                # Categorize the label and extract trait name
                category = categorize_label(label_col)
                trait = extract_trait_name(label_col)

                # Create directory structure
                trait_dir = create_directory_structure(
                    output_dir, "weekly", category, trait
                )

                # Prepare data for plotting
                agg_data = prepare_data_for_plotting(
                    weekly_df,
                    "week",
                    label_col,
                )

                # Create visualization
                output_path = os.path.join(
                    trait_dir,
                    f"weekly_{trait}_baseline.png",
                )
                title = f"Weekly {trait.replace('_', ' ').title()} - Baseline Across All Posts"
                ylabel = f"{category.title()} {trait.replace('_', ' ').title()}"

                create_time_series_plot(
                    agg_data,
                    "week",
                    label_col,
                    output_path,
                    title,
                    ylabel,
                )
        else:
            logger.error("Failed to load weekly data")
    else:
        logger.warning("No weekly files found")

    # Print summary
    logger.info("=== VISUALIZATION SUMMARY ===")
    logger.info(f"Output directory: {output_dir}")

    # Count generated files recursively
    png_count = 0
    for root, dirs, files in os.walk(output_dir):
        png_files = [f for f in files if f.endswith(".png")]
        png_count += len(png_files)
        if png_files:
            logger.info(
                f"Directory {os.path.relpath(root, output_dir)}: {len(png_files)} files"
            )

    logger.info(f"Generated {png_count} total visualization files")
    logger.info(f"All visualization assets saved in: {output_dir}")
    logger.info("Baseline content label metrics visualization complete")


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
