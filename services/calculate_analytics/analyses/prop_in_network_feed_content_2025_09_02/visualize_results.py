"""
Visualization for In-Network Feed Content Analysis

This script generates time series visualizations for the proportion of in-network
feed content, split by user condition. It creates plots similar to the toxicity
analysis but focused on in-network content proportions.

The visualization shows:
- Daily time series of average in-network proportions by condition
- Weekly time series of average in-network proportions by condition
- Individual data points as translucent bars for variability indication

Output files:
- daily_in_network_proportions_by_condition.png
- weekly_in_network_proportions_by_condition.png
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str

logger = get_logger(__name__)

# Base output directory
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(current_dir, "results")

# Expected file paths (will be updated with actual timestamped files)
DAILY_FILE_PATTERN = "daily_in_network_feed_content_proportions_*.csv"
WEEKLY_FILE_PATTERN = "weekly_in_network_feed_content_proportions_*.csv"


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
        pattern: File pattern to match (e.g., "daily_*.csv")
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


def prepare_data_for_plotting(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    """
    Prepare data for time series plotting by aggregating by condition and time.

    Args:
        df: DataFrame with the data
        time_col: Name of the time column ('date' or 'week')

    Returns:
        DataFrame aggregated by condition and time
    """
    # Convert time column to datetime if it's 'date'
    if time_col == "date":
        df[time_col] = pd.to_datetime(df[time_col])

    # Group by condition and time, calculate mean and std
    agg_data = (
        df.groupby(["condition", time_col])["feed_average_prop_in_network_posts"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )

    # Fill NaN std values with 0 (for single observations)
    agg_data["std"] = agg_data["std"].fillna(0)

    return agg_data


def create_time_series_plot(
    df: pd.DataFrame,
    time_col: str,
    output_path: str,
    title: str,
    ylabel: str = "Average In-Network Proportion",
):
    """
    Create a time series plot showing in-network proportions by condition.

    Args:
        df: DataFrame with aggregated data
        time_col: Name of the time column
        output_path: Path to save the plot
        title: Title for the plot
        ylabel: Y-axis label
    """
    plt.figure(figsize=(14, 8))

    # Define colors and labels for each condition
    condition_colors = {
        "engagement": "red",
        "representative_diversification": "green",
        "reverse_chronological": "black",
    }

    # Define condition label mappings
    condition_labels = {
        "reverse_chronological": "Reverse Chronological (RC)",
        "engagement": "Engagement-Based (EB)",
        "representative_diversification": "Diversified Extremity (DE)",
    }

    # Get unique conditions
    conditions = df["condition"].unique()

    # Create the main plot
    for condition in conditions:
        condition_data = df[df["condition"] == condition].sort_values(time_col)

        if len(condition_data) == 0:
            continue

        color = condition_colors.get(condition, "blue")

        # Plot the main line (mean)
        condition_label = condition_labels.get(condition, condition)
        plt.plot(
            condition_data[time_col],
            condition_data["mean"],
            color=color,
            linewidth=2.5,
            label=condition_label,
            marker="o",
            markersize=4,
        )

        # Add individual data points as translucent bars for one condition
        # (similar to the reference plot showing variability)
        if condition == "reverse_chronological":
            # Get the original data for this condition to show individual points
            original_data = df[df["condition"] == condition]
            if len(original_data) > 0:
                # Create scatter plot with individual data points
                plt.scatter(
                    original_data[time_col],
                    original_data["mean"],
                    color="grey",
                    alpha=0.3,
                    s=20,
                    zorder=1,
                )

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Date" if time_col == "date" else "Week", fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend(fontsize=11, loc="upper right")

    # Format x-axis
    if time_col == "date":
        plt.xticks(rotation=45)
        # Set x-axis limits to show full range
        plt.xlim(df[time_col].min(), df[time_col].max())

    # Set y-axis limits
    plt.ylim(0, 1)

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Time series plot saved to {output_path}")


def create_visualizations():
    """
    Create all visualizations for the in-network feed content analysis.
    """
    logger.info("Starting in-network feed content visualization")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Find the latest data files
    daily_file = find_latest_file(DAILY_FILE_PATTERN, BASE_OUTPUT_DIR)
    weekly_file = find_latest_file(WEEKLY_FILE_PATTERN, BASE_OUTPUT_DIR)

    if not daily_file and not weekly_file:
        logger.error("No data files found for visualization")
        return

    # Process daily data
    if daily_file:
        logger.info("Processing daily data...")
        daily_df = load_data(daily_file)

        if daily_df is not None:
            # Prepare data for plotting
            daily_plot_data = prepare_data_for_plotting(daily_df, "date")

            # Create daily visualization
            daily_output_path = os.path.join(
                output_dir, "daily_in_network_proportions_by_condition.png"
            )
            create_time_series_plot(
                daily_plot_data,
                "date",
                daily_output_path,
                "Time Series of Average In-Network Feed Content Proportion by Condition",
                "Average In-Network Proportion",
            )
        else:
            logger.error("Failed to load daily data")

    # Process weekly data
    if weekly_file:
        logger.info("Processing weekly data...")
        weekly_df = load_data(weekly_file)

        if weekly_df is not None:
            # Prepare data for plotting
            weekly_plot_data = prepare_data_for_plotting(weekly_df, "week")

            # Create weekly visualization
            weekly_output_path = os.path.join(
                output_dir, "weekly_in_network_proportions_by_condition.png"
            )
            create_time_series_plot(
                weekly_plot_data,
                "week",
                weekly_output_path,
                "Weekly Average In-Network Feed Content Proportion by Condition",
                "Average In-Network Proportion",
            )
        else:
            logger.error("Failed to load weekly data")

    # Print summary
    logger.info("=== VISUALIZATION SUMMARY ===")
    logger.info(f"Output directory: {output_dir}")

    # Count generated files
    png_files = [f for f in os.listdir(output_dir) if f.endswith(".png")]
    logger.info(f"Generated {len(png_files)} visualization files:")
    for file in png_files:
        logger.info(f"  - {file}")

    logger.info(f"All visualization assets saved in: {output_dir}")
    logger.info("In-network feed content visualization complete")


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
