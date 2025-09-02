"""
Visualization for In-Network vs Out-of-Network Feed Label Comparison

This script generates time series visualizations comparing content labels between
in-network and out-of-network posts used in feeds. It creates overlay plots
showing the differences in content characteristics between the two network types.

The visualization shows:
- Daily time series comparing in-network vs out-of-network for each label
- Weekly time series comparing in-network vs out-of-network for each label
- Separate plots for each content label (toxicity, constructive, political, etc.)
- Overlay visualization with distinct colors for each network type

Output files:
- Multiple PNG files, one for each label and time aggregation (daily/weekly)
- Files named like: daily_toxic_in_network_vs_out_of_network.png
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import glob
from typing import Optional, List, Tuple
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str

logger = get_logger(__name__)

# Base output directory
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(current_dir, "results")

# Expected file patterns
IN_NETWORK_DAILY_PATTERN = "daily_in_network_feed_content_analysis_*.csv"
IN_NETWORK_WEEKLY_PATTERN = "weekly_in_network_feed_content_analysis_*.csv"
OUT_NETWORK_DAILY_PATTERN = "daily_out_of_network_feed_content_analysis_*.csv"
OUT_NETWORK_WEEKLY_PATTERN = "weekly_out_of_network_feed_content_analysis_*.csv"

# Define colors for network types
NETWORK_COLORS = {
    "in_network": "blue",
    "out_of_network": "red",
}

# Define colors for conditions (from existing visualization)
CONDITION_COLORS = {
    "engagement": "red",
    "representative_diversification": "green",
    "reverse_chronological": "black",
}


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
    metadata_columns = {"bluesky_user_did", "condition", "date", "week", "user_handle"}

    # Get all columns that are not metadata
    label_columns = [col for col in df.columns if col not in metadata_columns]

    logger.info(f"Found {len(label_columns)} label columns: {label_columns[:5]}...")
    return label_columns


def prepare_data_for_plotting(
    in_network_df: pd.DataFrame,
    out_network_df: pd.DataFrame,
    time_col: str,
    label_col: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare data for time series plotting by aggregating by condition and time.

    Args:
        in_network_df: DataFrame with in-network data
        out_network_df: DataFrame with out-of-network data
        time_col: Name of the time column ('date' or 'week')
        label_col: Name of the label column to analyze

    Returns:
        Tuple of (in_network_agg_data, out_network_agg_data)
    """
    # Convert time column to datetime if it's 'date'
    if time_col == "date":
        in_network_df[time_col] = pd.to_datetime(in_network_df[time_col])
        out_network_df[time_col] = pd.to_datetime(out_network_df[time_col])

    # Group by condition and time, calculate mean and std for in-network
    in_network_agg = (
        in_network_df.groupby(["condition", time_col])[label_col]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    in_network_agg["std"] = in_network_agg["std"].fillna(0)
    in_network_agg["network_type"] = "in_network"

    # Group by condition and time, calculate mean and std for out-of-network
    out_network_agg = (
        out_network_df.groupby(["condition", time_col])[label_col]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    out_network_agg["std"] = out_network_agg["std"].fillna(0)
    out_network_agg["network_type"] = "out_of_network"

    return in_network_agg, out_network_agg


def create_comparison_plot(
    in_network_agg: pd.DataFrame,
    out_network_agg: pd.DataFrame,
    time_col: str,
    label_col: str,
    output_path: str,
    title: str,
    ylabel: str,
):
    """
    Create a comparison plot showing in-network vs out-of-network for a specific label.

    Args:
        in_network_agg: Aggregated in-network data
        out_network_agg: Aggregated out-of-network data
        time_col: Name of the time column
        label_col: Name of the label column
        output_path: Path to save the plot
        title: Title for the plot
        ylabel: Y-axis label
    """
    plt.figure(figsize=(14, 8))

    # Get unique conditions
    conditions = set(in_network_agg["condition"].unique()) | set(
        out_network_agg["condition"].unique()
    )

    # Create the main plot
    for condition in conditions:
        # Plot in-network data
        in_condition_data = in_network_agg[
            in_network_agg["condition"] == condition
        ].sort_values(time_col)
        if len(in_condition_data) > 0:
            plt.plot(
                in_condition_data[time_col],
                in_condition_data["mean"],
                color=CONDITION_COLORS.get(condition, "blue"),
                linewidth=2.5,
                label=f"{condition} (In-Network)",
                marker="o",
                markersize=4,
                linestyle="-",
            )

        # Plot out-of-network data
        out_condition_data = out_network_agg[
            out_network_agg["condition"] == condition
        ].sort_values(time_col)
        if len(out_condition_data) > 0:
            plt.plot(
                out_condition_data[time_col],
                out_condition_data["mean"],
                color=CONDITION_COLORS.get(condition, "blue"),
                linewidth=2.5,
                label=f"{condition} (Out-of-Network)",
                marker="s",
                markersize=4,
                linestyle="--",
            )

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Date" if time_col == "date" else "Week", fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend(fontsize=10, loc="upper right")
    plt.grid(True, alpha=0.3)

    # Format x-axis
    if time_col == "date":
        plt.xticks(rotation=45)
        # Set x-axis limits to show full range
        all_dates = list(in_network_agg[time_col]) + list(out_network_agg[time_col])
        if all_dates:
            plt.xlim(min(all_dates), max(all_dates))

    # Set y-axis limits based on data range
    all_values = list(in_network_agg["mean"]) + list(out_network_agg["mean"])
    if all_values:
        min_val = min(all_values)
        max_val = max(all_values)
        # Add some padding
        padding = (max_val - min_val) * 0.1
        plt.ylim(max(0, min_val - padding), min(1, max_val + padding))

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Comparison plot saved to {output_path}")


def create_visualizations():
    """
    Create all visualizations comparing in-network vs out-of-network feed content.
    """
    logger.info("Starting in-network vs out-of-network feed content visualization")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Find the latest data files
    in_network_daily_files = find_latest_files_by_pattern(
        IN_NETWORK_DAILY_PATTERN, BASE_OUTPUT_DIR
    )
    in_network_weekly_files = find_latest_files_by_pattern(
        IN_NETWORK_WEEKLY_PATTERN, BASE_OUTPUT_DIR
    )
    out_network_daily_files = find_latest_files_by_pattern(
        OUT_NETWORK_DAILY_PATTERN, BASE_OUTPUT_DIR
    )
    out_network_weekly_files = find_latest_files_by_pattern(
        OUT_NETWORK_WEEKLY_PATTERN, BASE_OUTPUT_DIR
    )

    if not (
        in_network_daily_files
        or in_network_weekly_files
        or out_network_daily_files
        or out_network_weekly_files
    ):
        logger.error("No data files found for visualization")
        return

    # Process daily data
    if in_network_daily_files and out_network_daily_files:
        logger.info("Processing daily data...")

        # Load the most recent files
        in_network_daily_df = load_data(in_network_daily_files[0])
        out_network_daily_df = load_data(out_network_daily_files[0])

        if in_network_daily_df is not None and out_network_daily_df is not None:
            # Get label columns (should be the same for both)
            label_columns = get_label_columns(in_network_daily_df)

            # Create visualizations for each label
            for label_col in label_columns:
                logger.info(f"Creating daily visualization for {label_col}...")

                # Prepare data for plotting
                in_network_agg, out_network_agg = prepare_data_for_plotting(
                    in_network_daily_df, out_network_daily_df, "date", label_col
                )

                # Create visualization
                output_path = os.path.join(
                    output_dir, f"daily_{label_col}_in_network_vs_out_of_network.png"
                )
                title = f"Daily {label_col.replace('_', ' ').title()} - In-Network vs Out-of-Network"
                ylabel = f"Average {label_col.replace('_', ' ').title()}"

                create_comparison_plot(
                    in_network_agg,
                    out_network_agg,
                    "date",
                    label_col,
                    output_path,
                    title,
                    ylabel,
                )
        else:
            logger.error("Failed to load daily data")

    # Process weekly data
    if in_network_weekly_files and out_network_weekly_files:
        logger.info("Processing weekly data...")

        # Load the most recent files
        in_network_weekly_df = load_data(in_network_weekly_files[0])
        out_network_weekly_df = load_data(out_network_weekly_files[0])

        if in_network_weekly_df is not None and out_network_weekly_df is not None:
            # Get label columns (should be the same for both)
            label_columns = get_label_columns(in_network_weekly_df)

            # Create visualizations for each label
            for label_col in label_columns:
                logger.info(f"Creating weekly visualization for {label_col}...")

                # Prepare data for plotting
                in_network_agg, out_network_agg = prepare_data_for_plotting(
                    in_network_weekly_df, out_network_weekly_df, "week", label_col
                )

                # Create visualization
                output_path = os.path.join(
                    output_dir, f"weekly_{label_col}_in_network_vs_out_of_network.png"
                )
                title = f"Weekly {label_col.replace('_', ' ').title()} - In-Network vs Out-of-Network"
                ylabel = f"Average {label_col.replace('_', ' ').title()}"

                create_comparison_plot(
                    in_network_agg,
                    out_network_agg,
                    "week",
                    label_col,
                    output_path,
                    title,
                    ylabel,
                )
        else:
            logger.error("Failed to load weekly data")

    # Print summary
    logger.info("=== VISUALIZATION SUMMARY ===")
    logger.info(f"Output directory: {output_dir}")

    # Count generated files
    png_files = [f for f in os.listdir(output_dir) if f.endswith(".png")]
    logger.info(f"Generated {len(png_files)} visualization files:")
    for file in sorted(png_files):
        logger.info(f"  - {file}")

    logger.info(f"All visualization assets saved in: {output_dir}")
    logger.info("In-network vs out-of-network feed content visualization complete")


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
