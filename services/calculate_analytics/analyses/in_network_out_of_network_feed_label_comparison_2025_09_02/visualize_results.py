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
VISUALIZATIONS_BASE_DIR = os.path.join(current_dir, "results", "visualizations")

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

# Define colors and labels for conditions
CONDITION_COLORS = {
    "engagement": "red",
    "representative_diversification": "green",
    "reverse_chronological": "black",
}

# Define condition label mappings
CONDITION_LABELS = {
    "reverse_chronological": "Reverse Chronological (RC)",
    "engagement": "Engagement-Based (EB)",
    "representative_diversification": "Diversified Extremity (DE)",
    "all": "All Conditions",
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
    in_network_df: pd.DataFrame,
    out_network_df: pd.DataFrame,
    time_col: str,
    label_col: str,
    condition: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare data for time series plotting by aggregating by condition and time.

    Args:
        in_network_df: DataFrame with in-network data
        out_network_df: DataFrame with out-of-network data
        time_col: Name of the time column ('date' or 'week')
        label_col: Name of the label column to analyze
        condition: Specific condition to filter by, or None for all conditions

    Returns:
        Tuple of (in_network_agg_data, out_network_agg_data)
    """
    # Convert time column to datetime if it's 'date'
    if time_col == "date":
        in_network_df[time_col] = pd.to_datetime(in_network_df[time_col])
        out_network_df[time_col] = pd.to_datetime(out_network_df[time_col])

    # Filter by condition if specified (but not for "all")
    if condition is not None and condition != "all":
        in_network_df = in_network_df[in_network_df["condition"] == condition]
        out_network_df = out_network_df[out_network_df["condition"] == condition]

    # Group by time only - we want to aggregate across all users and conditions
    group_cols = [time_col]
    in_network_agg = (
        in_network_df.groupby(group_cols)[label_col]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    in_network_agg["std"] = in_network_agg["std"].fillna(0)
    in_network_agg["network_type"] = "in_network"

    # Group by time (and condition if not filtered), calculate mean and std across all users
    out_network_agg = (
        out_network_df.groupby(group_cols)[label_col]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    out_network_agg["std"] = out_network_agg["std"].fillna(0)
    out_network_agg["network_type"] = "out_of_network"

    return in_network_agg, out_network_agg


def create_combined_condition_plot(
    in_network_df: pd.DataFrame,
    out_network_df: pd.DataFrame,
    time_col: str,
    label_col: str,
    output_path: str,
    title: str,
    ylabel: str,
):
    """
    Create a combined plot showing all conditions with in-network vs out-of-network lines.

    Args:
        in_network_df: DataFrame with in-network data
        out_network_df: DataFrame with out-of-network data
        time_col: Name of the time column
        label_col: Name of the label column
        output_path: Path to save the plot
        title: Title for the plot
        ylabel: Y-axis label
    """
    plt.figure(figsize=(14, 8))

    # Define colors for conditions
    condition_colors = {
        "engagement": "red",
        "representative_diversification": "green",
        "reverse_chronological": "black",
    }

    # Convert time column to datetime if needed
    if time_col == "date":
        in_network_df[time_col] = pd.to_datetime(in_network_df[time_col])
        out_network_df[time_col] = pd.to_datetime(out_network_df[time_col])

    # Get unique conditions (excluding 'all')
    conditions = [c for c in in_network_df["condition"].unique() if c != "all"]

    # Create plots for each condition
    for condition in conditions:
        if condition not in condition_colors:
            continue

        color = condition_colors[condition]
        condition_label = CONDITION_LABELS.get(
            condition, condition.replace("_", " ").title()
        )

        # Filter data for this condition
        in_network_condition = in_network_df[in_network_df["condition"] == condition]
        out_network_condition = out_network_df[out_network_df["condition"] == condition]

        # Aggregate by time
        in_network_agg = (
            in_network_condition.groupby(time_col)[label_col]
            .agg(["mean"])
            .reset_index()
            .sort_values(time_col)
        )

        out_network_agg = (
            out_network_condition.groupby(time_col)[label_col]
            .agg(["mean"])
            .reset_index()
            .sort_values(time_col)
        )

        # Plot in-network (solid line)
        if len(in_network_agg) > 0:
            plt.plot(
                in_network_agg[time_col],
                in_network_agg["mean"],
                color=color,
                linewidth=2.5,
                label=f"{condition_label} (In-Network)",
                marker="o",
                markersize=4,
                linestyle="-",
            )

        # Plot out-of-network (dashed line)
        if len(out_network_agg) > 0:
            plt.plot(
                out_network_agg[time_col],
                out_network_agg["mean"],
                color=color,
                linewidth=2.5,
                label=f"{condition_label} (Out-of-Network)",
                marker="s",
                markersize=4,
                linestyle="--",
            )

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Week", fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend(fontsize=10, loc="upper right")

    # Set y-axis limits based on data range with better scaling
    all_values = []
    for condition in conditions:
        if condition in condition_colors:
            in_condition = in_network_df[in_network_df["condition"] == condition]
            out_condition = out_network_df[out_network_df["condition"] == condition]
            if len(in_condition) > 0:
                all_values.extend(in_condition[label_col].dropna().tolist())
            if len(out_condition) > 0:
                all_values.extend(out_condition[label_col].dropna().tolist())

    if all_values:
        min_val = min(all_values)
        max_val = max(all_values)

        # Better scaling: floor(max_val + 0.2, 1.0) for upper bound
        y_max = min(1.0, max_val + 0.2)

        # Set minimum to 0 or slightly below the minimum
        y_min = max(0, min_val - 0.05)

        plt.ylim(y_min, y_max)

    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Combined condition plot saved to {output_path}")


def create_comparison_plot(
    in_network_agg: pd.DataFrame,
    out_network_agg: pd.DataFrame,
    time_col: str,
    label_col: str,
    output_path: str,
    title: str,
    ylabel: str,
    condition: Optional[str] = None,
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
        condition: Specific condition name for the plot
    """
    plt.figure(figsize=(14, 8))

    # Sort data by time
    in_network_agg = in_network_agg.sort_values(time_col)
    out_network_agg = out_network_agg.sort_values(time_col)

    # Plot in-network data
    if len(in_network_agg) > 0:
        plt.plot(
            in_network_agg[time_col],
            in_network_agg["mean"],
            color="blue",
            linewidth=2.5,
            label="In-Network (Solid)",
            marker="o",
            markersize=4,
            linestyle="-",
        )

    # Plot out-of-network data
    if len(out_network_agg) > 0:
        plt.plot(
            out_network_agg[time_col],
            out_network_agg["mean"],
            color="red",
            linewidth=2.5,
            label="Out-of-Network (Dashed)",
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
        label_col: Name of the label column (e.g., 'feed_average_toxic')

    Returns:
        Trait name (e.g., 'toxic')
    """
    # Remove common prefixes
    trait = label_col.replace("feed_average_", "").replace("feed_proportion_", "")
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
    Create all visualizations comparing in-network vs out-of-network feed content.
    """
    logger.info("Starting in-network vs out-of-network feed content visualization")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Hardcoded file paths for the specific analysis results
    in_network_daily_file = os.path.join(
        BASE_OUTPUT_DIR,
        "2025-09-02-04:21:25",
        "daily_in_network_feed_content_analysis_2025-09-02-04:21:25.csv",
    )
    in_network_weekly_file = os.path.join(
        BASE_OUTPUT_DIR,
        "2025-09-02-04:21:25",
        "weekly_in_network_feed_content_analysis_2025-09-02-04:21:25.csv",
    )
    out_network_daily_file = os.path.join(
        BASE_OUTPUT_DIR,
        "2025-09-02-04:01:15",
        "daily_out_of_network_feed_content_analysis_2025-09-02-04:01:15.csv",
    )
    out_network_weekly_file = os.path.join(
        BASE_OUTPUT_DIR,
        "2025-09-02-04:01:15",
        "weekly_out_of_network_feed_content_analysis_2025-09-02-04:01:15.csv",
    )

    # Check if all required files exist
    required_files = [
        in_network_daily_file,
        in_network_weekly_file,
        out_network_daily_file,
        out_network_weekly_file,
    ]

    missing_files = [f for f in required_files if not os.path.exists(f)]
    if missing_files:
        logger.error(f"Missing required files: {missing_files}")
        return

    # Define conditions to create separate plots for
    conditions = [
        "all",
        "reverse_chronological",
        "representative_diversification",
        "engagement",
    ]

    # Process daily data
    logger.info("Processing daily data...")

    # Load the data files
    in_network_daily_df = load_data(in_network_daily_file)
    out_network_daily_df = load_data(out_network_daily_file)

    if in_network_daily_df is not None and out_network_daily_df is not None:
        # Get label columns (should be the same for both)
        label_columns = get_label_columns(in_network_daily_df)

        # Create visualizations for each label and condition
        for label_col in label_columns:
            logger.info(f"Creating daily visualizations for {label_col}...")

            # Categorize the label and extract trait name
            category = categorize_label(label_col)
            trait = extract_trait_name(label_col)

            # Create directory structure
            trait_dir = create_directory_structure(output_dir, "daily", category, trait)

            for condition in conditions:
                logger.info(f"  Creating plot for condition: {condition}")

                # Prepare data for plotting
                condition_filter = None if condition == "all" else condition
                in_network_agg, out_network_agg = prepare_data_for_plotting(
                    in_network_daily_df,
                    out_network_daily_df,
                    "date",
                    label_col,
                    condition_filter,
                )

                # Create visualization
                output_path = os.path.join(
                    trait_dir,
                    f"daily_{trait}_{condition}_in_network_vs_out_of_network.png",
                )
                condition_label = CONDITION_LABELS.get(
                    condition, condition.replace("_", " ").title()
                )
                title = f"Daily {trait.replace('_', ' ').title()} - {condition_label} - In-Network vs Out-of-Network"
                ylabel = "Average"

                create_comparison_plot(
                    in_network_agg,
                    out_network_agg,
                    "date",
                    label_col,
                    output_path,
                    title,
                    ylabel,
                    condition,
                )
    else:
        logger.error("Failed to load daily data")

    # Create combined daily visualizations for all traits
    if in_network_daily_df is not None and out_network_daily_df is not None:
        logger.info("Creating combined daily visualizations for all traits...")

        # Get label columns from daily data
        daily_label_columns = get_label_columns(in_network_daily_df)

        # Create combined daily directory
        combined_daily_dir = os.path.join(output_dir, "combined", "daily")
        os.makedirs(combined_daily_dir, exist_ok=True)

        for label_col in daily_label_columns:
            logger.info(f"Creating combined daily visualization for {label_col}...")

            # Categorize the label and extract trait name
            category = categorize_label(label_col)
            trait = extract_trait_name(label_col)

            # Create combined plot
            combined_output_path = os.path.join(
                combined_daily_dir,
                f"daily_combined_{trait}_{category}_in_network_vs_out_of_network.png",
            )
            title = f"Daily {trait.replace('_', ' ').title()} ({category.title()}) - In-Network vs Out-of-Network"
            ylabel = "Average" if category == "average" else "Proportion"

            create_combined_condition_plot(
                in_network_daily_df,
                out_network_daily_df,
                "date",
                label_col,
                combined_output_path,
                title,
                ylabel,
            )

    # Process weekly data
    logger.info("Processing weekly data...")

    # Load the data files
    in_network_weekly_df = load_data(in_network_weekly_file)
    out_network_weekly_df = load_data(out_network_weekly_file)

    if in_network_weekly_df is not None and out_network_weekly_df is not None:
        # Get label columns (should be the same for both)
        label_columns = get_label_columns(in_network_weekly_df)

        # Create visualizations for each label and condition
        for label_col in label_columns:
            logger.info(f"Creating weekly visualizations for {label_col}...")

            # Categorize the label and extract trait name
            category = categorize_label(label_col)
            trait = extract_trait_name(label_col)

            # Create directory structure
            trait_dir = create_directory_structure(
                output_dir, "weekly", category, trait
            )

            for condition in conditions:
                logger.info(f"  Creating plot for condition: {condition}")

                # Prepare data for plotting
                condition_filter = None if condition == "all" else condition
                in_network_agg, out_network_agg = prepare_data_for_plotting(
                    in_network_weekly_df,
                    out_network_weekly_df,
                    "week",
                    label_col,
                    condition_filter,
                )

                # Create visualization
                output_path = os.path.join(
                    trait_dir,
                    f"weekly_{trait}_{condition}_in_network_vs_out_of_network.png",
                )
                condition_label = CONDITION_LABELS.get(
                    condition, condition.replace("_", " ").title()
                )
                title = f"Weekly {trait.replace('_', ' ').title()} - {condition_label} - In-Network vs Out-of-Network"
                ylabel = "Average"

                create_comparison_plot(
                    in_network_agg,
                    out_network_agg,
                    "week",
                    label_col,
                    output_path,
                    title,
                    ylabel,
                    condition,
                )
    else:
        logger.error("Failed to load weekly data")

    # Create combined visualizations for all traits (all conditions on one plot)
    if in_network_weekly_df is not None and out_network_weekly_df is not None:
        logger.info("Creating combined visualizations for all traits...")

        # Get label columns from weekly data
        weekly_label_columns = get_label_columns(in_network_weekly_df)

        # Create combined weekly directory
        combined_weekly_dir = os.path.join(output_dir, "combined", "weekly")
        os.makedirs(combined_weekly_dir, exist_ok=True)

        for label_col in weekly_label_columns:
            logger.info(f"Creating combined weekly visualization for {label_col}...")

            # Categorize the label and extract trait name
            category = categorize_label(label_col)
            trait = extract_trait_name(label_col)

            # Create combined plot
            combined_output_path = os.path.join(
                combined_weekly_dir,
                f"weekly_combined_{trait}_{category}_in_network_vs_out_of_network.png",
            )
            title = f"Weekly {trait.replace('_', ' ').title()} ({category.title()}) - In-Network vs Out-of-Network"
            ylabel = "Average" if category == "average" else "Proportion"

            create_combined_condition_plot(
                in_network_weekly_df,
                out_network_weekly_df,
                "week",
                label_col,
                combined_output_path,
                title,
                ylabel,
            )

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
