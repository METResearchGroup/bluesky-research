"""
Visualization for TF-IDF analysis results.

This script generates publication-quality visualizations for TF-IDF analysis results,
including comparative charts, keyword rankings, and stratified analysis plots.

The visualization shows:
- Top keywords by TF-IDF score (overall, by condition, by election period)
- Comparative charts across experimental conditions
- Pre/post election period comparisons
- Cross-dimensional analysis heatmaps

Output files:
- Multiple PNG files for different analysis dimensions
- Files organized by analysis type (overall, condition, election_period, cross_dimensional)
- Publication-ready charts with scientific styling
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
from typing import Optional, Tuple, Dict

from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str

logger = get_logger(__name__)

# Base output directory
current_dir = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(current_dir, "results")
VISUALIZATIONS_BASE_DIR = os.path.join(current_dir, "results", "visualizations")

# Expected file patterns for TF-IDF results
TFIDF_RESULTS_PATTERN = "tfidf_analysis_results_*.csv"
TFIDF_METADATA_PATTERN = "training_metadata_*.json"

# Define colors for different analysis dimensions
CONDITION_COLORS = {
    "control": "blue",
    "treatment": "red",
    "engagement": "green",
    "representative_diversification": "orange",
    "reverse_chronological": "purple",
}

ELECTION_PERIOD_COLORS = {
    "pre": "lightblue",
    "post": "darkblue",
}

# Scientific visualization settings
FIGURE_PARAMS = {"figsize": (12, 8), "dpi": 300, "facecolor": "white"}

COLOR_PALETTE = sns.color_palette("viridis", 10)


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


def find_latest_tfidf_results(base_dir: str) -> Optional[str]:
    """
    Find the latest TF-IDF results file.

    Args:
        base_dir: Base directory to search in

    Returns:
        Path to the latest TF-IDF results file, or None if not found
    """
    # Search for TF-IDF results files
    search_pattern = os.path.join(base_dir, "**", TFIDF_RESULTS_PATTERN)
    matching_files = glob.glob(search_pattern, recursive=True)

    if not matching_files:
        logger.warning(
            f"No TF-IDF results files found matching pattern: {search_pattern}"
        )
        return None

    # Sort by modification time and return the newest
    matching_files.sort(key=os.path.getmtime, reverse=True)
    latest_file = matching_files[0]
    logger.info(f"Found latest TF-IDF results file: {latest_file}")
    return latest_file


def load_tfidf_results(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load TF-IDF results CSV data.

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


def load_training_metadata(base_dir: str) -> Optional[Dict]:
    """
    Load training metadata from JSON file.

    Args:
        base_dir: Base directory to search for metadata

    Returns:
        Dictionary with metadata if successful, None otherwise
    """
    # Search for metadata files
    search_pattern = os.path.join(base_dir, "**", TFIDF_METADATA_PATTERN)
    matching_files = glob.glob(search_pattern, recursive=True)

    if not matching_files:
        logger.warning(
            f"No training metadata files found matching pattern: {search_pattern}"
        )
        return None

    # Get the latest metadata file
    matching_files.sort(key=os.path.getmtime, reverse=True)
    metadata_file = matching_files[0]

    try:
        import json

        with open(metadata_file, "r") as f:
            metadata = json.load(f)
        logger.info(f"Loaded training metadata from {os.path.basename(metadata_file)}")
        return metadata
    except Exception as e:
        logger.error(f"Error loading metadata from {metadata_file}: {e}")
        return None


def create_top_keywords_chart(
    scores_df: pd.DataFrame,
    title: str,
    output_path: str,
    top_n: int = 20,
    figsize: Tuple[int, int] = (12, 8),
) -> None:
    """
    Create a horizontal bar chart showing top keywords by TF-IDF score.

    Args:
        scores_df: DataFrame with 'term' and 'score' columns
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords to show
        figsize: Figure size tuple
    """
    if scores_df.empty:
        logger.warning(f"No data to create chart for: {title}")
        return

    # Get top N terms
    top_terms = scores_df.head(top_n)

    plt.figure(
        figsize=figsize, dpi=FIGURE_PARAMS["dpi"], facecolor=FIGURE_PARAMS["facecolor"]
    )

    # Create horizontal bar chart
    bars = plt.barh(range(len(top_terms)), top_terms["score"], color=COLOR_PALETTE[0])

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("TF-IDF Score", fontsize=12, fontweight="bold")
    plt.ylabel("Term", fontsize=12, fontweight="bold")

    # Set y-axis labels
    plt.yticks(range(len(top_terms)), top_terms["term"])

    # Add value labels on bars
    for i, (bar, score) in enumerate(zip(bars, top_terms["score"])):
        plt.text(
            bar.get_width() + 0.001,
            bar.get_y() + bar.get_height() / 2,
            f"{score:.3f}",
            ha="left",
            va="center",
            fontsize=9,
        )

    plt.grid(True, alpha=0.3, axis="x")
    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(
        output_path, dpi=FIGURE_PARAMS["dpi"], bbox_inches="tight", facecolor="white"
    )
    plt.close()

    logger.info(f"Top keywords chart saved to {output_path}")


def create_condition_comparison_chart(
    condition_results: Dict[str, pd.DataFrame],
    title: str,
    output_path: str,
    top_n: int = 15,
    figsize: Tuple[int, int] = (14, 10),
) -> None:
    """
    Create a comparison chart showing top keywords across conditions.

    Args:
        condition_results: Dictionary mapping conditions to TF-IDF results
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords per condition to show
        figsize: Figure size tuple
    """
    if not condition_results:
        logger.warning(f"No condition data to create chart for: {title}")
        return

    # Prepare data for plotting
    plot_data = []
    for condition, df in condition_results.items():
        if not df.empty:
            top_terms = df.head(top_n)
            top_terms["condition"] = condition
            plot_data.append(top_terms)

    if not plot_data:
        logger.warning(f"No valid condition data for chart: {title}")
        return

    combined_df = pd.concat(plot_data, ignore_index=True)

    plt.figure(
        figsize=figsize, dpi=FIGURE_PARAMS["dpi"], facecolor=FIGURE_PARAMS["facecolor"]
    )

    # Create grouped bar chart
    sns.barplot(
        data=combined_df,
        x="term",
        y="score",
        hue="condition",
        palette=CONDITION_COLORS,
        ax=plt.gca(),
    )

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Term", fontsize=12, fontweight="bold")
    plt.ylabel("TF-IDF Score", fontsize=12, fontweight="bold")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Condition", fontsize=10)
    plt.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(
        output_path, dpi=FIGURE_PARAMS["dpi"], bbox_inches="tight", facecolor="white"
    )
    plt.close()

    logger.info(f"Condition comparison chart saved to {output_path}")


def create_election_period_comparison(
    election_results: Dict[str, pd.DataFrame],
    title: str,
    output_path: str,
    top_n: int = 20,
    figsize: Tuple[int, int] = (12, 8),
) -> None:
    """
    Create a comparison chart for pre/post election periods.

    Args:
        election_results: Dictionary mapping periods to TF-IDF results
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords to show
        figsize: Figure size tuple
    """
    if not election_results:
        logger.warning(f"No election period data to create chart for: {title}")
        return

    # Prepare data for plotting
    plot_data = []
    for period, df in election_results.items():
        if not df.empty:
            top_terms = df.head(top_n)
            top_terms["period"] = period
            plot_data.append(top_terms)

    if not plot_data:
        logger.warning(f"No valid election period data for chart: {title}")
        return

    combined_df = pd.concat(plot_data, ignore_index=True)

    plt.figure(
        figsize=figsize, dpi=FIGURE_PARAMS["dpi"], facecolor=FIGURE_PARAMS["facecolor"]
    )

    # Create grouped bar chart
    sns.barplot(
        data=combined_df,
        x="term",
        y="score",
        hue="period",
        palette=ELECTION_PERIOD_COLORS,
        ax=plt.gca(),
    )

    # Customize the plot
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Term", fontsize=12, fontweight="bold")
    plt.ylabel("TF-IDF Score", fontsize=12, fontweight="bold")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Election Period", fontsize=10)
    plt.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    # Save the plot
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(
        output_path, dpi=FIGURE_PARAMS["dpi"], bbox_inches="tight", facecolor="white"
    )
    plt.close()

    logger.info(f"Election period comparison chart saved to {output_path}")


def create_cross_dimensional_heatmap(
    cross_results: Dict[str, pd.DataFrame],
    title: str,
    output_path: str,
    top_n: int = 15,
    figsize: Tuple[int, int] = (12, 8),
) -> None:
    """
    Create a heatmap showing cross-dimensional keyword differences.

    Args:
        cross_results: Dictionary with comparison results
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords to show
        figsize: Figure size tuple
    """
    # TODO: Implement heatmap creation for cross-dimensional analysis
    logger.info(f"Creating cross-dimensional heatmap: {title}")
    pass


def create_visualizations():
    """
    Create all visualizations for TF-IDF analysis results.
    """
    logger.info("Starting TF-IDF analysis visualization")

    timestamp = generate_current_datetime_str()
    output_dir = create_timestamped_output_dir(timestamp)

    # Find latest TF-IDF results
    tfidf_results_file = find_latest_tfidf_results(BASE_OUTPUT_DIR)
    if not tfidf_results_file:
        logger.error("No TF-IDF results files found. Please run training first.")
        return

    # Load TF-IDF results
    tfidf_df = load_tfidf_results(tfidf_results_file)
    if tfidf_df is None:
        logger.error("Failed to load TF-IDF results")
        return

    # Load training metadata
    metadata = load_training_metadata(BASE_OUTPUT_DIR)
    if metadata is None:
        logger.warning("No training metadata found. Proceeding without metadata.")

    # TODO: Parse TF-IDF results and create visualizations
    # This would involve:
    # 1. Parsing the TF-IDF results DataFrame
    # 2. Extracting different analysis dimensions (overall, condition, election period)
    # 3. Creating appropriate charts for each dimension
    # 4. Saving all visualizations to the output directory

    logger.info(f"TF-IDF visualization completed. Results saved in: {output_dir}")


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
