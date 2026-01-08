from pathlib import Path
import os
from typing import Dict

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.nonparametric.smoothers_lowess import lowess

from lib.constants import project_home_directory
from lib.datetime_utils import generate_current_datetime_str


def plot_baseline_time_series(
    csv_path: str,
    output_dir: str = "output",
    loess_frac: float = 0.15,  # Fraction parameter for LOESS smoothing
    figsize=(12, 8),
    dpi=100,
):
    """
    Create time series plots for each metric in the baseline dataset, comparing
    'firehose' and 'most_liked' sources.

    Args:
        csv_path: Path to the CSV file with baseline metrics
        output_dir: Directory to save the plots
        loess_frac: Fraction parameter for LOESS smoothing (determines smoothness)
        figsize: Figure size (width, height)
        dpi: DPI for the figure
    """
    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])

    # Forward fill NaN values based on previous day's value
    # First, sort by source and date
    df = df.sort_values(["source", "date"])
    # Group by source and forward fill NaNs
    df = df.groupby("source").apply(lambda x: x.ffill())

    # Get all metric columns (columns starting with 'avg_')
    metric_columns = [col for col in df.columns if col.startswith("avg_")]

    # Set the style for plots
    sns.set_style("whitegrid")

    # Define specific colors for each source
    color_mapping = {"firehose": "blue", "most_liked": "orange"}

    # Plot each metric
    for metric in metric_columns:
        metric_name = metric.replace("avg_", "").replace("_", " ").title()
        plot_metric(
            df=df,
            metric=metric,
            metric_name=metric_name,
            output_dir=output_dir,
            color_mapping=color_mapping,
            loess_frac=loess_frac,
            figsize=figsize,
            dpi=dpi,
        )

    print(f"All plots saved to {output_dir}")


def plot_metric(
    df: pd.DataFrame,
    metric: str,
    metric_name: str,
    output_dir: str,
    color_mapping: Dict[str, str],
    loess_frac: float = 0.15,
    figsize=(12, 8),
    dpi=100,
):
    """
    Plot a specific metric comparing 'firehose' and 'most_liked' sources.

    Args:
        df: DataFrame containing the data
        metric: Column name of the metric to plot
        metric_name: Display name for the metric
        output_dir: Directory to save the plot
        color_mapping: Dictionary mapping sources to colors
        loess_frac: Fraction parameter for LOESS smoothing
        figsize: Figure size (width, height)
        dpi: DPI for the figure
    """
    plt.figure(figsize=figsize, dpi=dpi)

    # Plot raw data points with lower alpha for each source
    for source in df["source"].unique():
        source_data = df[df["source"] == source]
        plt.scatter(
            source_data["date"],
            source_data[metric],
            alpha=0.1,  # Reduced alpha for better visibility
            color="gray",
            s=5,
        )

    # Add smoothed trend lines for each source
    for source in df["source"].unique():
        if source not in color_mapping:
            continue  # Skip sources not in the mapping

        source_data = df[df["source"] == source]

        # Sort data by date
        source_data = source_data.sort_values("date")

        # Convert dates to numeric format for LOESS
        date_numeric = pd.to_numeric(source_data["date"])

        # Only apply smoothing if we have enough data points
        if len(source_data) > 10:  # Minimum points needed for reasonable smoothing
            # Apply LOESS smoothing
            # Convert to numeric values for LOESS
            x = (date_numeric - date_numeric.min()) / (
                1e9 * 60 * 60 * 24
            )  # Convert to days since min date
            y = source_data[metric].values

            # Apply LOESS smoothing
            smoothed = lowess(y, x, frac=loess_frac, it=1, return_sorted=True)

            # Map the smoothed values back to the original dates
            smoothed_dates = pd.to_datetime(
                smoothed[:, 0] * 1e9 * 60 * 60 * 24 + date_numeric.min()
            )

            # Plot the smoothed line
            plt.plot(
                smoothed_dates,
                smoothed[:, 1],
                label=source.replace("_", " ").title(),
                color=color_mapping[source],
                linewidth=2.5,
            )
        else:
            # If not enough data points for smoothing, use the original lineplot
            sns.lineplot(
                data=source_data,
                x="date",
                y=metric,
                estimator="mean",
                label=source.replace("_", " ").title(),
                color=color_mapping[source],
                linewidth=2.5,
            )

    # Set labels and title
    plt.title(f"Bluesky Baseline - {metric_name}", fontsize=16)
    plt.xlabel("Date", fontsize=14)
    plt.ylabel(f"Average {metric_name}", fontsize=14)

    plt.xticks(fontsize=12, rotation=45)
    plt.yticks(fontsize=12)
    plt.legend(title="Source", fontsize=12, title_fontsize=14)
    plt.tight_layout()

    # Save the plot
    output_filename = f"{metric.lower()}.png"
    output_path = os.path.join(output_dir, output_filename)
    plt.savefig(output_path)
    plt.close()

    print(f"Saved {metric_name} plot to {output_path}")


def main():
    """Main entry point for the script."""
    # Define paths
    current_dir = Path(__file__).parent
    repo_root = Path(project_home_directory)
    # Generate timestamp for output directory
    timestamp = generate_current_datetime_str()

    data_path = os.path.join(
        repo_root,
        "services",
        "calculate_analytics",
        "study_analytics",
        "generate_reports",
        "baselines",
        "baseline_averages_per_day_2025-03-21-00:55:19.csv",
    )

    output_dir = os.path.join(current_dir, f"output/baselines_{timestamp}")

    # Create the plots
    plot_baseline_time_series(csv_path=str(data_path), output_dir=str(output_dir))


if __name__ == "__main__":
    main()
