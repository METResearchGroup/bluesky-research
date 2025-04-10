from pathlib import Path
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.nonparametric.smoothers_lowess import lowess

from lib.constants import project_home_directory
from lib.helper import generate_current_datetime_str


def plot_firehose_label_baseline_time_series(
    csv_path: str,
    output_dir: str = "output/firehose_label_baselines",
    loess_frac: float = 0.15,  # Fraction parameter for LOESS smoothing
    figsize=(12, 8),
    dpi=100,
):
    """
    Create time series plots for each metric in the firehose label baseline dataset.

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
    df = df.sort_values("date")
    df = df.ffill()

    # Get all metric columns (columns starting with 'prop_')
    metric_columns = [col for col in df.columns if col.startswith("prop_")]

    # Set the style for plots
    sns.set_style("whitegrid")

    # Plot each metric
    for metric in metric_columns:
        metric_name = metric.replace("prop_", "").replace("_", " ").title()
        plt.figure(figsize=figsize, dpi=dpi)

        # Plot raw data points with lower alpha
        plt.scatter(
            df["date"],
            df[metric],
            alpha=0.1,  # Reduced alpha for better visibility
            color="gray",
            s=5,
        )

        # Apply LOESS smoothing
        date_numeric = pd.to_numeric(df["date"])
        if len(df) > 10:  # Minimum points needed for reasonable smoothing
            x = (date_numeric - date_numeric.min()) / (
                1e9 * 60 * 60 * 24
            )  # Convert to days since min date
            y = df[metric].values
            smoothed = lowess(y, x, frac=loess_frac, it=1, return_sorted=True)
            smoothed_dates = pd.to_datetime(
                smoothed[:, 0] * 1e9 * 60 * 60 * 24 + date_numeric.min()
            )
            plt.plot(
                smoothed_dates,
                smoothed[:, 1],
                label="Firehose baseline proportion",
                color="blue",
                linewidth=2.5,
            )

        # Set labels and title
        plt.title(f"Firehose Label Baseline - {metric_name}", fontsize=16)
        plt.xlabel("Date", fontsize=14)
        plt.ylabel(f"Proportion of {metric_name}", fontsize=14)

        plt.xticks(fontsize=12, rotation=45)
        plt.yticks(fontsize=12)
        plt.legend(fontsize=12)
        plt.tight_layout()

        # Save the plot
        output_filename = f"{metric}.png"
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
        "firehose_label_baseline_proportions_per_day_2025-04-10-06:48:48.csv",
    )

    output_dir = os.path.join(
        current_dir, f"output/firehose_label_baselines_{timestamp}"
    )

    # Create the plots
    plot_firehose_label_baseline_time_series(
        csv_path=str(data_path), output_dir=str(output_dir)
    )


if __name__ == "__main__":
    main()
