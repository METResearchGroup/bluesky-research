import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from statsmodels.nonparametric.smoothers_lowess import lowess


def plot_toxicity_time_series(
    csv_path: str,
    output_dir: str = "output",
    output_filename: str = "toxicity_by_condition.png",
    figsize=(12, 8),
    dpi=100,
    loess_frac: float = 0.15,  # Fraction parameter for LOESS smoothing
):
    """
    Create a time series plot of average toxicity probability by condition,
    matching the example in the chart.

    Args:
        csv_path: Path to the CSV file
        output_dir: Directory to save the plot
        output_filename: Filename for the saved plot
        figsize: Figure size (width, height)
        dpi: DPI for the figure
        loess_frac: Fraction parameter for LOESS smoothing (determines smoothness)
    """
    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])

    # Remove rows with NaN in the condition column
    df = df.dropna(subset=["condition"])

    # Rename condition labels
    condition_mapping = {
        "reverse_chronological": "Reverse Chronological",
        "representative_diversification": "Diversified Extremity",
        "engagement": "Engagement-Based",
    }
    df["condition"] = df["condition"].replace(condition_mapping)

    # Create the figure
    plt.figure(figsize=figsize, dpi=dpi)

    # Set the style
    sns.set_style("whitegrid")

    # Plot raw data points with lower alpha
    for condition in df["condition"].unique():
        condition_data = df[df["condition"] == condition]
        plt.scatter(
            condition_data["date"],
            condition_data["avg_prob_toxic"],
            alpha=0.1,  # Reduced alpha for better visibility
            color="gray",
            s=5,
        )

    # Define specific colors for each condition
    color_mapping = {
        "Reverse Chronological": "black",
        "Engagement-Based": "red",
        "Diversified Extremity": "green",
    }

    # Add smoothed trend lines with specified colors
    for condition in df["condition"].unique():
        if condition not in color_mapping:
            continue  # Skip conditions not in the mapping

        condition_data = df[df["condition"] == condition]

        # Sort data by date
        condition_data = condition_data.sort_values("date")

        # Convert dates to numeric format for LOESS
        date_numeric = pd.to_numeric(condition_data["date"])

        # Only apply smoothing if we have enough data points
        if len(condition_data) > 10:  # Minimum points needed for reasonable smoothing
            # Apply LOESS smoothing
            # Convert to numeric values for LOESS
            x = (date_numeric - date_numeric.min()) / (
                1e9 * 60 * 60 * 24
            )  # Convert to days since min date
            y = condition_data["avg_prob_toxic"].values

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
                label=condition,
                color=color_mapping[condition],
                linewidth=2.5,
            )
        else:
            # If not enough data points for smoothing, use the original lineplot
            sns.lineplot(
                data=condition_data,
                x="date",
                y="avg_prob_toxic",
                estimator="mean",
                label=condition,
                color=color_mapping[condition],
                linewidth=2.5,
            )

    # Set labels and title
    plt.title("Bluesky Study - Toxic by Condition", fontsize=16)
    plt.xlabel("Date", fontsize=14)
    plt.ylabel("Average Toxic Probability", fontsize=14)

    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="Condition", fontsize=12, title_fontsize=14)
    plt.tight_layout()

    # Save the plot
    output_path = os.path.join(output_dir, output_filename)
    plt.savefig(output_path)
    plt.close()

    print(f"Saved toxicity plot to {output_path}")


def main():
    """Main entry point for the script."""
    # Define paths
    current_dir = Path(__file__).parent
    repo_root = current_dir.parent.parent.parent.parent  # 4 levels up from this file

    data_path = (
        repo_root
        / "services"
        / "calculate_analytics"
        / "study_analytics"
        / "generate_reports"
        / "condition_aggregated.csv"
    )
    output_dir = current_dir / "output"

    # Create the plot
    plot_toxicity_time_series(csv_path=str(data_path), output_dir=str(output_dir))


if __name__ == "__main__":
    main()
