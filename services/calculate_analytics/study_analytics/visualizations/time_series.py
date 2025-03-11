import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import List, Optional, Tuple
from statsmodels.nonparametric.smoothers_lowess import lowess


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load the CSV data and parse date column.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame with loaded data
    """
    df = pd.read_csv(file_path)
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

    return df


def get_feature_columns(df: pd.DataFrame, exclude_cols: List[str]) -> List[str]:
    """
    Get all feature columns (excluding specified columns).

    Args:
        df: DataFrame with data
        exclude_cols: List of columns to exclude

    Returns:
        List of feature column names
    """
    return [col for col in df.columns if col not in exclude_cols]


def plot_time_series_by_condition(
    df: pd.DataFrame,
    feature_col: str,
    output_dir: str = "output",
    title_prefix: Optional[str] = None,
    figsize: Tuple[int, int] = (12, 8),
    dpi: int = 100,
    loess_frac: float = 0.15,  # Fraction of data used for each LOESS estimation
) -> None:
    """
    Create and save a time series plot for a specific feature column, grouped by condition.

    Args:
        df: DataFrame with data
        feature_col: Column to plot
        output_dir: Directory to save the plots
        title_prefix: Optional prefix for the plot title
        figsize: Figure size (width, height)
        dpi: DPI for the figure
        loess_frac: Fraction parameter for LOESS smoothing (determines smoothness)
    """
    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Format the title
    feature_display_name = (
        feature_col.replace("avg_prob_", "")
        .replace("avg_is_", "")
        .replace("_", " ")
        .title()
    )
    if title_prefix:
        title = f"{title_prefix} - {feature_display_name} by Condition"
    else:
        title = (
            f"Time Series of Average {feature_display_name} Probability by Condition"
        )

    plt.figure(figsize=figsize, dpi=dpi)

    # Create plot with raw data and smoothed trendlines
    sns.set_style("whitegrid")

    # Plot raw data points with lower alpha
    for condition in df["condition"].unique():
        condition_data = df[df["condition"] == condition]
        plt.scatter(
            condition_data["date"],
            condition_data[feature_col],
            alpha=0.1,  # Reduced alpha value for background dots
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
            y = condition_data[feature_col].values

            # Apply LOESS smoothing
            smoothed = lowess(y, x, frac=loess_frac, it=1, return_sorted=True)

            # Map the smoothed values back to the original dates
            # First ensure smoothed x is in the same scale as our original x
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
                y=feature_col,
                estimator="mean",
                label=condition,
                color=color_mapping[condition],
                linewidth=2.5,
            )

    # Set labels and title
    plt.title(title, fontsize=16)
    plt.xlabel("Date", fontsize=14)

    y_label = f"Average {feature_display_name} Probability"
    plt.ylabel(y_label, fontsize=14)

    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title="Condition", fontsize=12, title_fontsize=14)
    plt.tight_layout()

    # Save the plot
    filename = f"{feature_col}.png"
    output_path = os.path.join(output_dir, filename)
    plt.savefig(output_path)
    plt.close()

    print(f"Saved plot for {feature_col} to {output_path}")


def generate_all_feature_plots(
    csv_path: str,
    output_dir: str = "output",
    exclude_cols: Optional[List[str]] = None,
    title_prefix: Optional[str] = None,
) -> None:
    """
    Generate plots for all feature columns in the CSV file.

    Args:
        csv_path: Path to the CSV file
        output_dir: Directory to save the plots
        exclude_cols: List of columns to exclude from plotting
        title_prefix: Optional prefix for the plot titles
    """
    # Default columns to exclude
    if exclude_cols is None:
        exclude_cols = ["bluesky_handle", "user_did", "condition", "date", "Unnamed: 0"]

    # Load data
    print(f"Loading data from {csv_path}...")
    df = load_data(csv_path)

    # Get feature columns
    feature_cols = get_feature_columns(df, exclude_cols)
    print(f"Found {len(feature_cols)} feature columns to plot.")

    # Create plots for each feature
    for i, col in enumerate(feature_cols):
        print(f"Generating plot {i+1}/{len(feature_cols)}: {col}")
        plot_time_series_by_condition(df, col, output_dir, title_prefix)

    print(f"All plots saved to {output_dir}")


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

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate all plots
    generate_all_feature_plots(
        csv_path=str(data_path),
        output_dir=str(output_dir),
        title_prefix="Bluesky Study",
    )


if __name__ == "__main__":
    main()
