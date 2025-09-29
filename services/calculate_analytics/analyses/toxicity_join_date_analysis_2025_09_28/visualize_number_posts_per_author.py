"""
Visualize the distribution of posts per author from aggregated toxicity data.

This script loads the aggregated author-to-average toxicity/outrage data and creates
a histogram showing the distribution of posts per author, with vertical lines
indicating the top percentiles (0.1%, 1%, 5%, 10%) of post counts.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime


def load_aggregated_data() -> pd.DataFrame:
    """
    Load the aggregated author-to-average toxicity/outrage data.

    Returns:
        DataFrame with columns: author_did, average_toxicity, average_outrage, total_labeled_posts
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Hardcoded path to the results directory
    results_dir = os.path.join(script_dir, "results", "2025-09-28_16:43:34")
    data_file = os.path.join(results_dir, "aggregated_author_toxicity_outrage.parquet")

    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Data file not found: {data_file}")

    print(f"📊 Loading data from: {data_file}")
    df = pd.read_parquet(data_file)
    print(f"✅ Loaded {len(df):,} authors")
    print(f"   - Total posts: {df['total_labeled_posts'].sum():,}")
    print(
        f"   - Posts per author range: {df['total_labeled_posts'].min()} - {df['total_labeled_posts'].max()}"
    )

    # Remove outliers using IQR method (box and whisker plot definition)
    post_counts = df["total_labeled_posts"]
    Q1 = post_counts.quantile(0.25)
    Q3 = post_counts.quantile(0.75)
    IQR = Q3 - Q1

    # Define outlier bounds (1.5 × IQR beyond quartiles)
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Filter out outliers
    original_count = len(df)
    df_filtered = df[(post_counts >= lower_bound) & (post_counts <= upper_bound)]
    removed_count = original_count - len(df_filtered)

    print(f"🔧 Removed {removed_count:,} outliers using IQR method")
    print(f"   - IQR bounds: {lower_bound:.0f} - {upper_bound:.0f} posts")
    print(f"   - Remaining authors: {len(df_filtered):,}")
    print(
        f"   - New range: {df_filtered['total_labeled_posts'].min()} - {df_filtered['total_labeled_posts'].max()}"
    )

    return df_filtered


def calculate_percentiles(df: pd.DataFrame) -> dict[str, int]:
    """
    Calculate the post count thresholds for different percentiles.

    Args:
        df: DataFrame with total_labeled_posts column

    Returns:
        Dictionary mapping percentile names to post count thresholds
    """
    post_counts = df["total_labeled_posts"]

    percentiles = {
        "top_0.1%": np.percentile(post_counts, 99.9),
        "top_1%": np.percentile(post_counts, 99.0),
        "top_5%": np.percentile(post_counts, 95.0),
        "top_10%": np.percentile(post_counts, 90.0),
    }

    print("📈 Post count percentiles:")
    for name, threshold in percentiles.items():
        count = (post_counts >= threshold).sum()
        percentage = (count / len(df)) * 100
        print(
            f"   - {name}: {threshold:.0f} posts ({count:,} authors, {percentage:.1f}%)"
        )

    return percentiles


def create_histogram(df: pd.DataFrame, percentiles: dict[str, int], output_path: str):
    """
    Create a histogram of posts per author with percentile markers.

    Args:
        df: DataFrame with total_labeled_posts column
        percentiles: Dictionary of percentile thresholds
        output_path: Path to save the visualization
    """
    # Set up the plot
    plt.figure(figsize=(12, 8))

    # Create histogram with grey bars
    post_counts = df["total_labeled_posts"]
    plt.hist(
        post_counts, bins=50, color="grey", alpha=0.7, edgecolor="black", linewidth=0.5
    )

    # Define colors for percentile lines (different hues of red, darkest to lightest)
    colors = {
        "top_0.1%": "#8B0000",  # Dark red
        "top_1%": "#DC143C",  # Crimson
        "top_5%": "#FF6347",  # Tomato
        "top_10%": "#FFA07A",  # Light salmon
    }

    # Add vertical lines for each percentile
    for percentile_name, threshold in percentiles.items():
        color = colors[percentile_name]
        plt.axvline(
            x=threshold,
            color=color,
            linestyle="--",
            linewidth=2,
            label=f"{percentile_name.replace('_', ' ').title()} ({threshold:.0f} posts)",
        )

    # Customize the plot
    plt.xlabel("Number of Posts per Author", fontsize=12, fontweight="bold")
    plt.ylabel("Number of Authors", fontsize=12, fontweight="bold")
    plt.title(
        "Distribution of Posts per Author\n(Top Percentiles Highlighted, Outliers Removed)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )

    # Add grid for better readability
    plt.grid(True, alpha=0.3, axis="y")

    # Add legend
    plt.legend(loc="upper right", framealpha=0.9)

    # Add statistics text box
    stats_text = f"""Statistics:
Total Authors: {len(df):,}
Total Posts: {df['total_labeled_posts'].sum():,}
Mean Posts/Author: {df['total_labeled_posts'].mean():.1f}
Median Posts/Author: {df['total_labeled_posts'].median():.1f}
Max Posts/Author: {df['total_labeled_posts'].max():,}"""

    plt.text(
        0.02,
        0.98,
        stats_text,
        transform=plt.gca().transAxes,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
        fontsize=10,
        fontfamily="monospace",
    )

    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"💾 Visualization saved to: {output_path}")


def save_visualization(df: pd.DataFrame, percentiles: dict[str, int]) -> str:
    """
    Save the histogram visualization to a timestamped directory.

    Args:
        df: DataFrame with total_labeled_posts column
        percentiles: Dictionary of percentile thresholds

    Returns:
        Path to the saved visualization
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create timestamp-based directory structure
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    viz_dir = os.path.join(
        script_dir, "visualizations", "number_of_posts_per_author", timestamp
    )
    os.makedirs(viz_dir, exist_ok=True)

    # Create the visualization
    output_file = os.path.join(viz_dir, "posts_per_author_distribution.png")
    create_histogram(df, percentiles, output_file)

    return output_file


def main():
    """Main function to run the visualization process."""
    print("📊 Starting Posts per Author Distribution Visualization")
    print("=" * 60)

    try:
        # Load the aggregated data
        df = load_aggregated_data()

        # Calculate percentiles
        percentiles = calculate_percentiles(df)

        # Create and save visualization
        output_path = save_visualization(df, percentiles)

        print()
        print("🎉 Visualization completed successfully!")
        print(f"📁 Results saved to: {output_path}")
        print()
        print("📈 Key Insights:")
        print(f"   - {len(df):,} authors analyzed")
        print(f"   - {df['total_labeled_posts'].sum():,} total posts")
        print(f"   - Top 0.1% of authors have ≥{percentiles['top_0.1%']:.0f} posts")
        print(f"   - Top 1% of authors have ≥{percentiles['top_1%']:.0f} posts")
        print(f"   - Top 5% of authors have ≥{percentiles['top_5%']:.0f} posts")
        print(f"   - Top 10% of authors have ≥{percentiles['top_10%']:.0f} posts")

    except Exception as e:
        print(f"❌ Error creating visualization: {e}")
        raise


if __name__ == "__main__":
    main()
