"""Visualization demo for hashtag analysis module.

This script creates visualizations for hashtag analysis results using the
proper directory structure with separate folders for different visualization types.
"""

import os
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import Optional

# Add the parent directory to the path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import timestamp function from lib
try:
    from lib.helper import generate_current_datetime_str
except ImportError:
    # Fallback if lib is not available
    def generate_current_datetime_str():
        return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# Set style for better looking plots
plt.style.use("seaborn-v0_8")
sns.set_palette("husl")


def find_latest_analysis(base_dir: str) -> Optional[str]:
    """Find the latest analysis directory."""
    analysis_dir = os.path.join(base_dir, "results", "analysis")
    if not os.path.exists(analysis_dir):
        return None

    # Get all timestamp directories
    timestamp_dirs = [
        d
        for d in os.listdir(analysis_dir)
        if os.path.isdir(os.path.join(analysis_dir, d))
    ]

    if not timestamp_dirs:
        return None

    # Sort by timestamp and return the latest
    timestamp_dirs.sort(reverse=True)
    return os.path.join(analysis_dir, timestamp_dirs[0])


def load_analysis_data(analysis_dir: str) -> pd.DataFrame:
    """Load the main hashtag analysis data."""
    # Find the main analysis file
    for file in os.listdir(analysis_dir):
        if file.startswith("hashtag_analysis_") and file.endswith(".csv"):
            file_path = os.path.join(analysis_dir, file)
            return pd.read_csv(file_path)

    raise FileNotFoundError(f"No hashtag analysis CSV file found in {analysis_dir}")


def create_condition_visualizations(df: pd.DataFrame, output_dir: str, timestamp: str):
    """Create visualizations by condition."""
    print("📊 Creating condition visualizations...")

    condition_dir = os.path.join(output_dir, "condition")
    os.makedirs(condition_dir, exist_ok=True)

    # Top hashtags by condition
    condition_totals = df.groupby(["condition", "hashtag"])["count"].sum().reset_index()

    for condition in df["condition"].unique():
        condition_data = condition_totals[condition_totals["condition"] == condition]
        condition_data = condition_data.nlargest(10, "count")

        if condition_data.empty:
            continue

        plt.figure(figsize=(12, 8))
        bars = plt.barh(range(len(condition_data)), condition_data["count"])
        plt.yticks(range(len(condition_data)), condition_data["hashtag"])
        plt.xlabel("Count")
        plt.title(f"Top Hashtags - {condition.title()}")
        plt.gca().invert_yaxis()

        # Add value labels on bars
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(
                width + 0.1,
                bar.get_y() + bar.get_height() / 2,
                f"{int(width)}",
                ha="left",
                va="center",
            )

        plt.tight_layout()
        output_path = os.path.join(
            condition_dir, f"top_hashtags_{condition}_{timestamp}.png"
        )
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"   ✅ Created: top_hashtags_{condition}_{timestamp}.png")


def create_election_date_visualizations(
    df: pd.DataFrame, output_dir: str, timestamp: str
):
    """Create visualizations by election date."""
    print("📊 Creating election date visualizations...")

    election_dir = os.path.join(output_dir, "election_date")
    os.makedirs(election_dir, exist_ok=True)

    # Pre/post election comparison
    period_totals = (
        df.groupby(["pre_post_election", "hashtag"])["count"].sum().reset_index()
    )

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    periods = ["pre_election", "post_election"]

    for i, period in enumerate(periods):
        ax = axes[i]
        period_data = period_totals[period_totals["pre_post_election"] == period]
        period_data = period_data.nlargest(10, "count")

        if not period_data.empty:
            bars = ax.barh(range(len(period_data)), period_data["count"])
            ax.set_yticks(range(len(period_data)))
            ax.set_yticklabels(period_data["hashtag"])
            ax.set_xlabel("Count")
            ax.set_title(f'Top Hashtags - {period.replace("_", " ").title()}')
            ax.invert_yaxis()

            # Add value labels on bars
            for j, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(
                    width + 0.1,
                    bar.get_y() + bar.get_height() / 2,
                    f"{int(width)}",
                    ha="left",
                    va="center",
                )
        else:
            ax.text(
                0.5, 0.5, "No Data", ha="center", va="center", transform=ax.transAxes
            )
            ax.set_title(f'Top Hashtags - {period.replace("_", " ").title()} (No Data)')

    # Overall title
    fig.suptitle("Top Hashtags: Pre vs Post Election", fontsize=16, fontweight="bold")
    plt.tight_layout()

    output_path = os.path.join(election_dir, f"pre_post_comparison_{timestamp}.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"   ✅ Created: pre_post_comparison_{timestamp}.png")


def create_overall_visualizations(df: pd.DataFrame, output_dir: str, timestamp: str):
    """Create overall visualizations."""
    print("📊 Creating overall visualizations...")

    overall_dir = os.path.join(output_dir, "overall")
    os.makedirs(overall_dir, exist_ok=True)

    # Overall hashtag frequency distribution
    hashtag_counts = df.groupby("hashtag")["count"].sum().values

    plt.figure(figsize=(12, 8))
    plt.hist(hashtag_counts, bins=20, alpha=0.7, edgecolor="black", linewidth=0.5)
    plt.xlabel("Hashtag Frequency")
    plt.ylabel("Number of Hashtags")
    plt.title("Hashtag Frequency Distribution")
    plt.grid(True, alpha=0.3)

    # Add statistics
    mean_freq = hashtag_counts.mean()
    median_freq = pd.Series(hashtag_counts).median()
    plt.axvline(
        mean_freq,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Mean: {mean_freq:.1f}",
    )
    plt.axvline(
        median_freq,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"Median: {median_freq:.1f}",
    )
    plt.legend()

    plt.tight_layout()
    output_path = os.path.join(overall_dir, f"frequency_distribution_{timestamp}.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"   ✅ Created: frequency_distribution_{timestamp}.png")

    # Top hashtags overall
    overall_totals = df.groupby("hashtag")["count"].sum().reset_index()
    overall_totals = overall_totals.nlargest(15, "count")

    plt.figure(figsize=(12, 8))
    bars = plt.barh(range(len(overall_totals)), overall_totals["count"])
    plt.yticks(range(len(overall_totals)), overall_totals["hashtag"])
    plt.xlabel("Count")
    plt.title("Top Hashtags Overall")
    plt.gca().invert_yaxis()

    # Add value labels on bars
    for i, bar in enumerate(bars):
        width = bar.get_width()
        plt.text(
            width + 0.1,
            bar.get_y() + bar.get_height() / 2,
            f"{int(width)}",
            ha="left",
            va="center",
        )

    plt.tight_layout()
    output_path = os.path.join(overall_dir, f"top_hashtags_overall_{timestamp}.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"   ✅ Created: top_hashtags_overall_{timestamp}.png")


def create_visualization_metadata(
    df: pd.DataFrame, output_dir: str, timestamp: str, analysis_timestamp: str
):
    """Create metadata file for visualizations."""
    metadata = {
        "visualization_type": "hashtag_analysis",
        "timestamp": timestamp,
        "analysis_timestamp": analysis_timestamp,
        "election_date": "2024-11-05",
        "total_hashtags": len(df["hashtag"].unique()),
        "total_records": len(df),
        "conditions": list(df["condition"].unique()),
        "election_periods": list(df["pre_post_election"].unique()),
        "visualization_folders": ["condition", "election_date", "overall"],
        "created_at": datetime.now().isoformat(),
    }

    metadata_path = os.path.join(output_dir, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print("   ✅ Created metadata.json")


def main():
    """Run the visualization demo."""
    print("🚀 Starting Hashtag Analysis Visualization Demo")
    print("=" * 60)

    try:
        # Find the latest analysis
        base_dir = os.path.dirname(__file__)
        latest_analysis_dir = find_latest_analysis(base_dir)

        if not latest_analysis_dir:
            print("❌ No analysis data found. Please run demo.py first.")
            return 1

        print(f"📂 Using analysis data from: {os.path.basename(latest_analysis_dir)}")

        # Load analysis data
        df = load_analysis_data(latest_analysis_dir)
        print(
            f"📊 Loaded {len(df)} records with {len(df['hashtag'].unique())} unique hashtags"
        )

        # Create visualization directory structure
        results_dir = os.path.join(base_dir, "results")
        visualizations_dir = os.path.join(results_dir, "visualizations")

        timestamp = generate_current_datetime_str()
        timestamp_dir = os.path.join(visualizations_dir, timestamp)
        os.makedirs(timestamp_dir, exist_ok=True)

        # Create visualizations
        create_condition_visualizations(df, timestamp_dir, timestamp)
        create_election_date_visualizations(df, timestamp_dir, timestamp)
        create_overall_visualizations(df, timestamp_dir, timestamp)

        # Create metadata
        analysis_timestamp = os.path.basename(latest_analysis_dir)
        create_visualization_metadata(df, timestamp_dir, timestamp, analysis_timestamp)

        print("\n" + "=" * 60)
        print("🎉 Visualization demo completed successfully!")
        print("\n📋 Summary:")
        print("   ✅ Condition visualizations created")
        print("   ✅ Election date visualizations created")
        print("   ✅ Overall visualizations created")
        print("   ✅ Metadata file created")

        print(
            f"\n📁 Check the 'results/visualizations/{timestamp}' folder for generated files"
        )
        print(f"📂 Visualization directory: {timestamp_dir}")
        print(f"📊 Analysis source: {analysis_timestamp}")

    except Exception as e:
        print(f"\n❌ Visualization demo failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
