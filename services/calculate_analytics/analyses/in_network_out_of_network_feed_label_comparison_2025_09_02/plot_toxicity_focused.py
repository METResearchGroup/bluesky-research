#!/usr/bin/env python3
"""
Focused script to plot weekly toxicity averages by condition.
This replaces the over-engineered 745-line visualization system with a simple,
purpose-built script for toxicity analysis.
"""

import pandas as pd
import matplotlib.pyplot as plt
import os

# Define condition labels and colors
CONDITION_LABELS = {
    "reverse_chronological": "Reverse Chronological (RC)",
    "engagement": "Engagement-Based (EB)",
    "representative_diversification": "Diversified Extremity (DE)",
}

CONDITION_COLORS = {
    "engagement": "red",
    "representative_diversification": "green",
    "reverse_chronological": "black",
}


def load_data():
    """Load the weekly data files."""
    base_dir = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/in_network_out_of_network_feed_label_comparison_2025_09_02/results"

    # Use the specific file paths we found
    in_network_file = os.path.join(
        base_dir,
        "2025-09-02-04:21:25",
        "weekly_in_network_feed_content_analysis_2025-09-02-04:21:25.csv",
    )
    out_network_file = os.path.join(
        base_dir,
        "2025-09-02-04:01:15",
        "weekly_out_of_network_feed_content_analysis_2025-09-02-04:01:15.csv",
    )

    if not os.path.exists(in_network_file):
        raise FileNotFoundError(f"In-network file not found: {in_network_file}")
    if not os.path.exists(out_network_file):
        raise FileNotFoundError(f"Out-of-network file not found: {out_network_file}")

    print(f"Loading in-network data from: {in_network_file}")
    print(f"Loading out-of-network data from: {out_network_file}")

    in_network_df = pd.read_csv(in_network_file)
    out_network_df = pd.read_csv(out_network_file)

    return in_network_df, out_network_df


def create_toxicity_plot(in_network_df: pd.DataFrame, out_network_df: pd.DataFrame):
    """Create the focused toxicity visualization."""

    # Set up the plot
    plt.figure(figsize=(12, 8))
    plt.style.use("default")  # Clean, professional style

    # Plot each condition
    for condition in [
        "reverse_chronological",
        "engagement",
        "representative_diversification",
    ]:
        condition_label = CONDITION_LABELS[condition]
        color = CONDITION_COLORS[condition]

        # In-network data for this condition
        in_condition = in_network_df[in_network_df["condition"] == condition].copy()
        out_condition = out_network_df[out_network_df["condition"] == condition].copy()

        if len(in_condition) > 0 and len(out_condition) > 0:
            # Aggregate by week
            in_weekly = (
                in_condition.groupby("week")["feed_average_toxic"].mean().reset_index()
            )
            out_weekly = (
                out_condition.groupby("week")["feed_average_toxic"].mean().reset_index()
            )

            # Plot in-network (solid lines)
            plt.plot(
                in_weekly["week"],
                in_weekly["feed_average_toxic"],
                color=color,
                linewidth=2.5,
                linestyle="-",
                marker="o",
                markersize=4,
                label=f"{condition_label} - In-Network (Solid)",
            )

            # Plot out-of-network (dashed lines)
            plt.plot(
                out_weekly["week"],
                out_weekly["feed_average_toxic"],
                color=color,
                linewidth=2.5,
                linestyle="--",
                marker="s",
                markersize=4,
                label=f"{condition_label} - Out-of-Network (Dashed)",
            )

    # Customize the plot
    plt.title(
        "Weekly Feed Average Toxicity by Condition",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    plt.xlabel("Week", fontsize=12)
    plt.ylabel("Average Toxicity", fontsize=12)

    # Set y-axis scaling with fixed max at 0.2
    plt.ylim(0, 0.2)

    # Report actual data range for reference
    all_toxicity_values = []
    for df in [in_network_df, out_network_df]:
        all_toxicity_values.extend(df["feed_average_toxic"].dropna().tolist())

    if all_toxicity_values:
        min_val = min(all_toxicity_values)
        max_val = max(all_toxicity_values)
        print(f"Toxicity data range: {min_val:.4f} to {max_val:.4f}")
        print("Y-axis range: 0.0 to 0.2 (fixed)")

    # Format legend
    plt.legend(fontsize=10, loc="upper right", framealpha=0.9)

    # Format x-axis
    plt.xticks(rotation=0)
    plt.grid(True, alpha=0.3)

    plt.tight_layout()

    return plt.gcf()


def main():
    """Main function to generate the toxicity plot."""
    print("🎯 Creating focused toxicity visualization...")

    # Load data
    in_network_df, out_network_df = load_data()

    print(f"In-network data shape: {in_network_df.shape}")
    print(f"Out-of-network data shape: {out_network_df.shape}")

    # Create the plot
    fig = create_toxicity_plot(in_network_df, out_network_df)

    # Save the plot to timestamped directory like other visualizations
    base_output_dir = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/in_network_out_of_network_feed_label_comparison_2025_09_02/results"

    # Create timestamped directory
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    output_dir = os.path.join(base_output_dir, "visualizations", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "weekly_toxicity_averages_focused.png")
    fig.savefig(output_path, dpi=300, bbox_inches="tight")

    print(f"✅ Plot saved to: {output_path}")
    print(f"📁 Output directory: {output_dir}")
    print("🎯 Focused toxicity visualization complete!")


if __name__ == "__main__":
    main()
