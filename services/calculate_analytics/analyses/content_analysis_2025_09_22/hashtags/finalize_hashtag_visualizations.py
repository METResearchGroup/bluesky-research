"""Finalized hashtag visualization script based on feedback.

This script creates improved visualizations with:
1. Top 10 hashtags by condition as 3 separate tables (most frequent on top)
2. Top 20 hashtags pre/post election with most frequent on top
"""

import os
import csv
import matplotlib.pyplot as plt
from typing import Dict
from lib.helper import generate_current_datetime_str


def load_hashtags_from_csv(csv_file: str) -> Dict[str, int]:
    """Load hashtags from CSV file and return as dictionary."""
    hashtags = {}
    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "hashtag" in row:
                    hashtags[row["hashtag"]] = int(row["count"])
                elif "term" in row:
                    hashtags[row["term"]] = int(row["count"])
    except FileNotFoundError:
        print(f"Warning: Could not find {csv_file}")
    return hashtags


def create_condition_tables_visualization(hashtags_results_dir: str, output_dir: str):
    """Create 3 separate tables for top 10 hashtags by condition (most frequent on top)."""

    # Condition mapping for presentation
    condition_labels = {
        "reverse_chronological": "Reverse Chronological",
        "engagement": "Engagement-Based",
        "representative_diversification": "Diversified Extremity",
    }

    # Color palette
    condition_colors = {
        "reverse_chronological": "grey",
        "engagement": "red",
        "representative_diversification": "green",
    }

    # Load data for each condition
    condition_data = {}
    csv_files = [
        "top_30_hashtags_reverse_chronological.csv",
        "top_30_hashtags_engagement.csv",
        "top_30_hashtags_representative_diversification.csv",
    ]

    conditions = [
        "reverse_chronological",
        "engagement",
        "representative_diversification",
    ]

    for condition, csv_file in zip(conditions, csv_files):
        csv_path = os.path.join(hashtags_results_dir, "condition", csv_file)
        hashtags = load_hashtags_from_csv(csv_path)

        # Get top 10 and sort by frequency (most frequent first)
        top_10 = dict(
            list(sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:10])
        )
        condition_data[condition] = top_10

    # Create 3 separate subplots (tables)
    fig, axes = plt.subplots(1, 3, figsize=(20, 10))

    for i, condition in enumerate(conditions):
        hashtags = condition_data[condition]

        if hashtags:
            # Prepare data - hashtags are already sorted by frequency (descending)
            hashtag_names = [f"#{name}" for name in hashtags.keys()]
            frequencies = list(hashtags.values())

            # Create horizontal bar chart (most frequent at top)
            y_positions = range(len(hashtag_names))
            axes[i].barh(
                y_positions, frequencies, color=condition_colors[condition], alpha=0.8
            )

            # Set y-axis labels (hashtag names)
            axes[i].set_yticks(y_positions)
            axes[i].set_yticklabels(hashtag_names, fontsize=10)

            # Invert y-axis so most frequent appears at top
            axes[i].invert_yaxis()

            axes[i].set_xlabel("Frequency", fontsize=12)
            axes[i].set_xlim(0, 30000)  # Set X-axis limit to 30,000
            axes[i].set_title(
                f"Top 10 Hashtags - {condition_labels[condition]}",
                fontsize=14,
                fontweight="bold",
            )
            axes[i].grid(True, alpha=0.3, axis="x")

            # Add frequency labels on bars
            for j, freq in enumerate(frequencies):
                axes[i].text(
                    freq + max(frequencies) * 0.01,
                    j,
                    str(freq),
                    va="center",
                    fontsize=9,
                    fontweight="bold",
                )
        else:
            axes[i].text(
                0.5,
                0.5,
                "No hashtags found",
                ha="center",
                va="center",
                transform=axes[i].transAxes,
                fontsize=12,
            )
            axes[i].set_title(
                f"Top 10 Hashtags - {condition_labels[condition]}",
                fontsize=14,
                fontweight="bold",
            )

    plt.tight_layout()

    # Save the visualization
    output_path = os.path.join(output_dir, "finalized_top_10_hashtags_by_condition.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created finalized condition tables visualization: {output_path}")


def create_election_period_visualization(hashtags_results_dir: str, output_dir: str):
    """Create pre/post election visualization with top 20 hashtags (most frequent on top)."""

    # Load pre and post election data
    pre_csv = os.path.join(
        hashtags_results_dir, "election_date", "top_30_hashtags_pre_election.csv"
    )
    post_csv = os.path.join(
        hashtags_results_dir, "election_date", "top_30_hashtags_post_election.csv"
    )

    pre_hashtags = load_hashtags_from_csv(pre_csv)
    post_hashtags = load_hashtags_from_csv(post_csv)

    # Get top 20 for each period (already sorted by frequency)
    pre_top_20 = dict(list(pre_hashtags.items())[:20])
    post_top_20 = dict(list(post_hashtags.items())[:20])

    # Create side-by-side comparison
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))

    # Pre-election visualization
    if pre_top_20:
        pre_names = [f"#{name}" for name in pre_top_20.keys()]
        pre_freqs = list(pre_top_20.values())

        y_positions = range(len(pre_names))
        ax1.barh(y_positions, pre_freqs, color="lightblue", alpha=0.8, edgecolor="navy")
        ax1.set_yticks(y_positions)
        ax1.set_yticklabels(pre_names, fontsize=10)
        ax1.invert_yaxis()  # Most frequent at top
        ax1.set_xlabel("Frequency", fontsize=12)
        ax1.set_xlim(0, 50000)  # Set X-axis limit to 50,000
        ax1.set_title(
            "Top 20 Hashtags - Pre-Election (≤ 2024-11-05)",
            fontsize=14,
            fontweight="bold",
        )
        ax1.grid(True, alpha=0.3, axis="x")

        # Add frequency labels
        for i, freq in enumerate(pre_freqs):
            ax1.text(
                freq + max(pre_freqs) * 0.01,
                i,
                str(freq),
                va="center",
                fontsize=9,
                fontweight="bold",
            )

    # Post-election visualization
    if post_top_20:
        post_names = [f"#{name}" for name in post_top_20.keys()]
        post_freqs = list(post_top_20.values())

        y_positions = range(len(post_names))
        ax2.barh(y_positions, post_freqs, color="darkblue", alpha=0.8, edgecolor="navy")
        ax2.set_yticks(y_positions)
        ax2.set_yticklabels(post_names, fontsize=10)
        ax2.invert_yaxis()  # Most frequent at top
        ax2.set_xlabel("Frequency", fontsize=12)
        ax2.set_xlim(0, 50000)  # Set X-axis limit to 50,000
        ax2.set_title(
            "Top 20 Hashtags - Post-Election (> 2024-11-05)",
            fontsize=14,
            fontweight="bold",
        )
        ax2.grid(True, alpha=0.3, axis="x")

        # Add frequency labels
        for i, freq in enumerate(post_freqs):
            ax2.text(
                freq + max(post_freqs) * 0.01,
                i,
                str(freq),
                va="center",
                fontsize=9,
                fontweight="bold",
            )

    plt.tight_layout()

    # Save the visualization
    output_path = os.path.join(
        output_dir, "finalized_top_20_hashtags_pre_post_election.png"
    )
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created finalized election period visualization: {output_path}")


def main():
    """Main function to create finalized hashtag visualizations."""

    # Set up paths
    base_dir = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags"
    hashtags_results_dir = os.path.join(base_dir, "results", "2025-09-25-03:54:15")

    # Create output directory
    timestamp = generate_current_datetime_str()
    output_dir = os.path.join(base_dir, "finalized_results", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Creating finalized hashtag visualizations in: {output_dir}")

    # Create the visualizations
    try:
        create_condition_tables_visualization(hashtags_results_dir, output_dir)
        create_election_period_visualization(hashtags_results_dir, output_dir)
        print("✅ All finalized hashtag visualizations created successfully!")

    except Exception as e:
        print(f"❌ Error creating visualizations: {e}")
        raise


if __name__ == "__main__":
    main()
