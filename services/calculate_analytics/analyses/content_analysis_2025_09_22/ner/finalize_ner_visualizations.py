"""Finalized NER visualization script based on feedback.

This script creates improved visualizations with:
1. Top 10 entities by condition as 3 separate tables (most frequent on top)
2. Top 20 entities pre/post election with most frequent on top
"""

import os
import csv
import matplotlib.pyplot as plt
from typing import Dict
from lib.datetime_utils import generate_current_datetime_str


def load_entities_from_csv(csv_file: str) -> Dict[str, int]:
    """Load entities from CSV file and return as dictionary."""
    entities = {}
    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "term" in row:
                    entities[row["term"]] = int(row["count"])
                elif "entity" in row:
                    entities[row["entity"]] = int(row["count"])
    except FileNotFoundError:
        print(f"Warning: Could not find {csv_file}")
    return entities


def create_condition_tables_visualization(ner_results_dir: str, output_dir: str):
    """Create stacked chart with all 3 conditions (most frequent on top)."""

    # Color palette
    condition_colors = {
        "reverse_chronological": "grey",
        "engagement": "red",
        "representative_diversification": "green",
    }

    # Load data for each condition
    condition_data = {}
    csv_files = [
        "top_30_entities_reverse_chronological.csv",
        "top_30_entities_engagement.csv",
        "top_30_entities_representative_diversification.csv",
    ]

    conditions = [
        "reverse_chronological",
        "engagement",
        "representative_diversification",
    ]

    for condition, csv_file in zip(conditions, csv_files):
        csv_path = os.path.join(ner_results_dir, "condition", csv_file)
        entities = load_entities_from_csv(csv_path)

        # Get top 10 and sort by frequency (most frequent first)
        top_10 = dict(
            list(sorted(entities.items(), key=lambda x: x[1], reverse=True)[:10])
        )
        condition_data[condition] = top_10

    # Create single stacked chart
    fig, ax = plt.subplots(figsize=(16, 12))

    # Prepare data for stacking
    y_positions = []
    entity_labels = []
    frequencies = []
    bar_colors = []

    # No spacing between conditions - they should be directly adjacent
    current_y = 0
    for i, condition in enumerate(conditions):
        entities = condition_data[condition]

        if entities:
            # Add entities for this condition (no condition label bar)
            entity_names = list(entities.keys())
            entity_freqs = list(entities.values())

            for j, (name, freq) in enumerate(zip(entity_names, entity_freqs)):
                entity_labels.append(name)
                frequencies.append(freq)
                bar_colors.append(condition_colors[condition])
                y_positions.append(current_y)
                current_y += 2  # Increased spacing between bars (was 1, now 2)

    # Create horizontal bar chart
    ax.barh(y_positions, frequencies, color=bar_colors, alpha=0.8)

    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels(entity_labels, fontsize=15)

    # Invert y-axis so most frequent appears at top
    ax.invert_yaxis()

    # Set labels and limits
    ax.set_xlabel("Frequency", fontsize=12)
    ax.set_xlim(0, 500000)  # Set X-axis limit to 500,000
    ax.set_title("Top 10 Entities by Condition", fontsize=16, fontweight="bold")
    ax.grid(True, alpha=0.3, axis="x")

    # Add frequency labels on bars - properly aligned
    for i, (freq, color) in enumerate(zip(frequencies, bar_colors)):
        if color != "white" and freq > 0:  # Only add labels to actual data bars
            ax.text(
                freq + max([f for f in frequencies if f > 0]) * 0.01,
                y_positions[i],
                str(freq),
                va="center",
                ha="left",
                fontsize=10,
                fontweight="bold",
            )

    # Add legend for conditions
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="grey", alpha=0.8, label="Reverse Chronological"),
        Patch(facecolor="red", alpha=0.8, label="Engagement-Based"),
        Patch(facecolor="green", alpha=0.8, label="Diversified Extremity"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=12)

    plt.tight_layout()

    # Save the visualization
    output_path = os.path.join(output_dir, "finalized_top_10_entities_by_condition.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created finalized condition tables visualization: {output_path}")


def create_election_period_visualization(ner_results_dir: str, output_dir: str):
    """Create pre/post election visualization with top 20 entities (most frequent on top)."""

    # Load pre and post election data
    pre_csv = os.path.join(
        ner_results_dir, "election_date", "top_30_entities_pre_election.csv"
    )
    post_csv = os.path.join(
        ner_results_dir, "election_date", "top_30_entities_post_election.csv"
    )

    pre_entities = load_entities_from_csv(pre_csv)
    post_entities = load_entities_from_csv(post_csv)

    # Get top 20 for each period (already sorted by frequency)
    pre_top_20 = dict(list(pre_entities.items())[:20])
    post_top_20 = dict(list(post_entities.items())[:20])

    # Create side-by-side comparison
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))

    # Pre-election visualization
    if pre_top_20:
        pre_names = list(pre_top_20.keys())
        pre_freqs = list(pre_top_20.values())

        y_positions = range(len(pre_names))
        ax1.barh(y_positions, pre_freqs, color="lightblue", alpha=0.8, edgecolor="navy")
        ax1.set_yticks(y_positions)
        ax1.set_yticklabels(pre_names, fontsize=15)
        ax1.invert_yaxis()  # Most frequent at top
        ax1.set_xlabel("Frequency", fontsize=12)
        ax1.set_xlim(0, 500000)  # Set X-axis limit to 500,000
        ax1.set_title(
            "Top 20 Entities - Pre-Election (≤ 2024-11-05)",
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
        post_names = list(post_top_20.keys())
        post_freqs = list(post_top_20.values())

        y_positions = range(len(post_names))
        ax2.barh(y_positions, post_freqs, color="darkblue", alpha=0.8, edgecolor="navy")
        ax2.set_yticks(y_positions)
        ax2.set_yticklabels(post_names, fontsize=15)
        ax2.invert_yaxis()  # Most frequent at top
        ax2.set_xlabel("Frequency", fontsize=12)
        ax2.set_xlim(0, 500000)  # Set X-axis limit to 500,000
        ax2.set_title(
            "Top 20 Entities - Post-Election (> 2024-11-05)",
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
        output_dir, "finalized_top_20_entities_pre_post_election.png"
    )
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created finalized election period visualization: {output_path}")


def main():
    """Main function to create finalized NER visualizations."""

    # Set up paths
    base_dir = "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/content_analysis_2025_09_22/ner"
    ner_results_dir = os.path.join(base_dir, "results", "2025-09-25-18:39:34")

    # Create output directory
    timestamp = generate_current_datetime_str()
    output_dir = os.path.join(base_dir, "finalized_results", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Creating finalized NER visualizations in: {output_dir}")

    # Create the visualizations
    try:
        create_condition_tables_visualization(ner_results_dir, output_dir)
        create_election_period_visualization(ner_results_dir, output_dir)
        print("✅ All finalized NER visualizations created successfully!")

    except Exception as e:
        print(f"❌ Error creating visualizations: {e}")
        raise


if __name__ == "__main__":
    main()
