"""Visualization module for hashtag analysis results.

This module creates visualizations similar to the NER implementation,
consuming the structured data from transform.py.
"""

import os
import json
import csv
import matplotlib.pyplot as plt
from typing import Dict
from lib.helper import generate_current_datetime_str


def export_top_hashtags_to_csv(
    hashtags_dict: Dict[str, int], output_path: str, title: str
):
    """Export top hashtags to CSV file sorted by count (descending)."""
    # Sort hashtags by count (descending)
    sorted_hashtags = sorted(hashtags_dict.items(), key=lambda x: x[1], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["hashtag", "count"])
        for hashtag, count in sorted_hashtags:
            writer.writerow([hashtag, count])


def create_condition_visualizations(aggregated_data: Dict, output_dir: str):
    """Create visualizations for condition-based analysis."""

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

    condition_data = aggregated_data["condition"]

    # Export CSV for each condition
    for condition, top_hashtags_obj in condition_data.items():
        hashtags_dict = top_hashtags_obj.get_top_n(10)
        csv_path = os.path.join(output_dir, f"top_10_hashtags_{condition}.csv")
        export_top_hashtags_to_csv(
            hashtags_dict, csv_path, f"Top Hashtags - {condition_labels[condition]}"
        )

    # Create horizontal bar chart with top 10 hashtags per condition overlaid
    fig, ax = plt.subplots(figsize=(16, 12))

    conditions = list(condition_data.keys())

    # Get top 10 hashtags for each condition
    condition_top_hashtags = {}
    for condition, top_hashtags_obj in condition_data.items():
        top_hashtags = top_hashtags_obj.get_top_n(10)
        condition_top_hashtags[condition] = top_hashtags

    # Create horizontal bars for each condition
    y_positions = []
    hashtag_labels = []
    bar_colors = []
    bar_width = 0.6

    # Calculate max frequency for scaling
    max_freq = max(
        max(hashtags.values()) if hashtags else 0
        for hashtags in condition_top_hashtags.values()
    )

    # Create bars for each condition
    for i, condition in enumerate(conditions):
        hashtags = condition_top_hashtags[condition]
        if hashtags:
            # Sort hashtags by frequency (descending)
            sorted_hashtags = sorted(hashtags.items(), key=lambda x: x[1], reverse=True)

            for j, (hashtag_name, frequency) in enumerate(sorted_hashtags):
                y_pos = i * 10 + j  # Offset each condition by 10 positions
                y_positions.append(y_pos)
                hashtag_labels.append(f"#{hashtag_name}")
                bar_colors.append(condition_colors[condition])

                # Create horizontal bar
                ax.barh(
                    y_pos,
                    frequency,
                    bar_width,
                    color=condition_colors[condition],
                    alpha=0.8,
                )

                # Add hashtag name and frequency as text
                ax.text(
                    frequency + max_freq * 0.02,
                    y_pos,
                    f"#{hashtag_name} ({frequency})",
                    va="center",
                    fontsize=9,
                    fontweight="bold",
                )

    # Set y-axis labels and ticks
    ax.set_yticks(y_positions)
    ax.set_yticklabels(hashtag_labels)
    ax.set_xlabel("Frequency")
    ax.set_title("Top 10 Hashtags by Condition")
    ax.grid(True, alpha=0.3, axis="x")

    # Set x-axis limit with some padding
    ax.set_xlim(0, max_freq * 1.3)

    # Create legend
    legend_elements = []
    for condition in conditions:
        legend_elements.append(
            plt.Rectangle(
                (0, 0),
                1,
                1,
                facecolor=condition_colors[condition],
                alpha=0.8,
                label=condition_labels[condition],
            )
        )
    ax.legend(handles=legend_elements, loc="upper right")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "top_10_hashtags_by_condition.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()

    # Create a separate detailed chart showing top 5 hashtags per condition
    fig, axes = plt.subplots(1, 3, figsize=(18, 8))

    for i, condition in enumerate(conditions):
        hashtags = condition_top_hashtags[condition]
        if hashtags:
            # Get top 5 hashtags for this condition
            top_5 = dict(list(hashtags.items())[:5])

            hashtag_names = [f"#{name}" for name in top_5.keys()]
            frequencies = list(top_5.values())

            # Create horizontal bar chart for this condition
            axes[i].barh(
                range(len(hashtag_names)),
                frequencies,
                color=condition_colors[condition],
                alpha=0.8,
            )
            axes[i].set_yticks(range(len(hashtag_names)))
            axes[i].set_yticklabels(hashtag_names)
            axes[i].set_xlabel("Frequency")
            axes[i].set_title(f"Top 5 Hashtags - {condition_labels[condition]}")
            axes[i].grid(True, alpha=0.3, axis="x")
        else:
            axes[i].text(
                0.5,
                0.5,
                "No hashtags found",
                ha="center",
                va="center",
                transform=axes[i].transAxes,
            )
            axes[i].set_title(f"Top 5 Hashtags - {condition_labels[condition]}")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "top_5_hashtags_by_condition_detailed.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_election_date_visualizations(aggregated_data: Dict, output_dir: str):
    """Create visualizations for election date analysis."""

    election_data = aggregated_data["election_date"]
    ranking_comparison = aggregated_data["ranking_comparison"]

    # Get pre and post election hashtags
    pre_hashtags = election_data["pre_election"].get_top_n(10)
    post_hashtags = election_data["post_election"].get_top_n(10)

    # Export CSV files
    export_top_hashtags_to_csv(
        pre_hashtags,
        os.path.join(output_dir, "top_10_hashtags_pre_election.csv"),
        "Top Hashtags - Pre Election",
    )
    export_top_hashtags_to_csv(
        post_hashtags,
        os.path.join(output_dir, "top_10_hashtags_post_election.csv"),
        "Top Hashtags - Post Election",
    )

    # Create side-by-side comparison
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))

    # Pre-election hashtags
    if pre_hashtags:
        pre_names = [f"#{name}" for name in pre_hashtags.keys()]
        pre_freqs = list(pre_hashtags.values())

        ax1.barh(range(len(pre_names)), pre_freqs, color="lightblue", alpha=0.8)
        ax1.set_yticks(range(len(pre_names)))
        ax1.set_yticklabels(pre_names)
        ax1.set_xlabel("Frequency")
        ax1.set_title("Top 10 Hashtags - Pre Election")
        ax1.grid(True, alpha=0.3, axis="x")

        # Add frequency labels
        for i, freq in enumerate(pre_freqs):
            ax1.text(
                freq + max(pre_freqs) * 0.02,
                i,
                str(freq),
                va="center",
                fontsize=9,
                fontweight="bold",
            )

    # Post-election hashtags
    if post_hashtags:
        post_names = [f"#{name}" for name in post_hashtags.keys()]
        post_freqs = list(post_hashtags.values())

        ax2.barh(range(len(post_names)), post_freqs, color="darkblue", alpha=0.8)
        ax2.set_yticks(range(len(post_names)))
        ax2.set_yticklabels(post_names)
        ax2.set_xlabel("Frequency")
        ax2.set_title("Top 10 Hashtags - Post Election")
        ax2.grid(True, alpha=0.3, axis="x")

        # Add frequency labels
        for i, freq in enumerate(post_freqs):
            ax2.text(
                freq + max(post_freqs) * 0.02,
                i,
                str(freq),
                va="center",
                fontsize=9,
                fontweight="bold",
            )

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "top_10_hashtags_pre_post_election.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()

    # Create rank change visualization
    create_rank_change_visualization(
        ranking_comparison, output_dir, "overall_rank_change"
    )


def create_rank_change_visualization(
    ranking_comparison: Dict, output_dir: str, filename_prefix: str
):
    """Create rank movement analysis visualization (tornado chart style)."""

    # Prepare data for rank movement analysis
    pre_to_post = ranking_comparison["pre_to_post"]
    post_to_pre = ranking_comparison["post_to_pre"]

    # Collect all hashtags with their rank changes
    hashtags_data = []

    # Process pre-to-post changes
    for hashtag, data in pre_to_post.items():
        if isinstance(data["change"], int):
            hashtags_data.append(
                {
                    "hashtag": hashtag,
                    "change": data["change"],
                    "pre_rank": data["pre_rank"],
                    "post_rank": data["post_rank"],
                    "status": "improved"
                    if data["change"] > 0
                    else "declined"
                    if data["change"] < 0
                    else "unchanged",
                }
            )
        else:  # N/A - hashtag disappeared
            hashtags_data.append(
                {
                    "hashtag": hashtag,
                    "change": -20,  # Large negative for visualization
                    "pre_rank": data["pre_rank"],
                    "post_rank": 15,  # Off chart
                    "status": "dropped",
                }
            )

    # Process post-to-pre changes (for new hashtags)
    for hashtag, data in post_to_pre.items():
        if isinstance(data["change"], int) and data["change"] > 0:
            # This hashtag appeared in post-election but not in pre-election top 10
            hashtags_data.append(
                {
                    "hashtag": hashtag,
                    "change": 20,  # Large positive for visualization
                    "pre_rank": 15,  # Off chart
                    "post_rank": data["post_rank"],
                    "status": "new",
                }
            )

    # Sort by magnitude of change (descending)
    hashtags_data.sort(key=lambda x: abs(x["change"]), reverse=True)

    # Create tornado chart
    fig, ax = plt.subplots(figsize=(14, 10))

    # Separate hashtags by status
    improved_hashtags = [h for h in hashtags_data if h["status"] == "improved"]
    declined_hashtags = [h for h in hashtags_data if h["status"] == "declined"]
    dropped_hashtags = [h for h in hashtags_data if h["status"] == "dropped"]
    new_hashtags = [h for h in hashtags_data if h["status"] == "new"]
    unchanged_hashtags = [h for h in hashtags_data if h["status"] == "unchanged"]

    # Combine all hashtags for y-axis positioning
    all_hashtags = (
        improved_hashtags
        + declined_hashtags
        + dropped_hashtags
        + new_hashtags
        + unchanged_hashtags
    )
    y_positions = range(len(all_hashtags))

    # Create horizontal bars
    for i, hashtag_data in enumerate(all_hashtags):
        hashtag = hashtag_data["hashtag"]
        change = hashtag_data["change"]
        status = hashtag_data["status"]

        if status == "improved":
            # Bar extending to the right (positive) - light green
            ax.barh(i, change, color="lightgreen", alpha=0.8)
            ax.text(
                change + 0.5,
                i,
                f"+{change}",
                va="center",
                fontsize=9,
                fontweight="bold",
            )
        elif status == "declined":
            # Bar extending to the left (negative) - light red
            ax.barh(i, change, color="lightcoral", alpha=0.8)
            ax.text(
                change - 0.5,
                i,
                str(change),
                va="center",
                ha="right",
                fontsize=9,
                fontweight="bold",
            )
        elif status == "dropped":
            # Bar extending to the left (negative) - dark red
            ax.barh(i, change, color="darkred", alpha=0.8)
            ax.text(
                change - 0.5,
                i,
                "Dropped",
                va="center",
                ha="right",
                fontsize=9,
                fontweight="bold",
            )
        elif status == "new":
            # Bar extending to the right (positive) - dark green
            ax.barh(i, change, color="darkgreen", alpha=0.8)
            ax.text(
                change + 0.5,
                i,
                "New",
                va="center",
                fontsize=9,
                fontweight="bold",
            )
        else:  # unchanged
            # Bar extending to the right (zero) - grey
            ax.barh(i, change, color="grey", alpha=0.8)
            ax.text(
                change + 0.5,
                i,
                "No Change",
                va="center",
                fontsize=9,
                fontweight="bold",
            )

    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels([f"#{h['hashtag']}" for h in all_hashtags])

    # Set x-axis
    ax.set_xlabel("Δ Rank (Positive = Improved)")
    ax.set_xlim(-25, 25)

    # Add vertical line at zero
    ax.axvline(x=0, color="black", linestyle="-", alpha=0.3)

    # Add title
    ax.set_title(
        "Pre → Post Election Hashtag Rank Movement Analysis\nRank Movement Magnitude",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )

    # Add legend with more categories
    legend_elements = [
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="lightgreen", alpha=0.8, label="Improved Rank"
        ),
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="lightcoral", alpha=0.8, label="Declined Rank"
        ),
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="darkgreen", alpha=0.8, label="New (Post Only)"
        ),
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="darkred", alpha=0.8, label="Dropped (Pre Only)"
        ),
        plt.Rectangle((0, 0), 1, 1, facecolor="grey", alpha=0.8, label="No Change"),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    # Add grid
    ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, f"{filename_prefix}.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def create_overall_visualizations(aggregated_data: Dict, output_dir: str):
    """Create overall summary visualizations."""

    overall_data = aggregated_data["overall"]
    overall_hashtags = overall_data["overall"].get_top_n(20)

    # Export overall CSV
    export_top_hashtags_to_csv(
        overall_hashtags,
        os.path.join(output_dir, "top_20_hashtags_overall.csv"),
        "Top Hashtags - Overall",
    )

    # Create overall summary dashboard
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

    # Top 20 hashtags overall
    if overall_hashtags:
        hashtag_names = [f"#{name}" for name in list(overall_hashtags.keys())[:10]]
        hashtag_freqs = list(overall_hashtags.values())[:10]

        ax1.barh(range(len(hashtag_names)), hashtag_freqs, color="steelblue", alpha=0.8)
        ax1.set_yticks(range(len(hashtag_names)))
        ax1.set_yticklabels(hashtag_names)
        ax1.set_xlabel("Frequency")
        ax1.set_title("Top 10 Hashtags - Overall")
        ax1.grid(True, alpha=0.3, axis="x")

    # Condition comparison pie chart
    condition_data = aggregated_data["condition"]
    condition_totals = {}
    for condition, top_hashtags_obj in condition_data.items():
        total_count = sum(top_hashtags_obj.get_all_hashtags().values())
        condition_totals[condition] = total_count

    if condition_totals:
        labels = ["Reverse Chronological", "Engagement-Based", "Diversified Extremity"]
        sizes = [
            condition_totals.get("reverse_chronological", 0),
            condition_totals.get("engagement", 0),
            condition_totals.get("representative_diversification", 0),
        ]
        colors = ["grey", "red", "green"]

        ax2.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90)
        ax2.set_title("Hashtag Distribution by Condition")

    # Election period comparison
    election_data = aggregated_data["election_date"]
    pre_total = sum(election_data["pre_election"].get_all_hashtags().values())
    post_total = sum(election_data["post_election"].get_all_hashtags().values())

    periods = ["Pre Election", "Post Election"]
    totals = [pre_total, post_total]
    colors = ["lightblue", "darkblue"]

    ax3.bar(periods, totals, color=colors, alpha=0.8)
    ax3.set_ylabel("Total Hashtag Count")
    ax3.set_title("Hashtag Count by Election Period")
    ax3.grid(True, alpha=0.3, axis="y")

    # Frequency distribution
    if overall_hashtags:
        frequencies = list(overall_hashtags.values())
        ax4.hist(frequencies, bins=20, color="orange", alpha=0.7, edgecolor="black")
        ax4.set_xlabel("Hashtag Frequency")
        ax4.set_ylabel("Number of Hashtags")
        ax4.set_title("Hashtag Frequency Distribution")
        ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "overall_summary_dashboard.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_all_visualizations(aggregated_data: Dict) -> str:
    """Create all visualizations and return the output directory path."""

    # Create output directory structure
    timestamp = generate_current_datetime_str()
    base_output_dir = "services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags/results"
    output_dir = os.path.join(base_output_dir, timestamp)

    # Create subdirectories
    os.makedirs(os.path.join(output_dir, "condition"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "election_date"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "overall"), exist_ok=True)

    # Create visualizations
    create_condition_visualizations(
        aggregated_data, os.path.join(output_dir, "condition")
    )
    create_election_date_visualizations(
        aggregated_data, os.path.join(output_dir, "election_date")
    )
    create_overall_visualizations(aggregated_data, os.path.join(output_dir, "overall"))

    # Create metadata file
    metadata = {
        "timestamp": timestamp,
        "analysis_type": "hashtag_analysis",
        "total_conditions": len(aggregated_data["condition"]),
        "election_cutoff_date": "2024-11-05",
        "visualizations_created": [
            "top_10_hashtags_by_condition.png",
            "top_5_hashtags_by_condition_detailed.png",
            "top_10_hashtags_pre_post_election.png",
            "overall_rank_change.png",
            "overall_summary_dashboard.png",
        ],
        "csv_exports": [
            "top_10_hashtags_reverse_chronological.csv",
            "top_10_hashtags_engagement.csv",
            "top_10_hashtags_representative_diversification.csv",
            "top_10_hashtags_pre_election.csv",
            "top_10_hashtags_post_election.csv",
            "top_20_hashtags_overall.csv",
        ],
    }

    with open(os.path.join(output_dir, "visualization_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    return output_dir
