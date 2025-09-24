"""Visualization module for NER analysis results.

This module creates visualizations similar to the experimental results,
consuming the structured data from transform.py.
"""

import os
import json
import csv
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
from typing import Dict, Optional
from lib.helper import generate_current_datetime_str


def export_top_entities_to_csv(
    entities_dict: Dict[str, int], output_path: str, title: str
):
    """Export top entities to CSV file sorted by count (descending)."""
    # Sort entities by count (descending)
    sorted_entities = sorted(entities_dict.items(), key=lambda x: x[1], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["term", "count"])
        for entity, count in sorted_entities:
            writer.writerow([entity, count])


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
    for condition, top_entities_obj in condition_data.items():
        entities_dict = top_entities_obj.get_top_n(10)
        csv_path = os.path.join(output_dir, f"top_10_entities_{condition}.csv")
        export_top_entities_to_csv(
            entities_dict, csv_path, f"Top Entities - {condition_labels[condition]}"
        )

    # Create grouped bar chart
    fig, ax = plt.subplots(figsize=(14, 10))

    # Prepare data for grouped bar chart
    conditions = list(condition_data.keys())
    all_entities = set()
    for top_entities_obj in condition_data.values():
        all_entities.update(top_entities_obj.get_top_n(10).keys())

    # Get top 10 entities across all conditions
    entity_counts = Counter()
    for top_entities_obj in condition_data.values():
        entities_dict = top_entities_obj.get_top_n(10)
        for entity, count in entities_dict.items():
            entity_counts[entity] += count

    top_entities = [entity for entity, _ in entity_counts.most_common(10)]

    # Create grouped bar chart
    x = np.arange(len(top_entities))
    width = 0.25

    for i, condition in enumerate(conditions):
        entities_dict = condition_data[condition].get_top_n(10)
        counts = [entities_dict.get(entity, 0) for entity in top_entities]
        color = condition_colors[condition]
        label = condition_labels[condition]
        ax.bar(x + i * width, counts, width, label=label, color=color, alpha=0.8)

    ax.set_xlabel("Entities")
    ax.set_ylabel("Frequency")
    ax.set_title("Top 10 Entities by Condition")
    ax.set_xticks(x + width)
    ax.set_xticklabels(top_entities, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "top_10_entities_by_condition.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_election_date_visualizations(
    aggregated_data: Dict, ranking_comparison: Dict, output_dir: str
):
    """Create visualizations for election date analysis."""

    election_data = aggregated_data["election_date"]

    # Export CSV files for pre/post election entities
    pre_entities_dict = election_data["pre_election"].get_top_n(10)
    pre_csv_path = os.path.join(output_dir, "top_10_entities_pre_election.csv")
    export_top_entities_to_csv(
        pre_entities_dict, pre_csv_path, "Top Entities - Pre-Election"
    )

    post_entities_dict = election_data["post_election"].get_top_n(10)
    post_csv_path = os.path.join(output_dir, "top_10_entities_post_election.csv")
    export_top_entities_to_csv(
        post_entities_dict, post_csv_path, "Top Entities - Post-Election"
    )

    # Create side-by-side bar chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

    # Pre-election
    pre_entities = list(pre_entities_dict.keys())
    pre_counts = list(pre_entities_dict.values())

    ax1.barh(
        range(len(pre_entities)),
        pre_counts,
        color="lightblue",
        alpha=0.8,
        edgecolor="navy",
    )
    ax1.set_yticks(range(len(pre_entities)))
    ax1.set_yticklabels(pre_entities)
    ax1.set_xlabel("Frequency")
    ax1.set_title("Top 10 Entities - Pre-Election (≤ 2024-11-05)")
    ax1.grid(True, alpha=0.3)

    # Post-election
    post_entities = list(post_entities_dict.keys())
    post_counts = list(post_entities_dict.values())

    ax2.barh(
        range(len(post_entities)),
        post_counts,
        color="darkblue",
        alpha=0.8,
        edgecolor="navy",
    )
    ax2.set_yticks(range(len(post_entities)))
    ax2.set_yticklabels(post_entities)
    ax2.set_xlabel("Frequency")
    ax2.set_title("Top 10 Entities - Post-Election (> 2024-11-05)")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "top_10_entities_pre_post_election.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()

    # Create rank change visualization (tornado chart)
    create_rank_change_visualization(
        ranking_comparison, output_dir, "overall_rank_change"
    )


def create_rank_change_visualization(
    ranking_comparison: Dict, output_dir: str, filename_prefix: str
):
    """Create rank movement analysis visualization (tornado chart style)."""

    # Prepare data for rank movement analysis
    pre_to_post = ranking_comparison["pre_to_post"]

    # Collect all entities with their rank changes
    entities_data = []
    for entity, data in pre_to_post.items():
        if isinstance(data["change"], int):
            entities_data.append(
                {
                    "entity": entity,
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
        else:  # N/A - entity disappeared
            entities_data.append(
                {
                    "entity": entity,
                    "change": -20,  # Large negative for visualization
                    "pre_rank": data["pre_rank"],
                    "post_rank": 15,  # Off chart
                    "status": "dropped",
                }
            )

    # Sort by magnitude of change (descending)
    entities_data.sort(key=lambda x: abs(x["change"]), reverse=True)

    # Create tornado chart
    fig, ax = plt.subplots(figsize=(14, 10))

    # Separate improved and declined entities
    improved_entities = [e for e in entities_data if e["status"] == "improved"]
    declined_entities = [e for e in entities_data if e["status"] == "declined"]
    dropped_entities = [e for e in entities_data if e["status"] == "dropped"]

    # Combine all entities for y-axis positioning
    all_entities = improved_entities + declined_entities + dropped_entities
    y_positions = range(len(all_entities))

    # Create horizontal bars
    for i, entity_data in enumerate(all_entities):
        entity = entity_data["entity"]
        change = entity_data["change"]
        status = entity_data["status"]

        if status == "improved":
            # Bar extending to the right (positive)
            ax.barh(i, change, color="darkgreen", alpha=0.8)
            ax.text(
                change + 0.5,
                i,
                f"+{change}",
                va="center",
                fontsize=9,
                fontweight="bold",
            )
        elif status == "declined":
            # Bar extending to the left (negative)
            ax.barh(i, change, color="darkred", alpha=0.8)
            ax.text(
                change - 0.5,
                i,
                str(change),
                va="center",
                ha="right",
                fontsize=9,
                fontweight="bold",
            )
        else:  # dropped
            # Bar extending to the left (negative)
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

    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels([e["entity"] for e in all_entities])

    # Set x-axis
    ax.set_xlabel("Δ Rank (Positive = Improved)")
    ax.set_xlim(-25, 25)

    # Add vertical line at zero
    ax.axvline(x=0, color="black", linestyle="-", alpha=0.3)

    # Add title
    ax.set_title(
        "Pre → Post Election Rank Movement Analysis\nRank Movement Magnitude",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )

    # Add legend
    legend_elements = [
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="darkgreen", alpha=0.8, label="Improved Rank"
        ),
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="darkred", alpha=0.8, label="Declined Rank"
        ),
        plt.Rectangle(
            (0, 0), 1, 1, facecolor="darkred", alpha=0.8, label="Dropped (Pre Only)"
        ),
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    # Add grid
    ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, f"{filename_prefix}.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def create_overall_visualizations(
    aggregated_data: Dict, ranking_comparison: Dict, output_dir: str
):
    """Create overall summary visualizations."""

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

    # Create summary dashboard
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

    # Top entities across all conditions
    all_entities = Counter()
    condition_data = aggregated_data["condition"]
    for top_entities_obj in condition_data.values():
        entities_dict = top_entities_obj.get_top_n(10)
        for entity, count in entities_dict.items():
            all_entities[entity] += count

    top_overall = all_entities.most_common(10)
    entities, counts = zip(*top_overall)

    ax1.barh(range(len(entities)), counts, color="steelblue")
    ax1.set_yticks(range(len(entities)))
    ax1.set_yticklabels(entities)
    ax1.set_xlabel("Total Frequency")
    ax1.set_title("Top 10 Entities Overall")
    ax1.grid(True, alpha=0.3)

    # Pre vs Post comparison
    election_data = aggregated_data["election_date"]
    pre_counts = list(election_data["pre_election"].get_top_n(10).values())
    post_counts = list(election_data["post_election"].get_top_n(10).values())

    x = np.arange(len(pre_counts))
    width = 0.35

    ax2.bar(
        x - width / 2,
        pre_counts,
        width,
        label="Pre-Election",
        color="lightblue",
        alpha=0.8,
        edgecolor="navy",
    )
    ax2.bar(
        x + width / 2,
        post_counts,
        width,
        label="Post-Election",
        color="darkblue",
        alpha=0.8,
        edgecolor="navy",
    )

    ax2.set_xlabel("Rank")
    ax2.set_ylabel("Frequency")
    ax2.set_title("Pre vs Post-Election Entity Frequencies")
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"#{i+1}" for i in range(len(pre_counts))])
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Condition distribution with new colors
    condition_counts = [
        sum(top_entities_obj.get_top_n(10).values())
        for top_entities_obj in condition_data.values()
    ]
    condition_names = [condition_labels[cond] for cond in condition_data.keys()]
    condition_colors_list = [condition_colors[cond] for cond in condition_data.keys()]

    ax3.pie(
        condition_counts,
        labels=condition_names,
        colors=condition_colors_list,
        autopct="%1.1f%%",
        startangle=90,
    )
    ax3.set_title("Entity Distribution by Condition")

    # Rank change summary
    pre_to_post = ranking_comparison["pre_to_post"]
    improved = sum(
        1
        for data in pre_to_post.values()
        if isinstance(data["change"], int) and data["change"] > 0
    )
    declined = sum(
        1
        for data in pre_to_post.values()
        if isinstance(data["change"], int) and data["change"] < 0
    )
    unchanged = sum(
        1
        for data in pre_to_post.values()
        if isinstance(data["change"], int) and data["change"] == 0
    )
    disappeared = sum(1 for data in pre_to_post.values() if data["change"] == "N/A")

    categories = ["Improved", "Declined", "Unchanged", "Disappeared"]
    values = [improved, declined, unchanged, disappeared]
    colors = ["green", "red", "blue", "gray"]

    ax4.bar(categories, values, color=colors)
    ax4.set_ylabel("Number of Entities")
    ax4.set_title("Rank Change Summary (Pre → Post)")
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "overall_summary_dashboard.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_visualization_metadata(base_dir: str, timestamp: str, aggregated_data: Dict):
    """Create visualization metadata JSON file."""

    condition_data = aggregated_data["condition"]
    election_data = aggregated_data["election_date"]

    metadata = {
        "visualization_type": "named_entity_recognition",
        "creation_timestamp": timestamp,
        "election_date": "2024-11-05",
        "total_conditions": len(condition_data),
        "conditions": list(condition_data.keys()),
        "top_10_entities_by_condition": {
            condition: list(top_entities_obj.get_top_n(10).keys())
            for condition, top_entities_obj in condition_data.items()
        },
        "top_10_entities_pre_election": list(
            election_data["pre_election"].get_top_n(10).keys()
        ),
        "top_10_entities_post_election": list(
            election_data["post_election"].get_top_n(10).keys()
        ),
        "figure_parameters": {"figsize": [12, 8], "dpi": 300, "facecolor": "white"},
        "color_palette": [
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#e377c2",
            "#7f7f7f",
            "#bcbd22",
            "#17becf",
        ],
        "note": "NER analysis with spaCy en_core_web_sm model focusing on PERSON, ORG, GPE, DATE entities",
    }

    metadata_path = os.path.join(base_dir, "visualization_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Visualization metadata saved to: {metadata_path}")


def create_all_visualizations(
    aggregated_data: Dict, output_dir: Optional[str] = None
) -> str:
    """
    Create comprehensive visualizations for NER analysis results.

    Args:
        aggregated_data: Structured data from aggregate_entities_by_condition_and_pre_post
        output_dir: Base output directory (optional)

    Returns:
        Path to the created visualization directory
    """
    # Create timestamped directory structure
    timestamp = generate_current_datetime_str()
    if output_dir is None:
        output_dir = f"services/calculate_analytics/analyses/content_analysis_2025_09_22/ner/results/{timestamp}"

    base_dir = output_dir

    # Create directory structure
    directories = [
        os.path.join(base_dir, "condition"),
        os.path.join(base_dir, "election_date"),
        os.path.join(base_dir, "overall"),
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)

    # Set up plotting style
    plt.style.use("default")
    sns.set_palette("husl")

    # Create ranking comparison
    from .transform import create_ranking_comparison

    ranking_comparison = create_ranking_comparison(aggregated_data["election_date"])

    # 1. Top 10 entities by condition
    print("Creating condition-based visualizations...")
    create_condition_visualizations(
        aggregated_data, os.path.join(base_dir, "condition")
    )

    # 2. Pre/post-election visualizations
    print("Creating election date visualizations...")
    create_election_date_visualizations(
        aggregated_data, ranking_comparison, os.path.join(base_dir, "election_date")
    )

    # 3. Overall visualizations
    print("Creating overall visualizations...")
    create_overall_visualizations(
        aggregated_data, ranking_comparison, os.path.join(base_dir, "overall")
    )

    # 4. Create visualization metadata
    create_visualization_metadata(base_dir, timestamp, aggregated_data)

    print(f"All visualizations saved to: {base_dir}")
    return base_dir
