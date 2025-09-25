"""Downstream visualization script for NER analysis.

This script automatically finds the latest NER analysis results and creates
visualizations with different top K values. Given a CSV file with top N entities,
it can create visualizations showing top K entities where K < N.

Usage:
    python downstream_visualization.py --top_k 10
    python downstream_visualization.py --top_k 20 --csv_file specific_file.csv
"""

import argparse
import os
import csv
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, List, Tuple


def find_latest_results() -> str:
    """Find the latest NER analysis results directory."""
    results_dir = Path("results")

    if not results_dir.exists():
        raise FileNotFoundError("No results directory found")

    # Find timestamped directories
    timestamp_dirs = [d for d in results_dir.iterdir() if d.is_dir()]

    if not timestamp_dirs:
        raise FileNotFoundError("No timestamped results directories found")

    # Sort by timestamp (directory name) and get the latest
    latest_dir = sorted(timestamp_dirs)[-1]

    return str(latest_dir)


def find_csv_files(results_dir: str) -> Dict[str, str]:
    """Find all CSV files in the results directory."""
    csv_files = {}
    results_path = Path(results_dir)

    # Look for CSV files in condition, election_date, and overall directories
    for subdir in ["condition", "election_date", "overall"]:
        subdir_path = results_path / subdir
        if subdir_path.exists():
            for csv_file in subdir_path.glob("*.csv"):
                # Extract the type from filename (e.g., "pre_election", "engagement", etc.)
                filename = csv_file.stem
                if "pre_election" in filename:
                    csv_files["pre_election"] = str(csv_file)
                elif "post_election" in filename:
                    csv_files["post_election"] = str(csv_file)
                elif "reverse_chronological" in filename:
                    csv_files["reverse_chronological"] = str(csv_file)
                elif "engagement" in filename:
                    csv_files["engagement"] = str(csv_file)
                elif "representative_diversification" in filename:
                    csv_files["representative_diversification"] = str(csv_file)

    return csv_files


def load_entities_from_csv(csv_file: str) -> Dict[str, int]:
    """Load entities and counts from CSV file."""
    entities = {}

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = row["entity"]
            count = int(row["count"])
            entities[entity] = count

    return entities


def create_top_k_visualization(
    entities: Dict[str, int],
    top_k: int,
    title: str,
    output_path: str,
    color: str = "steelblue",
):
    """Create horizontal bar chart for top K entities."""
    # Sort entities by count (descending) and take top K
    sorted_entities = sorted(entities.items(), key=lambda x: x[1], reverse=True)[:top_k]

    if not sorted_entities:
        print(f"No entities found for {title}")
        return

    entity_names = [name for name, _ in sorted_entities]
    entity_counts = [count for _, count in sorted_entities]

    # Create visualization
    fig, ax = plt.subplots(figsize=(12, 8))

    # Create horizontal bars
    ax.barh(range(len(entity_names)), entity_counts, color=color, alpha=0.8)

    # Set labels and title
    ax.set_yticks(range(len(entity_names)))
    ax.set_yticklabels(entity_names)
    ax.set_xlabel("Frequency")
    ax.set_title(title)
    ax.grid(True, alpha=0.3, axis="x")

    # Add count labels on bars
    for i, count in enumerate(entity_counts):
        ax.text(
            count + max(entity_counts) * 0.01,
            i,
            str(count),
            va="center",
            fontweight="bold",
        )

    # Invert y-axis so highest counts are at top
    ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created visualization: {output_path}")


def create_comparison_visualization(
    pre_entities: Dict[str, int],
    post_entities: Dict[str, int],
    top_k: int,
    output_path: str,
):
    """Create side-by-side comparison of pre/post election entities."""
    # Get top K entities for each period
    pre_top_k = sorted(pre_entities.items(), key=lambda x: x[1], reverse=True)[:top_k]
    post_top_k = sorted(post_entities.items(), key=lambda x: x[1], reverse=True)[:top_k]

    if not pre_top_k or not post_top_k:
        print("No entities found for comparison")
        return

    # Create side-by-side visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))

    # Pre-election chart
    pre_names = [name for name, _ in pre_top_k]
    pre_counts = [count for _, count in pre_top_k]

    ax1.barh(range(len(pre_names)), pre_counts, color="lightblue", alpha=0.8)
    ax1.set_yticks(range(len(pre_names)))
    ax1.set_yticklabels(pre_names)
    ax1.set_xlabel("Frequency")
    ax1.set_title(f"Top {top_k} Entities - Pre Election")
    ax1.grid(True, alpha=0.3, axis="x")
    ax1.invert_yaxis()

    # Add count labels
    for i, count in enumerate(pre_counts):
        ax1.text(
            count + max(pre_counts) * 0.01,
            i,
            str(count),
            va="center",
            fontweight="bold",
        )

    # Post-election chart
    post_names = [name for name, _ in post_top_k]
    post_counts = [count for _, count in post_top_k]

    ax2.barh(range(len(post_names)), post_counts, color="darkblue", alpha=0.8)
    ax2.set_yticks(range(len(post_names)))
    ax2.set_yticklabels(post_names)
    ax2.set_xlabel("Frequency")
    ax2.set_title(f"Top {top_k} Entities - Post Election")
    ax2.grid(True, alpha=0.3, axis="x")
    ax2.invert_yaxis()

    # Add count labels
    for i, count in enumerate(post_counts):
        ax2.text(
            count + max(post_counts) * 0.01,
            i,
            str(count),
            va="center",
            fontweight="bold",
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created comparison visualization: {output_path}")


def create_condition_comparison(
    condition_files: List[Tuple[str, str]],  # (condition_name, csv_file_path)
    top_k: int,
    output_path: str,
):
    """Create comparison visualization across conditions."""
    condition_data = {}

    # Load data for each condition
    for condition_name, csv_file in condition_files:
        if os.path.exists(csv_file):
            condition_data[condition_name] = load_entities_from_csv(csv_file)
        else:
            print(f"Warning: CSV file not found: {csv_file}")

    if not condition_data:
        print("No condition data found")
        return

    # Color palette for conditions
    condition_colors = {
        "reverse_chronological": "grey",
        "engagement": "red",
        "representative_diversification": "green",
    }

    # Create visualization
    fig, ax = plt.subplots(figsize=(16, 12))

    y_positions = []
    entity_labels = []
    bar_colors = []

    # Process each condition
    for i, (condition, entities) in enumerate(condition_data.items()):
        # Get top K entities for this condition
        top_entities = sorted(entities.items(), key=lambda x: x[1], reverse=True)[
            :top_k
        ]

        if top_entities:
            # Sort entities by frequency (descending)
            sorted_entities = sorted(top_entities, key=lambda x: x[1], reverse=True)

            for j, (entity_name, frequency) in enumerate(sorted_entities):
                y_pos = i * top_k + j  # Offset each condition by top_k positions
                y_positions.append(y_pos)
                entity_labels.append(entity_name)
                bar_colors.append(condition_colors.get(condition, "blue"))

                # Create horizontal bar
                ax.barh(
                    y_pos,
                    frequency,
                    height=0.6,
                    color=condition_colors.get(condition, "blue"),
                    alpha=0.8,
                )

    # Set y-axis labels and ticks
    ax.set_yticks(y_positions)
    ax.set_yticklabels(entity_labels)
    ax.set_xlabel("Frequency")
    ax.set_title(f"Top {top_k} Entities by Condition")
    ax.grid(True, alpha=0.3, axis="x")

    # Create legend
    legend_elements = []
    for condition, color in condition_colors.items():
        if condition in condition_data:
            legend_elements.append(
                plt.Rectangle(
                    (0, 0),
                    1,
                    1,
                    facecolor=color,
                    alpha=0.8,
                    label=condition.replace("_", " ").title(),
                )
            )

    ax.legend(handles=legend_elements, loc="upper right")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Created condition comparison: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Create downstream NER visualizations")
    parser.add_argument(
        "--csv_file",
        help="Path to specific CSV file (optional, will auto-find latest if not provided)",
    )
    parser.add_argument(
        "--top_k", type=int, required=True, help="Number of top entities to show"
    )
    parser.add_argument(
        "--output_dir", help="Output directory (default: latest results directory)"
    )
    parser.add_argument("--title", help="Custom title for visualization")
    parser.add_argument("--color", default="steelblue", help="Color for bars")
    parser.add_argument(
        "--analysis_type",
        choices=["single", "comparison", "conditions"],
        default="single",
        help="Type of analysis to perform",
    )

    args = parser.parse_args()

    print("🔍 Finding latest NER analysis results...")

    try:
        # Find latest results directory
        latest_results_dir = find_latest_results()
        print(f"✅ Found latest results: {latest_results_dir}")

        # Find CSV files in the results
        csv_files = find_csv_files(latest_results_dir)
        print(f"✅ Found CSV files: {list(csv_files.keys())}")

    except FileNotFoundError as e:
        print(f"❌ Error finding results: {e}")
        return 1

    # Set output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = latest_results_dir

    os.makedirs(output_dir, exist_ok=True)

    if args.analysis_type == "single":
        # Single file analysis
        if args.csv_file:
            csv_file = args.csv_file
        else:
            # Use pre_election as default, or first available
            csv_file = csv_files.get("pre_election") or list(csv_files.values())[0]

        if not csv_file:
            print("❌ No CSV file found for analysis")
            return 1

        print(f"📊 Analyzing: {csv_file}")

        # Load entities from CSV
        entities = load_entities_from_csv(csv_file)

        if not entities:
            print(f"No entities found in {csv_file}")
            return 1

        # Generate title if not provided
        if not args.title:
            base_name = os.path.basename(csv_file).replace(".csv", "")
            args.title = (
                f"Top {args.top_k} Entities - {base_name.replace('_', ' ').title()}"
            )

        # Create output filename
        output_filename = f"top_{args.top_k}_entities_{os.path.basename(csv_file).replace('.csv', '')}.png"
        output_path = os.path.join(output_dir, output_filename)

        # Create visualization
        create_top_k_visualization(
            entities, args.top_k, args.title, output_path, args.color
        )

    elif args.analysis_type == "comparison":
        # Pre/post election comparison
        pre_csv = csv_files.get("pre_election")
        post_csv = csv_files.get("post_election")

        if not pre_csv or not post_csv:
            print("❌ Pre/post election CSV files not found for comparison")
            return 1

        print(f"📊 Comparing pre-election: {pre_csv}")
        print(f"📊 Comparing post-election: {post_csv}")

        pre_entities = load_entities_from_csv(pre_csv)
        post_entities = load_entities_from_csv(post_csv)

        comparison_output = os.path.join(
            output_dir, f"top_{args.top_k}_pre_post_comparison.png"
        )
        create_comparison_visualization(
            pre_entities, post_entities, args.top_k, comparison_output
        )

    elif args.analysis_type == "conditions":
        # Condition comparison
        condition_files = []
        for condition in [
            "reverse_chronological",
            "engagement",
            "representative_diversification",
        ]:
            if condition in csv_files:
                condition_files.append((condition, csv_files[condition]))

        if not condition_files:
            print("❌ No condition CSV files found for comparison")
            return 1

        print(f"📊 Comparing conditions: {[c[0] for c in condition_files]}")

        condition_output = os.path.join(
            output_dir, f"top_{args.top_k}_conditions_comparison.png"
        )
        create_condition_comparison(condition_files, args.top_k, condition_output)

    print("✅ Downstream visualization completed successfully!")
    return 0


if __name__ == "__main__":
    main()
