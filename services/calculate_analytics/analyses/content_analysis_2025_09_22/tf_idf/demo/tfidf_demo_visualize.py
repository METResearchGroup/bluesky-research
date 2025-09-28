"""
TF-IDF Demo Visualization Script

This script creates visualizations from the TF-IDF training results,
demonstrating various chart types for keyword analysis.

The script creates:
1. Horizontal bar chart showing top keywords by TF-IDF score
2. Comparison chart showing top keywords across sources
3. Comparison chart for pre/post periods
4. Heatmap showing cross-dimensional keyword differences

Usage:
    python tfidf_demo_visualize.py
"""

import json
import os
import pickle
from typing import Dict, Optional, Tuple

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set style for better-looking plots
plt.style.use("default")
sns.set_palette("husl")

# Configuration
OUTPUT_DIR = "demo_results"
VISUALIZATIONS_BASE_DIR = os.path.join(OUTPUT_DIR, "visualizations")

# Color schemes
CONDITION_COLORS = {
    "control": "#1f77b4",  # Blue
    "treatment": "#ff7f0e",  # Orange
    "engagement": "#2ca02c",  # Green
}

SOURCE_COLORS = {
    "A": "#1f77b4",  # Blue
    "B": "#ff7f0e",  # Orange
    "C": "#2ca02c",  # Green
}

PERIOD_COLORS = {
    "pre": "#d62728",  # Red
    "post": "#9467bd",  # Purple
}

# Chart styling
CHART_STYLE = {"figsize": (12, 8), "dpi": 300, "facecolor": "white"}


def find_latest_training_dir(base_dir: str) -> Optional[str]:
    """
    Find the latest training directory.

    Args:
        base_dir: Base directory to search in

    Returns:
        Path to the latest training directory, or None if not found
    """
    training_base = os.path.join(base_dir, "training")
    if not os.path.exists(training_base):
        print(f"Training directory not found: {training_base}")
        return None

    # Get all timestamped directories
    timestamp_dirs = [
        d
        for d in os.listdir(training_base)
        if os.path.isdir(os.path.join(training_base, d))
    ]

    if not timestamp_dirs:
        print(f"No timestamped training directories found in: {training_base}")
        return None

    # Sort by timestamp and return the newest
    timestamp_dirs.sort(reverse=True)
    latest_dir = os.path.join(training_base, timestamp_dirs[0])
    print(f"Found latest training directory: {latest_dir}")
    return latest_dir


def load_results() -> Tuple[pd.DataFrame, Dict, Dict, str]:
    """
    Load the training results.

    Returns:
        Tuple of (dataset, group_models, stratified_results, training_dir_relative_path)
    """
    print("Loading training results...")

    # Find the latest training directory
    latest_training_dir = find_latest_training_dir(str(OUTPUT_DIR))
    if not latest_training_dir:
        raise FileNotFoundError(
            "No training results found. Please run tfidf_demo_train.py first."
        )

    # Calculate relative path from results/ directory
    training_dir_relative_path = os.path.relpath(latest_training_dir, OUTPUT_DIR)

    # Load dataset
    dataset = pd.read_csv(os.path.join(latest_training_dir, "dataset.csv"))

    # Load group models
    with open(os.path.join(latest_training_dir, "group_models.pkl"), "rb") as f:
        group_models = pickle.load(f)

    # Load stratified results from organized directories
    stratified_results = {}

    # Load overall results
    overall_file = os.path.join(
        latest_training_dir, "overall", "top_keywords_overall.csv"
    )
    if os.path.exists(overall_file):
        stratified_results["overall"] = pd.read_csv(overall_file)

    # Load condition results
    condition_dir = os.path.join(latest_training_dir, "condition")
    if os.path.exists(condition_dir):
        for file in os.listdir(condition_dir):
            if file.startswith("top_keywords_") and file.endswith(".csv"):
                condition_name = file.replace("top_keywords_", "").replace(".csv", "")
                stratified_results[f"condition_{condition_name}"] = pd.read_csv(
                    os.path.join(condition_dir, file)
                )

    # Load period results
    period_dir = os.path.join(latest_training_dir, "period")
    if os.path.exists(period_dir):
        for file in os.listdir(period_dir):
            if file.startswith("top_keywords_") and file.endswith(".csv"):
                period_name = file.replace("top_keywords_", "").replace(".csv", "")
                stratified_results[f"period_{period_name}"] = pd.read_csv(
                    os.path.join(period_dir, file)
                )

    print(f"Loaded dataset with {len(dataset)} posts")
    print(f"Loaded {len(stratified_results)} analysis types")

    return dataset, group_models, stratified_results, training_dir_relative_path


def create_top_keywords_chart(
    results_df: pd.DataFrame, title: str, output_path: str, top_n: int = 15
) -> None:
    """
    Create a horizontal bar chart showing top keywords by TF-IDF score.

    Args:
        results_df: DataFrame with 'term' and 'score' columns
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords to show
    """
    if results_df.empty:
        print(f"Warning: No data for {title}")
        return

    # Get top N terms
    top_terms = results_df.head(top_n)

    # Create figure
    fig, ax = plt.subplots(figsize=CHART_STYLE["figsize"], dpi=CHART_STYLE["dpi"])

    # Create horizontal bar chart
    bars = ax.barh(
        range(len(top_terms)),
        top_terms["score"],
        color=plt.cm.viridis(np.linspace(0, 1, len(top_terms))),
    )

    # Customize the plot
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("TF-IDF Score", fontsize=12, fontweight="bold")
    ax.set_ylabel("Term", fontsize=12, fontweight="bold")

    # Set y-axis labels
    ax.set_yticks(range(len(top_terms)))
    ax.set_yticklabels(top_terms["term"])

    # Add value labels on bars
    for i, (bar, score) in enumerate(zip(bars, top_terms["score"])):
        ax.text(
            bar.get_width() + 0.001,
            bar.get_y() + bar.get_height() / 2,
            f"{score:.3f}",
            ha="left",
            va="center",
            fontsize=9,
        )

    # Styling
    ax.grid(True, alpha=0.3, axis="x")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    # Save the plot
    plt.savefig(
        output_path,
        dpi=CHART_STYLE["dpi"],
        bbox_inches="tight",
        facecolor=CHART_STYLE["facecolor"],
    )
    plt.close()

    print(f"Saved: {output_path}")


def create_condition_comparison_chart(
    stratified_results: Dict, title: str, output_path: str, top_n: int = 10
) -> None:
    """
    Create a comparison chart showing top keywords across conditions.

    Args:
        stratified_results: Dictionary with analysis results
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords per condition to show
    """
    # Prepare data for plotting
    plot_data = []
    conditions = ["control", "treatment", "engagement"]

    for condition in conditions:
        condition_key = f"condition_{condition}"
        if condition_key in stratified_results:
            condition_df = stratified_results[condition_key].head(top_n).copy()
            condition_df["condition"] = condition
            plot_data.append(condition_df)

    if not plot_data:
        print(f"Warning: No condition data for {title}")
        return

    combined_df = pd.concat(plot_data, ignore_index=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 10), dpi=CHART_STYLE["dpi"])

    # Create grouped bar chart
    sns.barplot(
        data=combined_df,
        x="term",
        y="score",
        hue="condition",
        palette=CONDITION_COLORS,
        ax=ax,
    )

    # Customize the plot
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Term", fontsize=12, fontweight="bold")
    ax.set_ylabel("TF-IDF Score", fontsize=12, fontweight="bold")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(title="Condition", fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")

    # Styling
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    # Save the plot
    plt.savefig(
        output_path,
        dpi=CHART_STYLE["dpi"],
        bbox_inches="tight",
        facecolor=CHART_STYLE["facecolor"],
    )
    plt.close()

    print(f"Saved: {output_path}")


def create_period_comparison_chart(
    stratified_results: Dict, title: str, output_path: str, top_n: int = 15
) -> None:
    """
    Create a comparison chart for pre/post periods.

    Args:
        stratified_results: Dictionary with analysis results
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords to show
    """
    # Prepare data for plotting
    plot_data = []
    periods = ["pre", "post"]

    for period in periods:
        period_key = f"period_{period}"
        if period_key in stratified_results:
            period_df = stratified_results[period_key].head(top_n).copy()
            period_df["period"] = period
            plot_data.append(period_df)

    if not plot_data:
        print(f"Warning: No period data for {title}")
        return

    combined_df = pd.concat(plot_data, ignore_index=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8), dpi=CHART_STYLE["dpi"])

    # Create grouped bar chart
    sns.barplot(
        data=combined_df,
        x="term",
        y="score",
        hue="period",
        palette=PERIOD_COLORS,
        ax=ax,
    )

    # Customize the plot
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Term", fontsize=12, fontweight="bold")
    ax.set_ylabel("TF-IDF Score", fontsize=12, fontweight="bold")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(title="Period", fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")

    # Styling
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    # Save the plot
    plt.savefig(
        output_path,
        dpi=CHART_STYLE["dpi"],
        bbox_inches="tight",
        facecolor=CHART_STYLE["facecolor"],
    )
    plt.close()

    print(f"Saved: {output_path}")


def create_cross_dimensional_heatmap(
    stratified_results: Dict, title: str, output_path: str, top_n: int = 10
) -> None:
    """
    Create a heatmap showing cross-dimensional keyword differences.

    Args:
        stratified_results: Dictionary with analysis results
        title: Chart title
        output_path: Path to save the chart
        top_n: Number of top keywords to show
    """
    # Prepare data for heatmap
    sources = ["A", "B", "C"]
    periods = ["pre", "post"]

    # Create a matrix of TF-IDF scores for each source-period combination
    heatmap_data = []

    for source in sources:
        row = []
        for period in periods:
            group_key = f"group_{source}_{period}"
            if group_key in stratified_results:
                # Get top N terms and their scores
                group_df = stratified_results[group_key].head(top_n)
                # Calculate mean score for this group
                mean_score = group_df["score"].mean()
                row.append(mean_score)
            else:
                row.append(0)
        heatmap_data.append(row)

    heatmap_df = pd.DataFrame(
        heatmap_data,
        index=[f"Source {s}" for s in sources],
        columns=[f"{p.capitalize()}" for p in periods],
    )

    # Create figure
    fig, ax = plt.subplots(figsize=(8, 6), dpi=CHART_STYLE["dpi"])

    # Create heatmap
    sns.heatmap(
        heatmap_df,
        annot=True,
        fmt=".3f",
        cmap="YlOrRd",
        ax=ax,
        cbar_kws={"label": "Mean TF-IDF Score"},
    )

    # Customize the plot
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Time Period", fontsize=12, fontweight="bold")
    ax.set_ylabel("Source", fontsize=12, fontweight="bold")

    plt.tight_layout()

    # Save the plot
    plt.savefig(
        output_path,
        dpi=CHART_STYLE["dpi"],
        bbox_inches="tight",
        facecolor=CHART_STYLE["facecolor"],
    )
    plt.close()

    print(f"Saved: {output_path}")


def create_rank_movement_analysis(
    stratified_results: Dict,
    title: str,
    output_path: str,
    top_k: int = 20,
    threshold: int = 2,
) -> None:
    """
    Create a rank movement analysis with diverging bars showing magnitude of movement.

    Uses refined color coding:
    - Light Green: Improved Rank (existing terms that moved up)
    - Dark Green: New (Post Only) - effectively "infinitely improved"
    - Light Red: Declined Rank (existing terms that moved down)
    - Dark Red: Dropped (Pre Only) - effectively "infinitely declined"
    - Gray: No Change

    Args:
        stratified_results: Dictionary with analysis results
        title: Chart title
        output_path: Path to save the chart
        top_k: Number of top terms to consider (K)
        threshold: Threshold for "No Change" category (τ)
    """
    # Get pre and post results
    pre_key = "period_pre"
    post_key = "period_post"

    if pre_key not in stratified_results or post_key not in stratified_results:
        print(f"Warning: Missing pre/post data for {title}")
        return

    pre_df = stratified_results[pre_key].copy()
    post_df = stratified_results[post_key].copy()

    # Get top-K terms from each period
    pre_top_k = pre_df.head(top_k)
    post_top_k = post_df.head(top_k)

    # Take union of top-K terms from both periods
    pre_terms_set = set(pre_top_k["term"].tolist())
    post_terms_set = set(post_top_k["term"].tolist())
    candidate_terms = pre_terms_set.union(post_terms_set)

    if len(candidate_terms) == 0:
        print(f"Warning: No candidate terms found for {title}")
        return

    # Compute ranks for each candidate term
    rank_data = []
    for term in candidate_terms:
        # Get ranks (1-based)
        pre_rank = top_k + 1  # Default to beyond top-K
        post_rank = top_k + 1

        if term in pre_df["term"].values:
            pre_rank = pre_df[pre_df["term"] == term].index[0] + 1
        if term in post_df["term"].values:
            post_rank = post_df[post_df["term"] == term].index[0] + 1

        # Calculate rank change (positive = improved)
        delta_rank = pre_rank - post_rank

        # Calculate TF-IDF change for context
        pre_tfidf = (
            pre_df[pre_df["term"] == term]["score"].iloc[0]
            if term in pre_df["term"].values
            else 0
        )
        post_tfidf = (
            post_df[post_df["term"] == term]["score"].iloc[0]
            if term in post_df["term"].values
            else 0
        )
        delta_tfidf = post_tfidf - pre_tfidf

        # Categorize term with refined color coding
        if pre_rank == top_k + 1 and post_rank <= top_k:
            category = "New"
            color = "#1b5e20"  # Dark Green - "infinitely improved"
        elif post_rank == top_k + 1 and pre_rank <= top_k:
            category = "Dropped"
            color = "#b71c1c"  # Dark Red - "infinitely declined"
        elif abs(delta_rank) < threshold:
            category = "No Change"
            color = "#7f7f7f"  # Gray
        elif delta_rank > 0:
            category = "Improved"
            color = "#4caf50"  # Light Green - existing term improved
        else:  # delta_rank < 0
            category = "Declined"
            color = "#f44336"  # Light Red - existing term declined

        rank_data.append(
            {
                "term": term,
                "pre_rank": pre_rank,
                "post_rank": post_rank,
                "delta_rank": delta_rank,
                "delta_tfidf": delta_tfidf,
                "category": category,
                "color": color,
                "pre_tfidf": pre_tfidf,
                "post_tfidf": post_tfidf,
            }
        )

    rank_df = pd.DataFrame(rank_data)

    # Sort by absolute delta rank for diverging bars
    rank_df_sorted = rank_df.sort_values("delta_rank", key=abs, ascending=False)

    # Create the visualization - single panel focused on diverging bars
    fig, ax = plt.subplots(figsize=(14, 10), dpi=CHART_STYLE["dpi"])

    # Create diverging bars
    bars = ax.barh(
        range(len(rank_df_sorted)),
        rank_df_sorted["delta_rank"],
        color=rank_df_sorted["color"],
        alpha=0.8,
    )

    # Customize diverging bars
    ax.set_yticks(range(len(rank_df_sorted)))
    ax.set_yticklabels(rank_df_sorted["term"], fontsize=10)
    ax.set_title("Rank Movement Magnitude", fontsize=16, fontweight="bold")
    ax.set_xlabel("Δ Rank (Positive = Improved)", fontsize=14)
    ax.axvline(x=0, color="black", linestyle="-", alpha=0.7, linewidth=2)
    ax.grid(True, alpha=0.3, axis="x")

    # Add value labels on bars for significant changes
    for i, (bar, delta) in enumerate(zip(bars, rank_df_sorted["delta_rank"])):
        if abs(delta) >= threshold:  # Only label significant changes
            ax.text(
                delta + (0.5 if delta >= 0 else -0.5),
                i,
                f"{delta:+d}",
                ha="left" if delta >= 0 else "right",
                va="center",
                fontsize=9,
                fontweight="bold",
            )

    # Add main title
    fig.suptitle(title, fontsize=18, fontweight="bold", y=0.95)

    # Add refined legend
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#4caf50", alpha=0.8, label="Improved Rank"),
        Patch(facecolor="#1b5e20", alpha=0.8, label="New (Post Only)"),
        Patch(facecolor="#f44336", alpha=0.8, label="Declined Rank"),
        Patch(facecolor="#b71c1c", alpha=0.8, label="Dropped (Pre Only)"),
        Patch(facecolor="#7f7f7f", alpha=0.8, label="No Change"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=12)

    # Save the plot
    plt.savefig(
        output_path,
        dpi=CHART_STYLE["dpi"],
        bbox_inches="tight",
        facecolor=CHART_STYLE["facecolor"],
    )
    plt.close()

    print(f"Saved: {output_path}")

    # Print summary statistics
    print("\nRank Movement Summary:")
    print(f"Total candidate terms: {len(rank_df)}")
    print(f"Improved: {len(rank_df[rank_df['category'] == 'Improved'])}")
    print(f"Declined: {len(rank_df[rank_df['category'] == 'Declined'])}")
    print(f"No Change: {len(rank_df[rank_df['category'] == 'No Change'])}")
    print(f"New: {len(rank_df[rank_df['category'] == 'New'])}")
    print(f"Dropped: {len(rank_df[rank_df['category'] == 'Dropped'])}")


def create_all_visualizations(
    dataset: pd.DataFrame, stratified_results: Dict, visualization_output_dir: str
) -> None:
    """
    Create all visualizations.

    Args:
        dataset: Original dataset
        stratified_results: Stratified analysis results
        visualization_output_dir: Directory to save visualizations
    """
    print("Creating visualizations...")

    # Create subdirectories to match training structure
    overall_dir = os.path.join(visualization_output_dir, "overall")
    condition_dir = os.path.join(visualization_output_dir, "condition")
    period_dir = os.path.join(visualization_output_dir, "period")

    os.makedirs(overall_dir, exist_ok=True)
    os.makedirs(condition_dir, exist_ok=True)
    os.makedirs(period_dir, exist_ok=True)

    # 1. Overall top keywords
    if "overall" in stratified_results:
        create_top_keywords_chart(
            stratified_results["overall"],
            "Top 15 Keywords - Overall Analysis",
            os.path.join(overall_dir, "top_10_keywords_overall.png"),
        )

    # 2. Condition comparison
    create_condition_comparison_chart(
        stratified_results,
        "Top 10 Keywords by Condition",
        os.path.join(condition_dir, "top_keywords_by_condition.png"),
    )

    # 3. Period comparison
    create_period_comparison_chart(
        stratified_results,
        "Top 15 Keywords by Time Period",
        os.path.join(period_dir, "pre_vs_post_keywords.png"),
    )

    # 3b. Rank movement analysis with slopegraph and diverging bars
    create_rank_movement_analysis(
        stratified_results,
        "Pre → Post Period Rank Movement Analysis",
        os.path.join(period_dir, "rank_movement_analysis.png"),
    )

    # 4. Cross-dimensional heatmap
    create_cross_dimensional_heatmap(
        stratified_results,
        "Cross-Dimensional Keyword Analysis Heatmap",
        os.path.join(visualization_output_dir, "cross_dimensional_heatmap.png"),
    )

    # 5. Individual condition analysis
    for condition in ["control", "treatment", "engagement"]:
        condition_key = f"condition_{condition}"
        if condition_key in stratified_results:
            create_top_keywords_chart(
                stratified_results[condition_key],
                f"Top 15 Keywords - Condition {condition}",
                os.path.join(condition_dir, f"condition_{condition}_keywords.png"),
            )

    # 6. Individual period analysis
    for period in ["pre", "post"]:
        period_key = f"period_{period}"
        if period_key in stratified_results:
            create_top_keywords_chart(
                stratified_results[period_key],
                f"Top 15 Keywords - {period.capitalize()} Period",
                os.path.join(period_dir, f"period_{period}_keywords.png"),
            )


def save_visualization_metadata(
    visualization_output_dir: str, training_dir_relative_path: str
) -> None:
    """Save metadata for the visualization run."""
    metadata = {
        "run_timestamp": pd.Timestamp.now().strftime("%Y-%m-%d_%H:%M:%S"),
        "mode": "demo",
        "source_training_data": training_dir_relative_path,
        "visualization_types": [
            "overall_top_keywords",
            "condition_comparison",
            "period_comparison",
            "cross_dimensional_heatmap",
            "individual_condition_analysis",
            "individual_period_analysis",
        ],
        "chart_style": CHART_STYLE,
        "condition_colors": CONDITION_COLORS,
        "source_colors": SOURCE_COLORS,
        "period_colors": PERIOD_COLORS,
        "output_directory": visualization_output_dir,
    }

    with open(os.path.join(visualization_output_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print(
        f"Visualization metadata saved to {os.path.join(visualization_output_dir, 'metadata.json')}"
    )


def main():
    """Main execution function."""
    print("=== TF-IDF Demo Visualization ===")

    # Check if results exist
    if not os.path.exists(OUTPUT_DIR):
        print(f"Error: Results directory {OUTPUT_DIR} not found.")
        print("Please run tfidf_demo_train.py first.")
        return

    # Load results
    dataset, group_models, stratified_results, training_dir_relative_path = (
        load_results()
    )

    # Create timestamped visualization directory
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H:%M:%S")
    visualization_output_dir = os.path.join(VISUALIZATIONS_BASE_DIR, timestamp)
    os.makedirs(visualization_output_dir, exist_ok=True)

    # Create visualizations
    create_all_visualizations(dataset, stratified_results, visualization_output_dir)

    # Save metadata
    save_visualization_metadata(visualization_output_dir, training_dir_relative_path)

    print("\n=== Visualization Complete ===")
    print(f"Visualizations saved to: {visualization_output_dir}")
    print("\nGenerated files:")

    # List files in subdirectories
    for subdir in ["overall", "condition", "period"]:
        subdir_path = os.path.join(visualization_output_dir, subdir)
        if os.path.exists(subdir_path):
            files = [f for f in os.listdir(subdir_path) if f.endswith(".png")]
            if files:
                print(f"  {subdir}/:")
                for file in files:
                    print(f"    - {file}")

    # List files in main directory
    main_files = [f for f in os.listdir(visualization_output_dir) if f.endswith(".png")]
    if main_files:
        print("  main directory:")
        for file in main_files:
            print(f"    - {file}")

    print(
        f"\nMetadata saved to: {os.path.join(visualization_output_dir, 'metadata.json')}"
    )


if __name__ == "__main__":
    main()
