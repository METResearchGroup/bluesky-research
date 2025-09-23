"""Visualization functions for hashtag analysis.

This module provides visualization functions for hashtag analysis results,
including charts for top hashtags by condition and election period.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from lib.log.logger import get_logger

logger = get_logger(__file__)

# Set style for better looking plots
plt.style.use("seaborn-v0_8")
sns.set_palette("husl")


def create_top_hashtags_by_condition_chart(
    df: pd.DataFrame, output_path: str, top_n: int = 10, title_suffix: str = ""
) -> None:
    """Create horizontal bar chart of top hashtags by condition.

    Args:
        df: Hashtag analysis DataFrame
        output_path: Path to save the chart
        top_n: Number of top hashtags to show per condition
        title_suffix: Additional text for the title
    """
    logger.info("Creating top hashtags by condition chart...")

    # Get top hashtags by condition
    condition_totals = df.groupby(["condition", "hashtag"])["count"].sum().reset_index()
    top_hashtags_by_condition = (
        condition_totals.groupby("condition")
        .apply(lambda x: x.nlargest(top_n, "count"))
        .reset_index(drop=True)
    )

    # Create subplots
    conditions = top_hashtags_by_condition["condition"].unique()
    n_conditions = len(conditions)

    # Determine subplot layout
    if n_conditions <= 2:
        fig, axes = plt.subplots(1, n_conditions, figsize=(8 * n_conditions, 6))
        if n_conditions == 1:
            axes = [axes]
    else:
        rows = (n_conditions + 1) // 2
        fig, axes = plt.subplots(rows, 2, figsize=(16, 6 * rows))
        axes = axes.flatten()

    for i, condition in enumerate(conditions):
        ax = axes[i]
        condition_data = top_hashtags_by_condition[
            top_hashtags_by_condition["condition"] == condition
        ]

        if not condition_data.empty:
            # Create horizontal bar chart
            bars = ax.barh(range(len(condition_data)), condition_data["count"])
            ax.set_yticks(range(len(condition_data)))
            ax.set_yticklabels(condition_data["hashtag"])
            ax.set_xlabel("Count")
            ax.set_title(f"Top {top_n} Hashtags - {condition}")
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
            ax.set_title(f"Top Hashtags - {condition} (No Data)")

    # Hide unused subplots
    for i in range(n_conditions, len(axes)):
        axes[i].set_visible(False)

    # Overall title
    title = f"Top Hashtags by Condition{title_suffix}"
    fig.suptitle(title, fontsize=16, fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Top hashtags by condition chart saved to {output_path}")


def create_pre_post_election_comparison_chart(
    df: pd.DataFrame, output_path: str, top_n: int = 10, title_suffix: str = ""
) -> None:
    """Create comparison chart of top hashtags pre vs post election.

    Args:
        df: Hashtag analysis DataFrame
        output_path: Path to save the chart
        top_n: Number of top hashtags to show per period
        title_suffix: Additional text for the title
    """
    logger.info("Creating pre/post election comparison chart...")

    # Get top hashtags by election period
    period_totals = (
        df.groupby(["pre_post_election", "hashtag"])["count"].sum().reset_index()
    )
    top_hashtags_by_period = (
        period_totals.groupby("pre_post_election")
        .apply(lambda x: x.nlargest(top_n, "count"))
        .reset_index(drop=True)
    )

    # Create subplots
    periods = ["pre_election", "post_election"]
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))

    for i, period in enumerate(periods):
        ax = axes[i]
        period_data = top_hashtags_by_period[
            top_hashtags_by_period["pre_post_election"] == period
        ]

        if not period_data.empty:
            # Create horizontal bar chart
            bars = ax.barh(range(len(period_data)), period_data["count"])
            ax.set_yticks(range(len(period_data)))
            ax.set_yticklabels(period_data["hashtag"])
            ax.set_xlabel("Count")
            ax.set_title(f'Top {top_n} Hashtags - {period.replace("_", " ").title()}')
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
    title = f"Top Hashtags: Pre vs Post Election{title_suffix}"
    fig.suptitle(title, fontsize=16, fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Pre/post election comparison chart saved to {output_path}")


def create_hashtag_frequency_distribution_chart(
    df: pd.DataFrame, output_path: str, title_suffix: str = ""
) -> None:
    """Create histogram of hashtag frequency distribution.

    Args:
        df: Hashtag analysis DataFrame
        output_path: Path to save the chart
        title_suffix: Additional text for the title
    """
    logger.info("Creating hashtag frequency distribution chart...")

    # Get overall hashtag counts
    hashtag_counts = df.groupby("hashtag")["count"].sum().values

    plt.figure(figsize=(12, 8))
    plt.hist(hashtag_counts, bins=50, alpha=0.7, edgecolor="black", linewidth=0.5)
    plt.xlabel("Hashtag Frequency")
    plt.ylabel("Number of Hashtags")
    plt.title(f"Hashtag Frequency Distribution{title_suffix}")
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
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Hashtag frequency distribution chart saved to {output_path}")


def create_condition_election_period_heatmap(
    df: pd.DataFrame, output_path: str, top_n: int = 20, title_suffix: str = ""
) -> None:
    """Create heatmap of hashtag usage by condition and election period.

    Args:
        df: Hashtag analysis DataFrame
        output_path: Path to save the chart
        top_n: Number of top hashtags to include
        title_suffix: Additional text for the title
    """
    logger.info("Creating condition-election period heatmap...")

    # Get top hashtags overall
    top_hashtags = df.groupby("hashtag")["count"].sum().nlargest(top_n).index

    # Create pivot table
    pivot_data = df[df["hashtag"].isin(top_hashtags)].pivot_table(
        index="hashtag",
        columns=["condition", "pre_post_election"],
        values="count",
        fill_value=0,
    )

    # Flatten column names
    pivot_data.columns = [
        f"{condition}_{period}" for condition, period in pivot_data.columns
    ]

    plt.figure(figsize=(14, 10))
    sns.heatmap(
        pivot_data, annot=True, fmt="d", cmap="YlOrRd", cbar_kws={"label": "Count"}
    )
    plt.title(f"Hashtag Usage by Condition and Election Period{title_suffix}")
    plt.xlabel("Condition - Election Period")
    plt.ylabel("Hashtag")
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logger.info(f"Condition-election period heatmap saved to {output_path}")


def create_all_visualizations(
    df: pd.DataFrame, output_dir: str, timestamp: str, top_n: int = 10
) -> None:
    """Create all hashtag analysis visualizations.

    Args:
        df: Hashtag analysis DataFrame
        output_dir: Output directory path
        timestamp: Timestamp string for filenames
        top_n: Number of top hashtags to show in charts
    """
    logger.info("Creating all hashtag analysis visualizations...")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Create visualizations
    try:
        # Top hashtags by condition
        create_top_hashtags_by_condition_chart(
            df=df,
            output_path=os.path.join(
                output_dir, f"top_hashtags_by_condition_{timestamp}.png"
            ),
            top_n=top_n,
            title_suffix=f" (Analysis: {timestamp})",
        )

        # Pre/post election comparison
        create_pre_post_election_comparison_chart(
            df=df,
            output_path=os.path.join(
                output_dir, f"pre_post_election_comparison_{timestamp}.png"
            ),
            top_n=top_n,
            title_suffix=f" (Analysis: {timestamp})",
        )

        # Frequency distribution
        create_hashtag_frequency_distribution_chart(
            df=df,
            output_path=os.path.join(
                output_dir, f"hashtag_frequency_distribution_{timestamp}.png"
            ),
            title_suffix=f" (Analysis: {timestamp})",
        )

        # Condition-election period heatmap
        create_condition_election_period_heatmap(
            df=df,
            output_path=os.path.join(
                output_dir, f"condition_election_heatmap_{timestamp}.png"
            ),
            top_n=top_n,
            title_suffix=f" (Analysis: {timestamp})",
        )

        logger.info("All hashtag analysis visualizations created successfully")

    except Exception as e:
        logger.error(f"Failed to create visualizations: {e}")
        raise
