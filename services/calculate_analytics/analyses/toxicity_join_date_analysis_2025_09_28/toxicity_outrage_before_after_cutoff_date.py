#!/usr/bin/env python3
"""
Toxicity and Outrage Analysis: Before vs After Cutoff Date

This script analyzes toxicity and outrage levels for users who joined before
vs after September 1, 2024, creating a comparative bar chart visualization.

Author: AI Assistant
Date: 2025-09-29
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def load_combined_profile_data() -> pd.DataFrame:
    """
    Load the combined toxicity and profile data from all chunk files across all timestamp directories.
    Handles errors by skipping problematic files and reporting which files loaded successfully.

    Returns:
        DataFrame with combined toxicity and profile data
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Find the profile data directory
    profiles_dir = os.path.join(script_dir, "sampled_user_profiles")
    if not os.path.exists(profiles_dir):
        raise FileNotFoundError(f"Profile data directory not found: {profiles_dir}")

    # Get all timestamp directories
    timestamp_dirs = [
        d
        for d in os.listdir(profiles_dir)
        if os.path.isdir(os.path.join(profiles_dir, d))
    ]
    if not timestamp_dirs:
        raise FileNotFoundError(
            "No timestamp directories found in sampled_user_profiles"
        )

    print(f"📊 Loading profile data from: {profiles_dir}")
    print(f"   - Found {len(timestamp_dirs)} timestamp directories")

    # Load all parquet files from all timestamp directories
    all_dfs = []
    total_files = 0
    successful_files = []
    failed_files = []

    for timestamp_dir in sorted(timestamp_dirs):
        timestamp_path = os.path.join(profiles_dir, timestamp_dir)
        parquet_files = [
            f for f in os.listdir(timestamp_path) if f.endswith(".parquet")
        ]

        for file in sorted(parquet_files):
            file_path = os.path.join(timestamp_path, file)
            try:
                df = pd.read_parquet(file_path)
                all_dfs.append(df)
                total_files += 1
                successful_files.append(f"{timestamp_dir}/{file}")
                print(f"   ✅ {timestamp_dir}/{file}: {len(df)} users")
            except Exception as e:
                failed_files.append(f"{timestamp_dir}/{file}")
                print(f"   ❌ {timestamp_dir}/{file}: Error - {str(e)[:100]}...")

    if not all_dfs:
        raise FileNotFoundError("No parquet files could be loaded successfully")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ Loaded {len(combined_df):,} user profiles from {total_files} files")

    if failed_files:
        print(f"⚠️  Failed to load {len(failed_files)} files:")
        for failed_file in failed_files:
            print(f"   - {failed_file}")

    print(f"📊 Summary: {len(successful_files)} successful, {len(failed_files)} failed")

    return combined_df


def process_join_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process join dates by converting to datetime and categorizing by cutoff date.

    Args:
        df: DataFrame with 'created_at' column

    Returns:
        DataFrame with processed join dates and cutoff categories
    """
    print("📅 Processing join dates...")

    df = df.copy()

    # Convert created_at to datetime with error handling
    try:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    except Exception as e:
        print(f"⚠️  Error converting timestamps: {e}")
        # Try alternative parsing methods
        try:
            df["created_at"] = pd.to_datetime(
                df["created_at"], format="ISO8601", errors="coerce"
            )
        except Exception as e2:
            print(f"⚠️  ISO8601 parsing failed: {e2}")
            try:
                df["created_at"] = pd.to_datetime(
                    df["created_at"], format="mixed", errors="coerce"
                )
            except Exception as e3:
                print(f"⚠️  Mixed format parsing failed: {e3}")
                print("   Setting invalid timestamps to NaT")
                df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # Check for invalid timestamps and report
    invalid_count = df["created_at"].isna().sum()
    if invalid_count > 0:
        print(f"⚠️  Found {invalid_count:,} invalid timestamps (set to NaT)")
        print(f"   - Valid timestamps: {len(df) - invalid_count:,}")
        print(f"   - Invalid timestamps: {invalid_count:,}")

    # Define cutoff date: September 1, 2024
    cutoff_date = pd.to_datetime("2024-09-01")

    # Categorize users by join date relative to cutoff
    def categorize_join_date(x):
        if pd.isna(x):
            return "Unknown"
        # Handle timezone-aware timestamps by converting to naive
        if hasattr(x, "tz") and x.tz is not None:
            x = x.tz_localize(None)
        return "Before Sep 1, 2024" if x < cutoff_date else "Sep 1, 2024 or Later"

    df["join_category"] = df["created_at"].apply(categorize_join_date)

    # Handle NaT values (invalid timestamps)
    df.loc[df["created_at"].isna(), "join_category"] = "Unknown"

    # Count users by category
    category_counts = df["join_category"].value_counts()
    print("📈 Users by join category:")
    for category, count in category_counts.items():
        print(f"   - {category}: {count:,} users")

    return df


def calculate_before_after_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average toxicity and outrage for before/after cutoff categories.

    Args:
        df: DataFrame with toxicity, outrage, and join_category columns

    Returns:
        DataFrame with calculated averages, standard deviations, and confidence intervals
    """
    print("📊 Calculating before/after cutoff averages...")

    # Filter out "Unknown" entries for analysis
    analysis_df = df[df["join_category"] != "Unknown"].copy()

    if analysis_df.empty:
        print("⚠️  No valid data available for analysis")
        return pd.DataFrame()

    # Calculate statistics for each metric including confidence intervals
    def calculate_stats(group):
        n = len(group)
        mean = group.mean()
        std = group.std(ddof=1)

        # Calculate 95% confidence interval for the mean
        import numpy as np
        from scipy import stats

        sem = std / np.sqrt(n)  # Standard error of the mean
        t_critical = stats.t.ppf(0.975, n - 1)  # 95% confidence, two-tailed
        margin_error = t_critical * sem
        ci_lower = mean - margin_error
        ci_upper = mean + margin_error

        return pd.Series(
            {
                "mean": mean,
                "std": std,
                "ci_lower": ci_lower,
                "ci_upper": ci_upper,
                "margin_error": margin_error,
            }
        )

    # Calculate statistics for each metric
    def calculate_group_stats(group_data, _group_name):
        n = len(group_data)
        mean = group_data.mean()
        std = group_data.std(ddof=1)

        # Calculate 95% confidence interval for the mean
        import numpy as np
        from scipy import stats

        sem = std / np.sqrt(n)  # Standard error of the mean
        t_critical = stats.t.ppf(0.975, n - 1)  # 95% confidence, two-tailed
        margin_error = t_critical * sem
        ci_lower = mean - margin_error
        ci_upper = mean + margin_error

        return {
            "mean": mean,
            "std": std,
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "margin_error": margin_error,
        }

    # Calculate statistics for each group and metric
    results = {}
    for category in analysis_df["join_category"].unique():
        group_data = analysis_df[analysis_df["join_category"] == category]

        toxicity_stats = calculate_group_stats(group_data["average_toxicity"], category)
        outrage_stats = calculate_group_stats(group_data["average_outrage"], category)
        total_posts = group_data["total_labeled_posts"].sum()

        results[category] = {
            "toxicity_mean": toxicity_stats["mean"],
            "toxicity_std": toxicity_stats["std"],
            "toxicity_ci_lower": toxicity_stats["ci_lower"],
            "toxicity_ci_upper": toxicity_stats["ci_upper"],
            "toxicity_margin_error": toxicity_stats["margin_error"],
            "outrage_mean": outrage_stats["mean"],
            "outrage_std": outrage_stats["std"],
            "outrage_ci_lower": outrage_stats["ci_lower"],
            "outrage_ci_upper": outrage_stats["ci_upper"],
            "outrage_margin_error": outrage_stats["margin_error"],
            "total_labeled_posts": total_posts,
        }

    # Convert to DataFrame
    averages = pd.DataFrame(results).T.round(4)

    print("📈 Average toxicity and outrage by join category:")
    for category in averages.index:
        toxicity_mean = averages.loc[category, "toxicity_mean"]
        toxicity_ci_lower = averages.loc[category, "toxicity_ci_lower"]
        toxicity_ci_upper = averages.loc[category, "toxicity_ci_upper"]
        outrage_mean = averages.loc[category, "outrage_mean"]
        outrage_ci_lower = averages.loc[category, "outrage_ci_lower"]
        outrage_ci_upper = averages.loc[category, "outrage_ci_upper"]
        posts = averages.loc[category, "total_labeled_posts"]
        print(f"   - {category}:")
        print(
            f"     * Toxicity: {toxicity_mean:.4f} [{toxicity_ci_lower:.4f}, {toxicity_ci_upper:.4f}]"
        )
        print(
            f"     * Outrage: {outrage_mean:.4f} [{outrage_ci_lower:.4f}, {outrage_ci_upper:.4f}]"
        )
        print(f"     * Total posts: {posts:,}")

    return averages


def create_before_after_bar_chart(
    averages_df: pd.DataFrame, df: pd.DataFrame, output_path: str
) -> str:
    """
    Create a bar chart comparing toxicity and outrage before vs after cutoff date.

    Args:
        averages_df: DataFrame with calculated averages
        df: Original DataFrame to calculate sample sizes
        output_path: Path to save the visualization
    """
    print("📊 Creating before/after cutoff bar chart...")

    if averages_df.empty:
        print("⚠️  No data available for visualization")
        return None

    # Set up the plot
    plt.figure(figsize=(12, 8))

    # Define categories and colors
    categories = ["Toxicity", "Outrage"]
    before_values = [
        averages_df.loc["Before Sep 1, 2024", "toxicity_mean"],
        averages_df.loc["Before Sep 1, 2024", "outrage_mean"],
    ]
    after_values = [
        averages_df.loc["Sep 1, 2024 or Later", "toxicity_mean"],
        averages_df.loc["Sep 1, 2024 or Later", "outrage_mean"],
    ]

    before_errors = [
        averages_df.loc["Before Sep 1, 2024", "toxicity_margin_error"],
        averages_df.loc["Before Sep 1, 2024", "outrage_margin_error"],
    ]
    after_errors = [
        averages_df.loc["Sep 1, 2024 or Later", "toxicity_margin_error"],
        averages_df.loc["Sep 1, 2024 or Later", "outrage_margin_error"],
    ]

    # Set up bar positions
    x = range(len(categories))
    width = 0.35

    # Create bars with error bars
    bars_before = plt.bar(
        [i - width / 2 for i in x],
        before_values,
        width,
        label="Before Sep 1, 2024",
        color="lightblue",
        alpha=0.8,
        edgecolor="navy",
        linewidth=1,
        yerr=before_errors,
        capsize=5,
        error_kw={"linewidth": 2, "capthick": 2},
    )

    bars_after = plt.bar(
        [i + width / 2 for i in x],
        after_values,
        width,
        label="Sep 1, 2024 or Later",
        color="darkblue",
        alpha=0.8,
        edgecolor="navy",
        linewidth=1,
        yerr=after_errors,
        capsize=5,
        error_kw={"linewidth": 2, "capthick": 2},
    )

    # Add value labels on bars (positioned to avoid overlap with error bars)
    for bars, values, errors in [
        (bars_before, before_values, before_errors),
        (bars_after, after_values, after_errors),
    ]:
        for bar, value, error in zip(bars, values, errors):
            height = bar.get_height()
            # Position label above the error bar cap
            label_y = height + error + 0.02
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                label_y,
                f"{value:.3f}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )

    # Customize the plot
    plt.xlabel("Trait", fontsize=14, fontweight="bold")
    plt.ylabel("Average Score", fontsize=14, fontweight="bold")
    plt.title(
        "Average Toxicity and Outrage: Before vs After Sep 1, 2024\n(95% Confidence Intervals)",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )

    plt.xticks(x, categories, fontsize=12)
    plt.yticks(fontsize=12)

    # Set y-axis limits to 0.5 for better legend spacing
    plt.ylim(0, 0.5)

    # Add significance indicators for outrage (p < 0.001)
    # Find the maximum height of the outrage bars (index 1)
    outrage_max_height = max(
        after_values[1] + after_errors[1],  # After bar + error
        before_values[1] + before_errors[1],  # Before bar + error
    )

    # Add horizontal connector line above outrage bars
    outrage_x_pos = 1  # Outrage is at index 1
    line_y = outrage_max_height + 0.02  # Position above the highest error bar

    # Draw horizontal line connecting both outrage bars
    plt.plot(
        [outrage_x_pos - width / 2, outrage_x_pos + width / 2],
        [line_y, line_y],
        "k-",
        linewidth=1,
    )

    # Add vertical lines connecting to bars
    plt.plot(
        [outrage_x_pos - width / 2, outrage_x_pos - width / 2],
        [outrage_max_height + 0.01, line_y],
        "k-",
        linewidth=1,
    )
    plt.plot(
        [outrage_x_pos + width / 2, outrage_x_pos + width / 2],
        [outrage_max_height + 0.01, line_y],
        "k-",
        linewidth=1,
    )

    # Add significance asterisks above the line
    plt.text(
        outrage_x_pos,
        line_y + 0.01,
        "***",
        ha="center",
        va="bottom",
        fontsize=14,
        fontweight="bold",
    )

    # Add legend
    plt.legend(fontsize=12, loc="upper right")

    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"💾 Before/After cutoff chart saved to: {output_path}")

    return output_path


def save_visualization(averages_df: pd.DataFrame, df: pd.DataFrame) -> list:
    """
    Save the before/after cutoff visualization to timestamped directory.

    Args:
        averages_df: DataFrame with calculated averages
        df: Original DataFrame for sample size calculation

    Returns:
        List of saved file paths
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create timestamped output directory
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    output_dir = os.path.join(
        script_dir,
        "visualizations",
        "toxicity_outrage_before_after_cutoff_date",
        timestamp,
    )
    os.makedirs(output_dir, exist_ok=True)

    # Create visualization
    chart_file = os.path.join(output_dir, "before_after_cutoff_comparison.png")
    saved_file = create_before_after_bar_chart(averages_df, df, chart_file)

    saved_files = []
    if saved_file:
        saved_files.append(saved_file)

    return saved_files


def main():
    """
    Main function to orchestrate the before/after cutoff analysis.
    """
    print("📊 Starting Toxicity/Outrage Before vs After Cutoff Analysis")
    print("=" * 60)

    try:
        # Load data
        df = load_combined_profile_data()

        # Process join dates
        df = process_join_dates(df)

        # Calculate averages
        averages_df = calculate_before_after_averages(df)

        if averages_df.empty:
            print("❌ No valid data available for analysis")
            return

        # Create and save visualizations
        saved_files = save_visualization(averages_df, df)

        print("\n🎉 Before/After cutoff analysis completed successfully!")
        print("📁 Visualizations saved to:")
        for file_path in saved_files:
            print(f"   - {file_path}")

        # Print summary statistics
        print("\n📊 Summary Statistics:")
        print(f"   - Total users analyzed: {len(df):,}")
        print(
            f"   - Users before Sep 1, 2024: {averages_df.loc['Before Sep 1, 2024', 'total_labeled_posts']:,} posts"
        )
        print(
            f"   - Users Sep 1, 2024 or later: {averages_df.loc['Sep 1, 2024 or Later', 'total_labeled_posts']:,} posts"
        )
        print(
            f"   - Before cutoff avg toxicity: {averages_df.loc['Before Sep 1, 2024', 'toxicity_mean']:.4f} "
            f"[{averages_df.loc['Before Sep 1, 2024', 'toxicity_ci_lower']:.4f}, {averages_df.loc['Before Sep 1, 2024', 'toxicity_ci_upper']:.4f}]"
        )
        print(
            f"   - After cutoff avg toxicity: {averages_df.loc['Sep 1, 2024 or Later', 'toxicity_mean']:.4f} "
            f"[{averages_df.loc['Sep 1, 2024 or Later', 'toxicity_ci_lower']:.4f}, {averages_df.loc['Sep 1, 2024 or Later', 'toxicity_ci_upper']:.4f}]"
        )
        print(
            f"   - Before cutoff avg outrage: {averages_df.loc['Before Sep 1, 2024', 'outrage_mean']:.4f} "
            f"[{averages_df.loc['Before Sep 1, 2024', 'outrage_ci_lower']:.4f}, {averages_df.loc['Before Sep 1, 2024', 'outrage_ci_upper']:.4f}]"
        )
        print(
            f"   - After cutoff avg outrage: {averages_df.loc['Sep 1, 2024 or Later', 'outrage_mean']:.4f} "
            f"[{averages_df.loc['Sep 1, 2024 or Later', 'outrage_ci_lower']:.4f}, {averages_df.loc['Sep 1, 2024 or Later', 'outrage_ci_upper']:.4f}]"
        )

    except Exception as e:
        print(f"❌ Error during before/after cutoff analysis: {e}")
        raise


if __name__ == "__main__":
    main()
