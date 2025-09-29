"""
Visualize toxicity and outrage patterns by user join date.

This script loads the combined toxicity and profile data, groups users by their
join date (year-month), and creates visualizations showing how average toxicity
and outrage vary based on when users joined Bluesky.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def load_combined_profile_data() -> pd.DataFrame:
    """
    Load the combined toxicity and profile data from all chunk files across all timestamp directories.

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

    for timestamp_dir in sorted(timestamp_dirs):
        timestamp_path = os.path.join(profiles_dir, timestamp_dir)
        parquet_files = [
            f for f in os.listdir(timestamp_path) if f.endswith(".parquet")
        ]

        for file in sorted(parquet_files):
            file_path = os.path.join(timestamp_path, file)
            df = pd.read_parquet(file_path)
            all_dfs.append(df)
            total_files += 1

    if not all_dfs:
        raise FileNotFoundError("No parquet files found in any timestamp directory")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ Loaded {len(combined_df):,} user profiles from {total_files} files")

    return combined_df


def process_join_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process join dates by converting to year-month format and handling pre-2024 dates.

    Args:
        df: DataFrame with created_at column

    Returns:
        DataFrame with processed join_date_ym column
    """
    print("📅 Processing join dates...")

    # Convert created_at to datetime
    df["created_at"] = pd.to_datetime(df["created_at"])

    # Extract year-month (YYYY-MM format)
    df["join_date_ym"] = df["created_at"].dt.to_period("M").astype(str)

    # Group everything before 2024 as "2023-12"
    df.loc[df["created_at"].dt.year < 2024, "join_date_ym"] = "2023-12"

    # Count users by join date
    join_date_counts = df["join_date_ym"].value_counts().sort_index()
    print("📈 Users by join date:")
    for date, count in join_date_counts.items():
        print(f"   - {date}: {count:,} users")

    return df


def calculate_monthly_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average toxicity and outrage for each join date month.

    Args:
        df: DataFrame with join_date_ym, average_toxicity, average_outrage columns

    Returns:
        DataFrame with monthly averages
    """
    print("📊 Calculating monthly averages...")

    # Group by join date and calculate averages
    monthly_stats = (
        df.groupby("join_date_ym")
        .agg(
            {
                "average_toxicity": ["mean", "std", "count"],
                "average_outrage": ["mean", "std", "count"],
                "followers_count": "mean",
                "follows_count": "mean",
                "posts_count": "mean",
            }
        )
        .round(4)
    )

    # Flatten column names
    monthly_stats.columns = ["_".join(col).strip() for col in monthly_stats.columns]
    monthly_stats = monthly_stats.reset_index()

    # Rename columns for clarity
    monthly_stats = monthly_stats.rename(
        columns={
            "average_toxicity_mean": "avg_toxicity",
            "average_toxicity_std": "toxicity_std",
            "average_toxicity_count": "toxicity_count",
            "average_outrage_mean": "avg_outrage",
            "average_outrage_std": "outrage_std",
            "average_outrage_count": "outrage_count",
            "followers_count_mean": "avg_followers",
            "follows_count_mean": "avg_follows",
            "posts_count_mean": "avg_posts",
        }
    )

    print(f"📈 Monthly averages calculated for {len(monthly_stats)} join date periods")

    return monthly_stats


def create_toxicity_outrage_plot(monthly_stats: pd.DataFrame, output_path: str):
    """
    Create line plots showing average toxicity and outrage by join date.

    Args:
        monthly_stats: DataFrame with monthly statistics
        output_path: Path to save the visualization
    """
    # Set up the plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Convert join_date_ym to datetime for plotting
    monthly_stats["plot_date"] = pd.to_datetime(monthly_stats["join_date_ym"])

    # Plot 1: Average Toxicity
    ax1.plot(
        monthly_stats["plot_date"],
        monthly_stats["avg_toxicity"],
        marker="o",
        linewidth=2,
        markersize=6,
        color="#1f77b4",
        label="Average Toxicity",
    )

    # Add error bars for toxicity
    ax1.errorbar(
        monthly_stats["plot_date"],
        monthly_stats["avg_toxicity"],
        yerr=monthly_stats["toxicity_std"],
        fmt="none",
        color="#1f77b4",
        alpha=0.6,
    )

    # Add study period vertical lines
    study_start = pd.to_datetime("2024-10")
    study_end = pd.to_datetime("2024-12")
    ax1.axvline(
        x=study_start,
        color="red",
        linestyle="--",
        linewidth=2,
        alpha=0.8,
        label="Study Period",
    )
    ax1.axvline(x=study_end, color="red", linestyle="--", linewidth=2, alpha=0.8)

    ax1.set_title(
        "Average Toxicity by User Join Date", fontsize=14, fontweight="bold", pad=20
    )
    ax1.set_ylabel("Average Toxicity Score", fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    # Format x-axis
    ax1.tick_params(axis="x", rotation=45)

    # Plot 2: Average Outrage
    ax2.plot(
        monthly_stats["plot_date"],
        monthly_stats["avg_outrage"],
        marker="s",
        linewidth=2,
        markersize=6,
        color="#ff7f0e",
        label="Average Outrage",
    )

    # Add error bars for outrage
    ax2.errorbar(
        monthly_stats["plot_date"],
        monthly_stats["avg_outrage"],
        yerr=monthly_stats["outrage_std"],
        fmt="none",
        color="#ff7f0e",
        alpha=0.6,
    )

    # Add study period vertical lines
    ax2.axvline(
        x=study_start,
        color="red",
        linestyle="--",
        linewidth=2,
        alpha=0.8,
        label="Study Period",
    )
    ax2.axvline(x=study_end, color="red", linestyle="--", linewidth=2, alpha=0.8)

    ax2.set_title(
        "Average Moral Outrage by User Join Date",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax2.set_ylabel("Average Outrage Score", fontsize=12, fontweight="bold")
    ax2.set_xlabel("User Join Date (Year-Month)", fontsize=12, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    # Format x-axis
    ax2.tick_params(axis="x", rotation=45)

    # Add overall title
    fig.suptitle(
        "Toxicity and Outrage Patterns by User Join Date\n(Red lines indicate study period)",
        fontsize=16,
        fontweight="bold",
        y=0.98,
    )

    # Adjust layout and save
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"💾 Toxicity/Outrage plot saved to: {output_path}")


def create_combined_plot(monthly_stats: pd.DataFrame, output_path: str):
    """
    Create a combined plot showing both toxicity and outrage on the same axes.

    Args:
        monthly_stats: DataFrame with monthly statistics
        output_path: Path to save the visualization
    """
    # Set up the plot
    plt.figure(figsize=(14, 8))

    # Convert join_date_ym to datetime for plotting
    monthly_stats["plot_date"] = pd.to_datetime(monthly_stats["join_date_ym"])

    # Plot both metrics
    plt.plot(
        monthly_stats["plot_date"],
        monthly_stats["avg_toxicity"],
        marker="o",
        linewidth=2,
        markersize=6,
        color="#1f77b4",
        label="Average Toxicity",
    )
    plt.plot(
        monthly_stats["plot_date"],
        monthly_stats["avg_outrage"],
        marker="s",
        linewidth=2,
        markersize=6,
        color="#ff7f0e",
        label="Average Outrage",
    )

    # Add error bars
    plt.errorbar(
        monthly_stats["plot_date"],
        monthly_stats["avg_toxicity"],
        yerr=monthly_stats["toxicity_std"],
        fmt="none",
        color="#1f77b4",
        alpha=0.6,
    )
    plt.errorbar(
        monthly_stats["plot_date"],
        monthly_stats["avg_outrage"],
        yerr=monthly_stats["outrage_std"],
        fmt="none",
        color="#ff7f0e",
        alpha=0.6,
    )

    # Add study period vertical lines
    study_start = pd.to_datetime("2024-10")
    study_end = pd.to_datetime("2024-12")
    plt.axvline(
        x=study_start,
        color="red",
        linestyle="--",
        linewidth=2,
        alpha=0.8,
        label="Study Period",
    )
    plt.axvline(x=study_end, color="red", linestyle="--", linewidth=2, alpha=0.8)

    # Customize the plot
    plt.title(
        "Toxicity and Outrage Patterns by User Join Date\n(Red lines indicate study period)",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    plt.xlabel("User Join Date (Year-Month)", fontsize=12, fontweight="bold")
    plt.ylabel("Average Score", fontsize=12, fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)

    # Format x-axis
    plt.tick_params(axis="x", rotation=45)

    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"💾 Combined plot saved to: {output_path}")


def create_user_count_plot(monthly_stats: pd.DataFrame, output_path: str):
    """
    Create a bar plot showing the number of users by join date.

    Args:
        monthly_stats: DataFrame with monthly statistics
        output_path: Path to save the visualization
    """
    # Set up the plot
    plt.figure(figsize=(14, 6))

    # Convert join_date_ym to datetime for plotting
    monthly_stats["plot_date"] = pd.to_datetime(monthly_stats["join_date_ym"])

    # Create bar plot
    bars = plt.bar(
        monthly_stats["plot_date"],
        monthly_stats["toxicity_count"],
        color="lightblue",
        edgecolor="navy",
        alpha=0.7,
        width=20,
    )

    # Add study period vertical lines
    study_start = pd.to_datetime("2024-10")
    study_end = pd.to_datetime("2024-12")
    plt.axvline(
        x=study_start,
        color="red",
        linestyle="--",
        linewidth=2,
        alpha=0.8,
        label="Study Period",
    )
    plt.axvline(x=study_end, color="red", linestyle="--", linewidth=2, alpha=0.8)

    # Add value labels on bars
    for bar, count in zip(bars, monthly_stats["toxicity_count"]):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            f"{count}",
            ha="center",
            va="bottom",
            fontweight="bold",
        )

    # Customize the plot
    plt.title(
        "Number of Sampled Users by Join Date\n(Red lines indicate study period)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    plt.xlabel("User Join Date (Year-Month)", fontsize=12, fontweight="bold")
    plt.ylabel("Number of Users", fontsize=12, fontweight="bold")
    plt.grid(True, alpha=0.3, axis="y")
    plt.legend()

    # Format x-axis
    plt.tick_params(axis="x", rotation=45)

    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"💾 User count plot saved to: {output_path}")


def save_visualizations(monthly_stats: pd.DataFrame) -> list[str]:
    """
    Save all visualizations to timestamped directory.

    Args:
        monthly_stats: DataFrame with monthly statistics

    Returns:
        List of paths to saved visualization files
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create timestamp-based directory structure
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    viz_dir = os.path.join(
        script_dir, "visualizations", "toxicity_by_join_date", timestamp
    )
    os.makedirs(viz_dir, exist_ok=True)

    saved_files = []

    # Create toxicity/outrage separate plots
    toxicity_outrage_file = os.path.join(viz_dir, "toxicity_outrage_by_join_date.png")
    create_toxicity_outrage_plot(monthly_stats, toxicity_outrage_file)
    saved_files.append(toxicity_outrage_file)

    # Create combined plot
    combined_file = os.path.join(viz_dir, "combined_toxicity_outrage_by_join_date.png")
    create_combined_plot(monthly_stats, combined_file)
    saved_files.append(combined_file)

    # Create user count plot
    count_file = os.path.join(viz_dir, "user_count_by_join_date.png")
    create_user_count_plot(monthly_stats, count_file)
    saved_files.append(count_file)

    return saved_files


def main():
    """Main function to run the join date analysis and visualization."""
    print("📊 Starting Toxicity Analysis by Join Date")
    print("=" * 50)

    try:
        # Load combined profile data
        df = load_combined_profile_data()

        # Process join dates
        df = process_join_dates(df)

        # Calculate monthly averages
        monthly_stats = calculate_monthly_averages(df)

        # Create and save visualizations
        saved_files = save_visualizations(monthly_stats)

        print()
        print("🎉 Join date analysis completed successfully!")
        print("📁 Visualizations saved to:")
        for file_path in saved_files:
            print(f"   - {file_path}")

        print()
        print("📊 Summary Statistics:")
        print(f"   - Total users analyzed: {len(df):,}")
        print(f"   - Join date periods: {len(monthly_stats)}")
        print(f"   - Overall average toxicity: {df['average_toxicity'].mean():.4f}")
        print(f"   - Overall average outrage: {df['average_outrage'].mean():.4f}")

        # Show date range
        if df["created_at"].notna().any():
            join_dates = df["created_at"]
            print(
                f"   - Join date range: {join_dates.min().strftime('%Y-%m-%d')} to {join_dates.max().strftime('%Y-%m-%d')}"
            )

        # Show study period statistics
        study_period_users = df[
            df["join_date_ym"].isin(["2024-10", "2024-11", "2024-12"])
        ]
        if not study_period_users.empty:
            print(
                f"   - Users who joined during study period: {len(study_period_users):,}"
            )
            print(
                f"   - Study period avg toxicity: {study_period_users['average_toxicity'].mean():.4f}"
            )
            print(
                f"   - Study period avg outrage: {study_period_users['average_outrage'].mean():.4f}"
            )

    except Exception as e:
        print(f"❌ Error during join date analysis: {e}")
        raise


if __name__ == "__main__":
    main()
