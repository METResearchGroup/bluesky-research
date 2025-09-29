#!/usr/bin/env python3
"""
Debug script to visualize user join counts by month.

This script loads profile data and creates a histogram showing the number of users
that joined each month, helping to debug data loading issues.
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

        print(f"   - Loading from {timestamp_dir}: {len(parquet_files)} files")

        for file in sorted(parquet_files):
            file_path = os.path.join(timestamp_path, file)
            try:
                df = pd.read_parquet(file_path)
                all_dfs.append(df)
                total_files += 1
                successful_files.append(f"{timestamp_dir}/{file}")
                print(f"     ✅ {file}: {len(df)} users")
            except Exception as e:
                failed_files.append(f"{timestamp_dir}/{file}")
                print(f"     ❌ {file}: Error - {str(e)[:100]}...")

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
    Process join dates by converting to year-month format and handling pre-2024 dates.

    Args:
        df: DataFrame with 'created_at' column

    Returns:
        DataFrame with processed join dates
    """
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

    # Convert to year-month format (YYYY-MM)
    df["join_date_ym"] = df["created_at"].dt.to_period("M").astype(str)

    # Group pre-2024 dates as "2023-12"
    df.loc[df["join_date_ym"] < "2024-01", "join_date_ym"] = "2023-12"

    # Handle NaT values (invalid timestamps)
    df.loc[df["join_date_ym"] == "NaT", "join_date_ym"] = "Unknown"

    return df


def create_join_count_histogram(df: pd.DataFrame, output_dir: str) -> str:
    """
    Create a histogram showing the number of users that joined each month.

    Args:
        df: DataFrame with processed join dates
        output_dir: Directory to save the plot

    Returns:
        Path to the saved plot file
    """
    # Count users per join month
    join_counts = df["join_date_ym"].value_counts().sort_index()

    print("📈 Users by join date:")
    for date, count in join_counts.items():
        print(f"   - {date}: {count:,} users")

    # Create the plot
    plt.figure(figsize=(14, 8))

    # Create bars
    bars = plt.bar(
        range(len(join_counts)),
        join_counts.values,
        color="steelblue",
        alpha=0.7,
        edgecolor="black",
        linewidth=0.5,
    )

    # Customize the plot
    plt.xlabel("Join Date (Year-Month)", fontsize=12, fontweight="bold")
    plt.ylabel("Number of Users", fontsize=12, fontweight="bold")
    plt.title(
        "Number of Users Joined per Month\n(Debug Visualization)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )

    # Set x-axis labels
    plt.xticks(range(len(join_counts)), join_counts.index, rotation=45, ha="right")

    # Add value labels on top of bars
    for i, (bar, value) in enumerate(zip(bars, join_counts.values)):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(join_counts.values) * 0.01,
            f"{value:,}",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    # Add grid for better readability
    plt.grid(True, alpha=0.3, axis="y")

    # Adjust layout
    plt.tight_layout()

    # Save the plot
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    filename = f"user_join_counts_debug_{timestamp}.png"
    filepath = os.path.join(output_dir, filename)

    plt.savefig(filepath, dpi=300, bbox_inches="tight")
    plt.close()

    return filepath


def main():
    """Main function to run the debug visualization."""
    print("🔍 Starting User Join Count Debug Analysis")
    print("=" * 50)

    try:
        # Load profile data
        df = load_combined_profile_data()

        # Process join dates
        print("📅 Processing join dates...")
        df = process_join_dates(df)

        # Create output directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, "visualizations", "debug_join_counts")
        os.makedirs(output_dir, exist_ok=True)

        # Create histogram
        print("📊 Creating join count histogram...")
        plot_path = create_join_count_histogram(df, output_dir)

        print(f"💾 Debug plot saved to: {plot_path}")

        # Print summary statistics
        print("\n📊 Summary Statistics:")
        print(f"   - Total users analyzed: {len(df):,}")
        print(f"   - Join date periods: {df['join_date_ym'].nunique()}")
        print(
            f"   - Date range: {df['created_at'].min().strftime('%Y-%m-%d')} to {df['created_at'].max().strftime('%Y-%m-%d')}"
        )

        print("\n🎉 Debug analysis completed successfully!")

    except Exception as e:
        print(f"❌ Error during debug analysis: {e}")
        raise


if __name__ == "__main__":
    main()
