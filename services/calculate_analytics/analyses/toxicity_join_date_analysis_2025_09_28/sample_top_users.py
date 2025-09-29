"""
Sample top users from aggregated toxicity data for further analysis.

This script loads the aggregated author-to-average toxicity/outrage data,
removes outliers using the IQR method, identifies the top 10% of users
by post count, and randomly samples 1000 of these users for detailed analysis.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime


def load_and_filter_data() -> pd.DataFrame:
    """
    Load the aggregated data and remove outliers using IQR method.

    Returns:
        DataFrame with outliers removed
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Hardcoded path to the results directory
    results_dir = os.path.join(script_dir, "results", "2025-09-28_16:43:34")
    data_file = os.path.join(results_dir, "aggregated_author_toxicity_outrage.parquet")

    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Data file not found: {data_file}")

    print(f"📊 Loading data from: {data_file}")
    df = pd.read_parquet(data_file)
    print(f"✅ Loaded {len(df):,} authors")
    print(f"   - Total posts: {df['total_labeled_posts'].sum():,}")
    print(
        f"   - Posts per author range: {df['total_labeled_posts'].min()} - {df['total_labeled_posts'].max()}"
    )

    # Remove outliers using IQR method (same as visualization script)
    post_counts = df["total_labeled_posts"]
    Q1 = post_counts.quantile(0.25)
    Q3 = post_counts.quantile(0.75)
    IQR = Q3 - Q1

    # Define outlier bounds (1.5 × IQR beyond quartiles)
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Filter out outliers
    original_count = len(df)
    df_filtered = df[(post_counts >= lower_bound) & (post_counts <= upper_bound)]
    removed_count = original_count - len(df_filtered)

    print(f"🔧 Removed {removed_count:,} outliers using IQR method")
    print(f"   - IQR bounds: {lower_bound:.0f} - {upper_bound:.0f} posts")
    print(f"   - Remaining authors: {len(df_filtered):,}")
    print(
        f"   - New range: {df_filtered['total_labeled_posts'].min()} - {df_filtered['total_labeled_posts'].max()}"
    )

    return df_filtered


def get_top_10_percent_users(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the top 10% of users by post count.

    Args:
        df: DataFrame with total_labeled_posts column

    Returns:
        DataFrame containing only the top 10% of users
    """
    post_counts = df["total_labeled_posts"]

    # Calculate 90th percentile threshold
    threshold_90th = np.percentile(post_counts, 90.0)

    # Filter for top 10% users
    top_10_percent = df[post_counts >= threshold_90th].copy()

    print("📈 Top 10% users analysis:")
    print(f"   - Threshold: ≥{threshold_90th:.0f} posts")
    print(f"   - Number of users: {len(top_10_percent):,}")
    print(
        f"   - Posts range: {top_10_percent['total_labeled_posts'].min()} - {top_10_percent['total_labeled_posts'].max()}"
    )
    print(
        f"   - Total posts by top 10%: {top_10_percent['total_labeled_posts'].sum():,}"
    )

    return top_10_percent


def sample_users(df: pd.DataFrame, sample_size: int = 1000) -> pd.DataFrame:
    """
    Randomly sample users from the dataframe.

    Args:
        df: DataFrame to sample from
        sample_size: Number of users to sample

    Returns:
        DataFrame with sampled users
    """
    if len(df) < sample_size:
        print(f"⚠️  Warning: Only {len(df):,} users available, sampling all of them")
        sample_size = len(df)

    # Set random seed for reproducibility
    np.random.seed(42)

    # Randomly sample users
    sampled_df = df.sample(n=sample_size, random_state=42).copy()

    print("🎯 Random sampling:")
    print(f"   - Sampled {len(sampled_df):,} users")
    print(
        f"   - Posts range: {sampled_df['total_labeled_posts'].min()} - {sampled_df['total_labeled_posts'].max()}"
    )
    print(
        f"   - Total posts by sampled users: {sampled_df['total_labeled_posts'].sum():,}"
    )

    return sampled_df


def save_sampled_data(df: pd.DataFrame) -> str:
    """
    Save the sampled user data to a parquet file.

    Args:
        df: DataFrame with sampled user data

    Returns:
        Path to the saved file
    """
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create timestamp-based directory structure
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    output_dir = os.path.join(script_dir, "sampled_users", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    # Select only the required columns
    columns_to_save = [
        "author_did",
        "average_toxicity",
        "average_outrage",
        "total_labeled_posts",
    ]
    df_to_save = df[columns_to_save].copy()

    # Save to parquet
    output_file = os.path.join(output_dir, "sampled_users.parquet")
    df_to_save.to_parquet(output_file, index=False)

    print(f"💾 Sampled data saved to: {output_file}")
    print(f"   - File size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
    print(f"   - Columns: {', '.join(columns_to_save)}")

    return output_file


def main():
    """Main function to run the sampling process."""
    print("🎯 Starting Top 10% User Sampling")
    print("=" * 50)

    try:
        # Load and filter data
        df_filtered = load_and_filter_data()

        # Get top 10% users
        top_10_percent = get_top_10_percent_users(df_filtered)

        # Sample 1000 users
        sampled_users = sample_users(top_10_percent, sample_size=1000)

        # Save sampled data
        output_path = save_sampled_data(sampled_users)

        print()
        print("🎉 Sampling completed successfully!")
        print(f"📁 Results saved to: {output_path}")
        print()
        print("📊 Summary:")
        print(f"   - Original authors: {len(df_filtered):,}")
        print(f"   - Top 10% authors: {len(top_10_percent):,}")
        print(f"   - Sampled authors: {len(sampled_users):,}")
        print(
            f"   - Posts range in sample: {sampled_users['total_labeled_posts'].min()} - {sampled_users['total_labeled_posts'].max()}"
        )
        print(
            f"   - Average toxicity in sample: {sampled_users['average_toxicity'].mean():.4f}"
        )
        print(
            f"   - Average outrage in sample: {sampled_users['average_outrage'].mean():.4f}"
        )

    except Exception as e:
        print(f"❌ Error during sampling: {e}")
        raise


if __name__ == "__main__":
    main()
