"""
Sample users from aggregated toxicity data for further analysis.

This script loads the aggregated author-to-average toxicity/outrage data,
applies configurable thresholding criteria (percentile or count-based),
and randomly samples users for detailed analysis.
"""

import os
import pandas as pd
import numpy as np
import yaml
from datetime import datetime
from typing import Dict, Any


def load_config() -> Dict[str, Any]:
    """
    Load sampling configuration from YAML file.

    Returns:
        Dictionary containing configuration parameters
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "sampling_config.yaml")

    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    print("📋 Loaded configuration:")
    print(f"   - Threshold criteria: {config['threshold_criteria']}")
    print(f"   - Threshold value: {config['threshold_value']}")
    print(f"   - Sample size: {config['sample_size']:,}")
    print(f"   - Remove outliers: {config['remove_outliers']}")

    return config


def find_latest_results_directory() -> str:
    """
    Find the most recent results directory.

    Returns:
        Path to the latest results directory
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_base_dir = os.path.join(script_dir, "results")

    if not os.path.exists(results_base_dir):
        raise FileNotFoundError(f"Results directory not found: {results_base_dir}")

    # Get all timestamp directories
    timestamp_dirs = [
        d
        for d in os.listdir(results_base_dir)
        if os.path.isdir(os.path.join(results_base_dir, d))
    ]

    if not timestamp_dirs:
        raise FileNotFoundError("No timestamp directories found in results")

    # Find the latest one
    latest_timestamp = sorted(timestamp_dirs)[-1]
    latest_dir = os.path.join(results_base_dir, latest_timestamp)

    print(f"📁 Using latest results directory: {latest_timestamp}")
    return latest_dir


def load_previously_sampled_users() -> set:
    """
    Load all previously sampled user DIDs from sampled_users directory.

    Returns:
        Set of DIDs that have been previously sampled
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sampled_users_dir = os.path.join(script_dir, "sampled_users")

    if not os.path.exists(sampled_users_dir):
        print("ℹ️  No sampled_users directory found")
        return set()

    previously_sampled_dids = set()

    # Check all timestamp directories for parquet files
    for timestamp_dir in os.listdir(sampled_users_dir):
        timestamp_path = os.path.join(sampled_users_dir, timestamp_dir)
        if os.path.isdir(timestamp_path):
            parquet_files = [
                f for f in os.listdir(timestamp_path) if f.endswith(".parquet")
            ]

            for file in parquet_files:
                file_path = os.path.join(timestamp_path, file)
                try:
                    df = pd.read_parquet(file_path)
                    if "author_did" in df.columns:
                        previously_sampled_dids.update(df["author_did"].tolist())
                        print(f"📁 Loaded {len(df)} DIDs from {timestamp_dir}/{file}")
                except Exception as e:
                    print(f"⚠️  Error reading {file_path}: {e}")

    print(f"🔧 Found {len(previously_sampled_dids):,} previously sampled DIDs")
    return previously_sampled_dids


def load_and_filter_data(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Load the aggregated data, optionally remove outliers, and filter out previously sampled users.

    Args:
        config: Configuration dictionary

    Returns:
        DataFrame with outliers removed (if configured) and previously sampled users excluded
    """
    # Find the latest results directory
    results_dir = find_latest_results_directory()
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

    # Load previously sampled users and filter them out
    previously_sampled_dids = load_previously_sampled_users()
    if previously_sampled_dids:
        original_count = len(df)
        df = df[~df["author_did"].isin(previously_sampled_dids)]
        filtered_count = len(df)
        print(
            f"🔧 Filtered out {original_count - filtered_count:,} previously sampled users"
        )
        print(f"   - Remaining authors: {filtered_count:,}")

    # Remove outliers if configured
    if config["remove_outliers"]:
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
    else:
        print("ℹ️  Skipping outlier removal as configured")
        return df


def apply_threshold_criteria(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply threshold criteria to filter users.

    Args:
        df: DataFrame with total_labeled_posts column
        config: Configuration dictionary

    Returns:
        DataFrame containing only users meeting threshold criteria
    """
    post_counts = df["total_labeled_posts"]
    criteria = config["threshold_criteria"]
    value = config["threshold_value"]

    if criteria == "percentile":
        # Calculate percentile threshold
        threshold = np.percentile(post_counts, 100 - value)
        filtered_df = df[post_counts >= threshold].copy()

        print("📈 Percentile-based thresholding:")
        print(f"   - Criteria: Top {value}% of users")
        print(f"   - Threshold: ≥{threshold:.0f} posts")

    elif criteria == "count":
        # Use minimum post count threshold
        threshold = value
        filtered_df = df[post_counts >= threshold].copy()

        print("📈 Count-based thresholding:")
        print(f"   - Criteria: Users with ≥{threshold} posts")
        print(f"   - Threshold: ≥{threshold} posts")

    else:
        raise ValueError(f"Unknown threshold criteria: {criteria}")

    print(f"   - Number of users meeting criteria: {len(filtered_df):,}")
    print(
        f"   - Posts range: {filtered_df['total_labeled_posts'].min()} - {filtered_df['total_labeled_posts'].max()}"
    )
    print(
        f"   - Total posts by filtered users: {filtered_df['total_labeled_posts'].sum():,}"
    )

    return filtered_df


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
    print("🎯 Starting Configurable User Sampling")
    print("=" * 50)

    try:
        # Load configuration
        config = load_config()

        # Load and filter data (automatically excludes previously sampled users)
        df_filtered = load_and_filter_data(config)

        # Apply threshold criteria
        thresholded_users = apply_threshold_criteria(df_filtered, config)

        # Sample users
        sampled_users = sample_users(
            thresholded_users, sample_size=config["sample_size"]
        )

        # Save sampled data
        output_path = save_sampled_data(sampled_users)

        print()
        print("🎉 Sampling completed successfully!")
        print(f"📁 Results saved to: {output_path}")
        print()
        print("📊 Summary:")
        print(f"   - Original authors: {len(df_filtered):,}")
        print(f"   - Authors meeting criteria: {len(thresholded_users):,}")
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
