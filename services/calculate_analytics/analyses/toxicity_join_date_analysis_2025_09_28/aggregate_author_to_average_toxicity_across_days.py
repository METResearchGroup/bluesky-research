"""
Aggregate author-to-average toxicity and outrage scores across all study days.

This script loads daily author-to-average toxicity/outrage data and calculates
weighted averages across all days for each author, providing a comprehensive
view of toxicity patterns across the entire study period.
"""

import os
import pandas as pd
from datetime import datetime
from lib.db.manage_local_data import load_data_from_local_storage
from services.calculate_analytics.shared.constants import (
    STUDY_END_DATE,
    STUDY_START_DATE,
    exclude_partition_dates,
)
from lib.datetime_utils import get_partition_dates


def aggregate_author_toxicity_across_days(partition_dates: list[str]) -> pd.DataFrame:
    """
    Aggregate author-to-average toxicity and outrage scores across all study days.

    This function loads daily data for each partition date and calculates weighted
    averages across all days for each author. The weighted average is calculated as:

    weighted_avg = sum(posts_per_day * avg_per_day) / sum(posts_per_day)

    Args:
        partition_dates: list of partition dates to process

    Returns:
        DataFrame with columns:
        - author_did: Author identifier
        - average_toxicity: Weighted average toxicity across all days
        - average_outrage: Weighted average outrage across all days
        - total_labeled_posts: Total posts across all days
    """
    print(f"🔄 Starting aggregation across {len(partition_dates)} partition dates...")

    # Dictionary to store running totals for each author
    author_data: dict[str, dict[str, float]] = {}

    for i, partition_date in enumerate(partition_dates, 1):
        print(
            f"📅 Processing partition date {i}/{len(partition_dates)}: {partition_date}"
        )

        try:
            # Load data for this partition date
            daily_data = load_data_from_local_storage(
                service="author_to_average_toxicity_outrage",
                directory="cache",
                export_format="parquet",
                partition_date=partition_date,
            )

            if daily_data.empty:
                print(f"⚠️  No data found for partition date {partition_date}")
                continue

            print(f"   📊 Loaded {len(daily_data)} authors for {partition_date}")

            # Process each author in this day's data
            for _, row in daily_data.iterrows():
                author_did = row["author_did"]
                daily_toxicity = row["average_toxicity"]
                daily_outrage = row["average_outrage"]
                daily_posts = row["total_labeled_posts"]

                # Initialize author data if not seen before
                if author_did not in author_data:
                    author_data[author_did] = {
                        "total_toxicity_weighted": 0.0,
                        "total_outrage_weighted": 0.0,
                        "total_posts": 0,
                    }

                # Add weighted contributions
                author_data[author_did]["total_toxicity_weighted"] += (
                    daily_toxicity * daily_posts
                )
                author_data[author_did]["total_outrage_weighted"] += (
                    daily_outrage * daily_posts
                )
                author_data[author_did]["total_posts"] += daily_posts

        except Exception as e:
            print(f"❌ Error processing partition date {partition_date}: {e}")
            continue

    print(f"✅ Processed data for {len(author_data)} unique authors")

    # Convert to DataFrame and calculate final weighted averages
    results = []
    for author_did, data in author_data.items():
        if data["total_posts"] > 0:
            weighted_toxicity = data["total_toxicity_weighted"] / data["total_posts"]
            weighted_outrage = data["total_outrage_weighted"] / data["total_posts"]

            results.append(
                {
                    "author_did": author_did,
                    "average_toxicity": weighted_toxicity,
                    "average_outrage": weighted_outrage,
                    "total_labeled_posts": data["total_posts"],
                }
            )

    # Create DataFrame and sort by total posts descending
    result_df = pd.DataFrame(results)

    if result_df.empty:
        print("⚠️  No data found to aggregate")
        return result_df

    result_df = result_df.sort_values(
        "total_labeled_posts", ascending=False
    ).reset_index(drop=True)

    print("📈 Final aggregation complete:")
    print(f"   - {len(result_df)} authors with data")
    print(f"   - {result_df['total_labeled_posts'].sum():,} total posts")
    print(
        f"   - Top author: {result_df.iloc[0]['author_did']} with {result_df.iloc[0]['total_labeled_posts']} posts"
    )

    return result_df


def export_aggregated_data(df: pd.DataFrame, output_dir: str) -> str:
    """
    Export aggregated data to parquet file with timestamp-based directory structure.

    Args:
        df: DataFrame to export
        output_dir: Base output directory

    Returns:
        Path to the exported file
    """
    # Create timestamp-based directory structure
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    results_dir = os.path.join(output_dir, "results", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    # Export to parquet
    filename = "aggregated_author_toxicity_outrage.parquet"
    filepath = os.path.join(results_dir, filename)

    df.to_parquet(filepath, index=False)

    print(f"💾 Exported aggregated data to: {filepath}")
    print(f"   - File size: {os.path.getsize(filepath) / 1024 / 1024:.2f} MB")

    return filepath


def main():
    """Main function to run the aggregation process."""
    print("🧪 Starting Author-to-Average Toxicity Aggregation")
    print("=" * 60)

    # Get partition dates
    partition_dates: list[str] = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )

    print(f"📅 Processing {len(partition_dates)} partition dates")
    print(f"   - Start date: {STUDY_START_DATE}")
    print(f"   - End date: {STUDY_END_DATE}")
    print(f"   - Excluded dates: {len(exclude_partition_dates)}")
    print()

    # Aggregate data across all days
    aggregated_df = aggregate_author_toxicity_across_days(partition_dates)

    if aggregated_df.empty:
        print("❌ No data found to aggregate")
        return

    # Export results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = export_aggregated_data(aggregated_df, script_dir)

    print()
    print("🎉 Aggregation completed successfully!")
    print(f"📁 Results exported to: {output_file}")
    print()
    print("📊 Summary:")
    print(f"   - Total authors: {len(aggregated_df):,}")
    print(f"   - Total posts: {aggregated_df['total_labeled_posts'].sum():,}")
    print(f"   - Average toxicity: {aggregated_df['average_toxicity'].mean():.4f}")
    print(f"   - Average outrage: {aggregated_df['average_outrage'].mean():.4f}")
    print(f"   - Top author posts: {aggregated_df.iloc[0]['total_labeled_posts']}")


if __name__ == "__main__":
    main()
