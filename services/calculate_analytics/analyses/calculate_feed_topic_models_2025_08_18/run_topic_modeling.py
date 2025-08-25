#!/usr/bin/env python3
"""
Simple script to run BERTopic on local data for feed-level topic analysis.

This script demonstrates the principle of simplicity:
- Load data directly from local storage
- Run BERTopic using the existing wrapper
- Export results to CSV
- No unnecessary abstractions or complexity

Author: AI Agent implementing MET-44
Date: 2025-08-25
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from lib.log.logger import get_logger
from lib.db.manage_local_data import load_data_from_local_storage
from lib.constants import study_start_date, study_end_date
from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper

logger = get_logger(__name__)


def load_local_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Load preprocessed posts for topic modeling from local storage.

    Simple, direct function - no abstractions needed.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with text data ready for BERTopic

    Raises:
        ValueError: If date validation fails or no data found
    """
    logger.info(f"Loading data from {start_date} to {end_date}")

    # Simple date validation
    if start_date < study_start_date or end_date > study_end_date:
        raise ValueError(
            f"Date range must be within study period: {study_start_date} to {study_end_date}"
        )

    # Load data directly using existing function
    df = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        start_partition_date=start_date,
        end_partition_date=end_date,
    )

    # Basic validation
    if df is None or len(df) == 0:
        raise ValueError(f"No data found for {start_date} to {end_date}")

    if "text" not in df.columns:
        raise ValueError("Data must contain 'text' column")

    # Simple cleaning
    initial_count = len(df)
    df = df.dropna(subset=["text"])
    df = df[df["text"].str.strip().str.len() > 0]
    df["text"] = df["text"].astype(str)
    final_count = len(df)

    if final_count < initial_count:
        logger.info(f"Cleaned data: {initial_count} -> {final_count} documents")

    logger.info(f"✅ Loaded {len(df)} documents")
    return df


def run_bertopic_analysis(df: pd.DataFrame, config: dict = None) -> BERTopicWrapper:
    """
    Run BERTopic analysis on the loaded data.

    Uses the existing BERTopicWrapper - no need to reinvent the wheel.

    Args:
        df: DataFrame with text data
        config: Optional custom configuration

    Returns:
        Trained BERTopicWrapper instance
    """
    logger.info("🤖 Running BERTopic analysis...")

    # Use default config if none provided (follows demo.py pattern)
    if config is None:
        config = {
            "embedding_model": {
                "name": "all-MiniLM-L6-v2",
                "device": "auto",  # Will auto-detect GPU if available
                "batch_size": 32,
            },
            "bertopic": {
                "top_n_words": 20,
                "min_topic_size": 15,
                "nr_topics": "auto",
                "calculate_probabilities": True,
                "verbose": True,
            },
            "random_seed": 42,
        }

    # Initialize and train (following demo.py pattern exactly)
    bertopic = BERTopicWrapper(config_dict=config)
    bertopic.fit(df, "text")

    logger.info("✅ BERTopic analysis completed!")
    return bertopic


def export_results(
    bertopic: BERTopicWrapper,
    start_date: str,
    end_date: str,
    output_dir: str = "./results",
):
    """
    Export results to CSV files.

    Simple export - no complex abstractions needed.

    Args:
        bertopic: Trained BERTopicWrapper instance
        start_date: Start date for filename
        end_date: End date for filename
        output_dir: Output directory path
    """
    logger.info("📁 Exporting results...")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Get results from BERTopic wrapper
    topics = bertopic.get_topics()
    topic_info = bertopic.get_topic_info()
    quality_metrics = bertopic.get_quality_metrics()

    # Export topic information
    topic_file = output_path / f"topics_{start_date}_to_{end_date}_{timestamp}.csv"
    topic_info.to_csv(topic_file, index=False)

    # Export quality metrics
    metrics_file = (
        output_path / f"quality_metrics_{start_date}_to_{end_date}_{timestamp}.csv"
    )
    pd.DataFrame([quality_metrics]).to_csv(metrics_file, index=False)

    # Export summary
    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "total_documents": len(bertopic._training_results.get("texts", [])),
        "total_topics": len([t for t in topics.keys() if t != -1]),
        "training_time_seconds": quality_metrics.get("training_time", 0),
        "c_v_coherence": quality_metrics.get("c_v_mean", 0),
        "c_npmi_coherence": quality_metrics.get("c_npmi_mean", 0),
        "export_timestamp": timestamp,
    }

    summary_file = output_path / f"summary_{start_date}_to_{end_date}_{timestamp}.csv"
    pd.DataFrame([summary]).to_csv(summary_file, index=False)

    logger.info(f"📊 Results exported to {output_path}")
    logger.info(f"   📋 Topics: {topic_file.name}")
    logger.info(f"   📈 Quality: {metrics_file.name}")
    logger.info(f"   📋 Summary: {summary_file.name}")


def display_results(bertopic: BERTopicWrapper):
    """
    Display analysis results in a readable format.

    Simple display - no complex formatting needed.
    """
    topics = bertopic.get_topics()
    topic_info = bertopic.get_topic_info()
    print(topic_info)
    quality_metrics = bertopic.get_quality_metrics()

    print("\n" + "=" * 80)
    print("🎯 TOPIC MODELING RESULTS")
    print("=" * 80)

    # Display basic stats
    print(f"📊 Total Documents: {len(bertopic._training_results.get('texts', []))}")
    print(f"🏷️  Total Topics: {len([t for t in topics.keys() if t != -1])}")
    print(f"⏱️  Training Time: {quality_metrics.get('training_time', 0):.2f} seconds")

    # Display quality metrics
    if "c_v_mean" in quality_metrics:
        print(f"📈 c_v Coherence: {quality_metrics['c_v_mean']:.3f}")
    if "c_npmi_mean" in quality_metrics:
        print(f"📈 c_npmi Coherence: {quality_metrics['c_npmi_mean']:.3f}")

    # Display topics
    print("\n🔍 Discovered Topics:")
    for topic_id, words in topics.items():
        if topic_id != -1:  # Skip outlier topic
            top_words = [word for word, _ in words[:8]]
            print(f"  Topic {topic_id}: {', '.join(top_words)}")

    print("=" * 80)


def main():
    """Main function - simple and direct."""
    parser = argparse.ArgumentParser(
        description="Run BERTopic topic modeling on local data"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=study_start_date,
        help=f"Start date (YYYY-MM-DD, default: {study_start_date})",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=study_end_date,
        help=f"End date (YYYY-MM-DD, default: {study_end_date})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./results",
        help="Output directory for results (default: ./results)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        logger.info("🚀 Starting Topic Modeling Analysis")
        logger.info(f"📅 Date range: {args.start_date} to {args.end_date}")
        logger.info(f"📁 Output directory: {args.output_dir}")

        # 1. Load data (simple function call)
        df = load_local_data(args.start_date, args.end_date)

        # 2. Run BERTopic (using existing wrapper)
        bertopic = run_bertopic_analysis(df)

        # 3. Export results (simple CSV export)
        export_results(bertopic, args.start_date, args.end_date, args.output_dir)

        # 4. Display results (simple console output)
        display_results(bertopic)

        logger.info("🎉 Analysis completed successfully!")

    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
