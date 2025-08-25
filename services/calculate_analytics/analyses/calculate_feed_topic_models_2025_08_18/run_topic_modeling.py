#!/usr/bin/env python3
"""
Simple script to run BERTopic on local data for feed-level topic analysis.

This script demonstrates the principle of simplicity:
- Load data directly from local storage
- Run BERTopic using the existing wrapper
- Export results to CSV
- No unnecessary abstractions or complexity

Author: AI Agent implementing MET-44 + Mark Torres
Date: 2025-08-25
"""

import os
from pathlib import Path

import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.load_data import (
    DataLoader,
)

output_dir = os.path.join(os.path.dirname(__file__), "results")
logger = get_logger(__name__)


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


def export_results(bertopic: BERTopicWrapper):
    """
    Export results to CSV files.

    Simple export - no complex abstractions needed.

    Args:
        bertopic: Trained BERTopicWrapper instance
    """
    logger.info("📁 Exporting results...")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Generate timestamp for unique filenames
    timestamp = generate_current_datetime_str()

    # Get results from BERTopic wrapper
    topics = bertopic.get_topics()
    topic_info = bertopic.get_topic_info()
    quality_metrics = bertopic.get_quality_metrics()

    # Export topic information
    topic_file = os.path.join(output_path, f"topics_{timestamp}.csv")
    topic_info.to_csv(topic_file, index=False)

    # Export quality metrics
    metrics_file = os.path.join(output_path, f"quality_metrics_{timestamp}.csv")
    pd.DataFrame([quality_metrics]).to_csv(metrics_file, index=False)

    # Export summary
    summary = {
        "start_date": STUDY_START_DATE,
        "end_date": STUDY_END_DATE,
        "total_documents": len(bertopic._training_results.get("texts", [])),
        "total_topics": len([t for t in topics.keys() if t != -1]),
        "training_time_seconds": quality_metrics.get("training_time", 0),
        "c_v_coherence": quality_metrics.get("c_v_mean", 0),
        "c_npmi_coherence": quality_metrics.get("c_npmi_mean", 0),
        "export_timestamp": timestamp,
    }

    summary_file = os.path.join(output_path, f"summary_{timestamp}.csv")
    pd.DataFrame([summary]).to_csv(summary_file, index=False)

    logger.info(f"📊 Results exported to {output_path}")
    logger.info(f"   📋 Topics: {os.path.basename(topic_file)}")
    logger.info(f"   📈 Quality: {os.path.basename(metrics_file)}")
    logger.info(f"   📋 Summary: {os.path.basename(summary_file)}")


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
    logger.info("🚀 Starting Topic Modeling Analysis")

    # 1. Load data
    mode = "local"
    dataloader = DataLoader(mode)
    df: pd.DataFrame = dataloader.load_data()

    # 2. Run BERTopic (using existing wrapper)
    bertopic = run_bertopic_analysis(df)

    # 3. Export results (simple CSV export)
    export_results(bertopic)

    # 4. Display results (simple console output)
    display_results(bertopic)

    logger.info("🎉 Analysis completed successfully!")


if __name__ == "__main__":
    main()
