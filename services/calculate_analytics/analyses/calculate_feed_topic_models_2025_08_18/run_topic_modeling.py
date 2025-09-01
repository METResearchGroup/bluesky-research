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
import argparse
import json
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


def run_bertopic_analysis(
    df: pd.DataFrame, config: dict = None, use_fallback_config: bool = False
) -> BERTopicWrapper:
    """
    Run BERTopic analysis on the loaded data.

    Uses the existing BERTopicWrapper - no need to reinvent the wheel.

    Args:
        df: DataFrame with text data
        config: Optional custom configuration
        use_fallback_config: If True, use conservative settings for large datasets

    Returns:
        Trained BERTopicWrapper instance
    """
    logger.info("🤖 Running BERTopic analysis...")

    if use_fallback_config:
        logger.warning("⚠️ Using fallback configuration for large dataset...")
        logger.warning("📊 This will use more conservative HDBSCAN settings")

    # Use default config if none provided (follows demo.py pattern)
    if config is None:
        if use_fallback_config or len(df) > 100000:
            logger.warning(
                f"📊 Large dataset detected ({len(df)} documents), using conservative configuration"
            )
            config = {
                "embedding_model": {
                    "name": "all-MiniLM-L6-v2",
                    "device": "auto",
                    "batch_size": 16,  # Reduced batch size for memory
                },
                "bertopic": {
                    "top_n_words": 15,
                    "min_topic_size": 50,  # Much larger for stability
                    "nr_topics": "auto",
                    "calculate_probabilities": False,  # Disable for memory
                    "verbose": True,
                    # Conservative HDBSCAN for large datasets
                    "hdbscan_model": {
                        "min_cluster_size": 100,  # Much larger for stability
                        "min_samples": 50,  # Much larger for stability
                        "cluster_selection_method": "leaf",  # More stable for large datasets
                        "prediction_data": True,
                        "gen_min_span_tree": False,  # Disable for memory
                        "core_dist_n_jobs": 1,  # Single thread for stability
                    },
                    # Conservative UMAP for large datasets
                    "umap_model": {
                        "n_neighbors": 10,  # Reduced for stability
                        "n_components": 3,  # Fewer dimensions for stability
                        "min_dist": 0.1,  # Increased for stability
                        "metric": "cosine",
                        "random_state": 42,
                        "low_memory": True,  # Enable low memory mode
                    },
                },
                "random_seed": 42,
            }
        else:
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
                    # Fix HDBSCAN configuration for large datasets
                    "hdbscan_model": {
                        "min_cluster_size": 20,  # Increased from default
                        "min_samples": 10,  # Increased from default
                        "cluster_selection_method": "eom",  # More robust for large datasets
                        "prediction_data": True,  # Explicitly enable prediction data
                        "gen_min_span_tree": True,  # Better for large datasets
                    },
                    # Adjust UMAP for large datasets
                    "umap_model": {
                        "n_neighbors": 15,  # Reduced from default for large datasets
                        "n_components": 5,  # Reduced dimensions for stability
                        "min_dist": 0.0,
                        "metric": "cosine",
                        "random_state": 42,
                    },
                },
                "random_seed": 42,
            }

    # Initialize and train (following demo.py pattern exactly)
    logger.info(f"🔧 Initializing BERTopicWrapper with config: {config}")
    bertopic = BERTopicWrapper(config_dict=config)

    logger.info(f"📊 Dataset info: {len(df)} documents, columns: {list(df.columns)}")
    logger.info(f"📝 Text sample (first 100 chars): {df['text'].iloc[0][:100]}...")
    logger.info(
        f"🔍 Text length stats: min={df['text'].str.len().min()}, max={df['text'].str.len().max()}, mean={df['text'].str.len().mean():.1f}"
    )

    logger.info("🚀 Starting BERTopic training...")
    try:
        bertopic.fit(df, "text")
        logger.info("✅ BERTopic analysis completed successfully!")
    except Exception as e:
        logger.error(f"❌ BERTopic training failed: {e}")
        logger.error(f"🔍 Error type: {type(e).__name__}")
        logger.error(f"📚 Full error details: {str(e)}")

        # Additional debugging info
        logger.info("🔧 Attempting to diagnose the issue...")
        logger.info(f"📊 Dataset shape: {df.shape}")
        logger.info(f"📝 Text column nulls: {df['text'].isnull().sum()}")
        logger.info(f"📝 Empty text count: {(df['text'] == '').sum()}")

        # Check if it's a memory issue
        import psutil

        memory_info = psutil.virtual_memory()
        logger.info(
            f"💾 Memory usage: {memory_info.percent}% used, {memory_info.available / (1024**3):.1f} GB available"
        )

        raise RuntimeError(f"BERTopic training failed: {e}")

    return bertopic


def export_results(bertopic: BERTopicWrapper):
    """
    Export results to files.

    - Topics: CSV format (tabular data)
    - Quality metrics: JSON format (structured data, easier to read)
    - Summary: JSON format (structured data, easier to read)

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

    # Export quality metrics as JSON (easier to read)
    metrics_file = os.path.join(output_path, f"quality_metrics_{timestamp}.json")
    with open(metrics_file, "w") as f:
        json.dump(quality_metrics, f, indent=2, default=str)

    # Export summary as JSON (easier to read)
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

    summary_file = os.path.join(output_path, f"summary_{timestamp}.json")
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info(f"📊 Results exported to {output_path}")
    logger.info(f"   📋 Topics: {os.path.basename(topic_file)} (CSV)")
    logger.info(f"   📈 Quality: {os.path.basename(metrics_file)} (JSON)")
    logger.info(f"   📋 Summary: {os.path.basename(summary_file)} (JSON)")


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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run BERTopic analysis on local data")
    parser.add_argument(
        "--force-fallback",
        action="store_true",
        help="Force use of conservative configuration for large datasets",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Sample size for testing (e.g., 10000 for 10K documents)",
    )
    args = parser.parse_args()

    logger.info("🚀 Starting Topic Modeling Analysis")
    if args.force_fallback:
        logger.warning("⚠️ Force fallback configuration enabled via command line")
    if args.sample_size:
        logger.warning(
            f"⚠️ Sample size limit set to {args.sample_size} documents for testing"
        )

    # 1. Load data
    logger.info("📁 Step 1: Loading data...")
    mode = "local"
    logger.info(f"🔧 Data loading mode: {mode}")

    dataloader = DataLoader(mode)
    logger.info("📊 DataLoader initialized successfully")

    df: pd.DataFrame = dataloader.load_data()
    logger.info(f"✅ Data loaded successfully: {len(df)} documents")

    # Apply sample size limit if specified
    if args.sample_size and len(df) > args.sample_size:
        logger.warning(
            f"📊 Sampling dataset from {len(df)} to {args.sample_size} documents for testing"
        )
        df = df.sample(n=args.sample_size, random_state=42).reset_index(drop=True)
        logger.info(f"✅ Dataset sampled to {len(df)} documents")

    # 2. Run BERTopic (using existing wrapper)
    logger.info("🤖 Step 2: Running BERTopic analysis...")

    # Auto-detect if we need fallback config for large datasets
    use_fallback = args.force_fallback or len(df) > 100000
    if use_fallback:
        if args.force_fallback:
            logger.warning("⚠️ Using fallback configuration (forced via command line)")
        else:
            logger.warning(
                f"📊 Large dataset detected ({len(df)} documents), will use conservative configuration"
            )

    bertopic = run_bertopic_analysis(df, use_fallback_config=use_fallback)
    logger.info("✅ BERTopic analysis completed successfully")

    # 3. Export results (simple CSV export)
    logger.info("📁 Step 3: Exporting results...")
    export_results(bertopic)
    logger.info("✅ Results exported successfully")

    # 4. Display results (simple console output)
    logger.info("📊 Step 4: Displaying results...")
    display_results(bertopic)
    logger.info("✅ Results displayed successfully")

    logger.info("🎉 Analysis completed successfully!")


if __name__ == "__main__":
    main()
