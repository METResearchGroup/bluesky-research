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
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.load.load_data import (
    DataLoader,
)
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.helper.topic_analysis_helpers import (
    compute_doc_topic_assignments,
    aggregate_topic_distributions_by_slice,
    export_stratified_analysis_results,
    get_topic_evolution_analysis,
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
                    "min_topic_size": 200,  # Much larger for stability - increased from 50
                    "nr_topics": 50,  # Fixed number instead of "auto" for more manageable analysis
                    "calculate_probabilities": False,  # Disable for memory
                    "verbose": True,
                    # Conservative HDBSCAN for large datasets
                    "hdbscan_model": {
                        "min_cluster_size": 200,  # Much larger for stability - increased from 100
                        "min_samples": 100,  # Much larger for stability - increased from 50
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
                    "min_topic_size": 100,  # Increased from 15 for more stable topics
                    "nr_topics": 50,  # Fixed number instead of "auto" for manageable analysis
                    "calculate_probabilities": True,
                    "verbose": True,
                    # Conservative HDBSCAN configuration for better stability
                    "hdbscan_model": {
                        "min_cluster_size": 100,  # Increased from 20 for more stable clusters
                        "min_samples": 50,  # Increased from 10 for more stable clusters
                        "cluster_selection_method": "leaf",  # More stable than "eom"
                        "prediction_data": True,  # Explicitly enable prediction data
                        "gen_min_span_tree": True,  # Better for large datasets
                    },
                    # Conservative UMAP for better stability
                    "umap_model": {
                        "n_neighbors": 10,  # Reduced from 15 for more stability
                        "n_components": 3,  # Reduced from 5 for more stability
                        "min_dist": 0.1,  # Increased from 0.0 for more stability
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

        # Check if it's a memory issue (optional dependency)
        try:
            import psutil  # type: ignore

            memory_info = psutil.virtual_memory()
            logger.info(
                f"💾 Memory usage: {memory_info.percent}% used, {memory_info.available / (1024**3):.1f} GB available"
            )
        except Exception:
            logger.info("🔎 psutil not available; skipping memory diagnostics")

        raise RuntimeError(f"BERTopic training failed: {e}")

    return bertopic


def export_results(
    bertopic: BERTopicWrapper,
    documents_df: pd.DataFrame = None,
    date_condition_uris_map: dict = None,
    uri_doc_map: pd.DataFrame = None,
    run_stratified_analysis: bool = True,
):
    """
    Export results to files.

    - Topics: CSV format (tabular data)
    - Quality metrics: JSON format (structured data, easier to read)
    - Summary: JSON format (structured data, easier to read)
    - Stratified Analysis: CSV format (MET-46 slice-level topic distributions)

    Args:
        bertopic: Trained BERTopicWrapper instance
        documents_df: Optional documents DataFrame for stratified analysis
        date_condition_uris_map: Optional mapping for stratified analysis
        uri_doc_map: Optional URI mapping for stratified analysis
        run_stratified_analysis: Whether to compute and export stratified analysis
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

    # MET-46 Stratified Analysis
    if (
        run_stratified_analysis
        and documents_df is not None
        and date_condition_uris_map is not None
        and uri_doc_map is not None
        and len(date_condition_uris_map) > 0
    ):
        logger.info("🎯 Running MET-46 stratified topic analysis...")

        try:
            # Compute doc_id -> topic_id assignments
            doc_topic_assignments = compute_doc_topic_assignments(
                bertopic, documents_df
            )

            # Aggregate topic distributions by slice
            topic_distributions = aggregate_topic_distributions_by_slice(
                date_condition_uris_map=date_condition_uris_map,
                uri_doc_map=uri_doc_map,
                doc_topic_assignments=doc_topic_assignments,
                include_temporal_slices=True,
            )

            # Export stratified results
            export_stratified_analysis_results(
                topic_distributions=topic_distributions,
                output_dir=str(output_path),
                timestamp=timestamp,
            )

            # Topic evolution analysis
            topic_evolution = get_topic_evolution_analysis(
                topic_distributions=topic_distributions, topic_info=topic_info
            )

            if not topic_evolution.empty:
                evolution_file = os.path.join(
                    output_path, f"topic_evolution_{timestamp}.csv"
                )
                topic_evolution.to_csv(evolution_file, index=False)
                logger.info(
                    f"   📈 Evolution: {os.path.basename(evolution_file)} (CSV)"
                )

            logger.info(
                f"✅ MET-46 stratified analysis completed: {len(topic_distributions)} slices"
            )

        except Exception as e:
            logger.error(f"❌ Stratified analysis failed: {e}")
            logger.error("📝 Continuing with basic results export...")

    elif run_stratified_analysis:
        logger.info(
            "ℹ️ Skipping stratified analysis (missing required data or local mode)"
        )
    else:
        logger.info("ℹ️ Stratified analysis disabled")


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

    # New return signature: (date_condition_uris_map, documents_df, uri_doc_map)
    load_result = dataloader.load_data()
    if isinstance(load_result, tuple) and len(load_result) == 3:
        date_condition_uris_map, documents_df, uri_doc_map = load_result
    else:
        # Backward compatibility fallback (should not occur with current DataLoader)
        date_condition_uris_map, documents_df = load_result
        uri_doc_map = None

    if date_condition_uris_map is not None and len(date_condition_uris_map) > 0:
        print("Successfully loaded date_condition_uris_map for prod run.")

    logger.info(
        f"✅ Data loaded successfully: {len(documents_df)} unique documents for training"
    )

    # Apply sample size limit if specified
    if args.sample_size and len(documents_df) > args.sample_size:
        logger.warning(
            f"📊 Sampling dataset from {len(documents_df)} to {args.sample_size} documents for testing"
        )
        documents_df = documents_df.sample(
            n=args.sample_size, random_state=42
        ).reset_index(drop=True)
        logger.info(f"✅ Dataset sampled to {len(documents_df)} documents")

    # 2. Run BERTopic (using existing wrapper)
    logger.info("🤖 Step 2: Running BERTopic analysis...")

    # Auto-detect if we need fallback config for large datasets
    use_fallback = args.force_fallback or len(documents_df) > 100000
    if use_fallback:
        if args.force_fallback:
            logger.warning("⚠️ Using fallback configuration (forced via command line)")
        else:
            logger.warning(
                f"📊 Large dataset detected ({len(documents_df)} documents), will use conservative configuration"
            )

    # Train on deduplicated documents_df (columns: [doc_id, text])
    bertopic = run_bertopic_analysis(
        documents_df[["text"]].copy(), use_fallback_config=use_fallback
    )
    logger.info("✅ BERTopic analysis completed successfully")

    # 3. Export results (simple CSV export + MET-46 stratified analysis)
    logger.info("📁 Step 3: Exporting results...")
    export_results(
        bertopic=bertopic,
        documents_df=documents_df,
        date_condition_uris_map=date_condition_uris_map,
        uri_doc_map=uri_doc_map,
        run_stratified_analysis=True,
    )
    logger.info("✅ Results exported successfully")

    # 4. Display results (simple console output)
    logger.info("📊 Step 4: Displaying results...")
    display_results(bertopic)
    logger.info("✅ Results displayed successfully")

    logger.info("🎉 Analysis completed successfully!")


if __name__ == "__main__":
    main()
