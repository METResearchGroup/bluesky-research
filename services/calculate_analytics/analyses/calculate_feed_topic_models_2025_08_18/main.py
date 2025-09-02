"""Calculate feed-level topic modeling analysis.

This analysis performs BERTopic topic modeling on Bluesky feed content to understand
topic distributions across experimental conditions and time periods.
"""

import argparse
import os
import json
import hashlib

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
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.topic_analysis_helpers import (
    compute_doc_topic_assignments,
    aggregate_topic_distributions_by_slice,
    export_stratified_analysis_results,
    get_topic_evolution_analysis,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Feed-level topic modeling analysis")
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
    parser.add_argument(
        "--mode",
        choices=["local", "prod"],
        default="prod",
        help="Data loading mode: 'local' for testing, 'prod' for production data",
    )
    return parser.parse_args()


def canonicalize_text(text: str) -> str:
    """Canonicalize text for stable hashing and deduplication."""
    if text is None:
        return ""
    return " ".join(str(text).split()).lower()


def compute_doc_id(text: str) -> str:
    """Compute stable doc_id from text."""
    canon = canonicalize_text(text)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def get_bertopic_config(dataset_size: int, force_fallback: bool = False) -> dict:
    """Get BERTopic configuration based on dataset size and requirements.

    Args:
        dataset_size: Number of documents in the dataset
        force_fallback: Whether to force conservative configuration

    Returns:
        Configuration dictionary for BERTopicWrapper
    """
    use_fallback = force_fallback or dataset_size > 100000

    if use_fallback:
        if force_fallback:
            logger.warning("⚠️ Using fallback configuration (forced via command line)")
        else:
            logger.warning(
                f"📊 Large dataset detected ({dataset_size} documents), using conservative configuration"
            )
        config = {
            "embedding_model": {
                "name": "all-MiniLM-L6-v2",
                "device": "auto",
                "batch_size": 16,  # Reduced batch size for memory
            },
            "bertopic": {
                "top_n_words": 15,
                "min_topic_size": 200,  # Much larger for stability
                "nr_topics": 50,  # Fixed number for manageable analysis
                "calculate_probabilities": False,  # Disable for memory
                "verbose": True,
                # Conservative HDBSCAN for large datasets
                "hdbscan_model": {
                    "min_cluster_size": 200,  # Much larger for stability
                    "min_samples": 100,  # Much larger for stability
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

    return config


def train_bertopic_model(
    documents_df: pd.DataFrame, force_fallback: bool = False
) -> BERTopicWrapper:
    """Train BERTopic model on the documents.

    Args:
        documents_df: DataFrame with columns [doc_id, text]
        force_fallback: Whether to force conservative configuration

    Returns:
        Trained BERTopicWrapper instance
    """
    logger.info("🤖 Training BERTopic model...")

    # Get configuration based on dataset size
    config = get_bertopic_config(len(documents_df), force_fallback)

    try:
        logger.info(f"🔧 Initializing BERTopicWrapper with config: {config}")
        bertopic = BERTopicWrapper(config_dict=config)

        logger.info(
            f"📊 Dataset info: {len(documents_df)} documents, columns: {list(documents_df.columns)}"
        )
        logger.info(
            f"📝 Text sample (first 100 chars): {documents_df['text'].iloc[0][:100]}..."
        )
        logger.info(
            f"🔍 Text length stats: min={documents_df['text'].str.len().min()}, max={documents_df['text'].str.len().max()}, mean={documents_df['text'].str.len().mean():.1f}"
        )

        logger.info("🚀 Starting BERTopic training...")
        bertopic.fit(documents_df, "text")
        logger.info("✅ BERTopic training completed successfully!")

    except Exception as e:
        logger.error(f"❌ BERTopic training failed: {e}")
        logger.error(f"🔍 Error type: {type(e).__name__}")
        logger.error(f"📚 Full error details: {str(e)}")

        # Additional debugging info
        logger.info("🔧 Attempting to diagnose the issue...")
        logger.info(f"📊 Dataset shape: {documents_df.shape}")
        logger.info(f"📝 Text column nulls: {documents_df['text'].isnull().sum()}")
        logger.info(f"📝 Empty text count: {(documents_df['text'] == '').sum()}")

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


def do_setup(mode: str = "prod", sample_size: int = None):
    """Setup steps for topic modeling analysis.

    Args:
        mode: Data loading mode ('local' or 'prod')
        sample_size: Optional sample size for testing

    Returns:
        Dictionary containing loaded data
    """
    logger.info("📁 Step 1: Loading data...")
    logger.info(f"🔧 Data loading mode: {mode}")

    try:
        # Use DataLoader for both local and production modes
        dataloader = DataLoader(mode)
        logger.info("📊 DataLoader initialized successfully")

        # Load data using DataLoader
        load_result = dataloader.load_data()
        if isinstance(load_result, tuple) and len(load_result) == 3:
            date_condition_uris_map, documents_df, uri_doc_map = load_result
        else:
            # Backward compatibility fallback (should not occur with current DataLoader)
            date_condition_uris_map, documents_df = load_result
            uri_doc_map = None

        if date_condition_uris_map is not None and len(date_condition_uris_map) > 0:
            logger.info("Successfully loaded date_condition_uris_map for prod run.")

        # Apply sample size limit if specified
        if sample_size and len(documents_df) > sample_size:
            logger.warning(
                f"📊 Sampling dataset from {len(documents_df)} to {sample_size} documents for testing"
            )
            documents_df = documents_df.sample(
                n=sample_size, random_state=42
            ).reset_index(drop=True)
            logger.info(f"✅ Dataset sampled to {len(documents_df)} documents")

        logger.info(
            f"✅ Data loaded successfully: {len(documents_df)} unique documents for training"
        )

    except Exception as e:
        logger.error(f"Failed to load topic modeling data: {e}")
        raise

    return {
        "documents_df": documents_df,
        "date_condition_uris_map": date_condition_uris_map,
        "uri_doc_map": uri_doc_map,
    }


def do_analysis_and_export_results(
    documents_df: pd.DataFrame,
    date_condition_uris_map: dict[str, dict[str, set[str]]],
    uri_doc_map: pd.DataFrame,
    force_fallback: bool = False,
):
    """Perform topic modeling analysis and export results.

    Args:
        documents_df: DataFrame with documents for training
        date_condition_uris_map: Mapping of dates to conditions to post URIs
        uri_doc_map: Mapping of URIs to document IDs
        force_fallback: Whether to force conservative configuration
    """
    # Generate timestamp for unique filenames
    timestamp = generate_current_datetime_str()

    # Create output directory
    output_dir = os.path.join(current_dir, "results")
    os.makedirs(output_dir, exist_ok=True)

    # 1. Train BERTopic model
    logger.info("🤖 Step 2: Training BERTopic model...")
    try:
        bertopic = train_bertopic_model(documents_df, force_fallback)
    except Exception as e:
        logger.error(f"Failed to train BERTopic model: {e}")
        raise

    # 2. Compute topic assignments
    logger.info("🎯 Computing topic assignments...")
    try:
        doc_topic_assignments = compute_doc_topic_assignments(bertopic, documents_df)
    except Exception as e:
        logger.error(f"Failed to compute topic assignments: {e}")
        raise

    # 3. Perform stratified analysis
    logger.info("📊 Performing stratified analysis...")
    try:
        topic_distributions = aggregate_topic_distributions_by_slice(
            date_condition_uris_map, uri_doc_map, doc_topic_assignments
        )
    except Exception as e:
        logger.error(f"Failed to perform stratified analysis: {e}")
        raise

    # 4. Export results
    logger.info("📁 Step 3: Exporting results...")
    try:
        # Get results from BERTopic wrapper
        topics = bertopic.get_topics()
        topic_info = bertopic.get_topic_info()
        quality_metrics = bertopic.get_quality_metrics()

        # Export topic information
        topic_file = os.path.join(output_dir, f"topics_{timestamp}.csv")
        topic_info.to_csv(topic_file, index=False)
        logger.info(f"📋 Topics: {os.path.basename(topic_file)} (CSV)")

        # Export quality metrics as JSON (easier to read)
        metrics_file = os.path.join(output_dir, f"quality_metrics_{timestamp}.json")
        with open(metrics_file, "w") as f:
            json.dump(quality_metrics, f, indent=2, default=str)
        logger.info(f"📈 Quality: {os.path.basename(metrics_file)} (JSON)")

        # Export summary as JSON (easier to read)
        summary = {
            "start_date": STUDY_START_DATE,
            "end_date": STUDY_END_DATE,
            "total_documents": len(documents_df),
            "total_topics": len([t for t in topics.keys() if t != -1]),
            "training_time_seconds": quality_metrics.get("training_time", 0),
            "c_v_coherence": quality_metrics.get("c_v_mean", 0),
            "c_npmi_coherence": quality_metrics.get("c_npmi_mean", 0),
            "export_timestamp": timestamp,
        }

        summary_file = os.path.join(output_dir, f"summary_{timestamp}.json")
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"📋 Summary: {os.path.basename(summary_file)} (JSON)")

        # Export stratified analysis results
        export_stratified_analysis_results(
            topic_distributions=topic_distributions,
            output_dir=output_dir,
            timestamp=timestamp,
        )

        # Topic evolution analysis
        topic_evolution = get_topic_evolution_analysis(
            topic_distributions=topic_distributions, topic_info=topic_info
        )

        if not topic_evolution.empty:
            evolution_file = os.path.join(
                output_dir, f"topic_evolution_{timestamp}.csv"
            )
            topic_evolution.to_csv(evolution_file, index=False)
            logger.info(f"📈 Evolution: {os.path.basename(evolution_file)} (CSV)")

        logger.info(f"📊 Results exported to {output_dir}")
        logger.info(
            f"✅ MET-46 stratified analysis completed: {len(topic_distributions)} slices"
        )

    except Exception as e:
        logger.error(f"Failed to export results: {e}")
        raise

    # 5. Display results
    logger.info("📊 Step 4: Displaying results...")
    try:
        display_results(bertopic, documents_df, quality_metrics, topics)
        logger.info("✅ Results displayed successfully")
    except Exception as e:
        logger.error(f"Failed to display results: {e}")
        raise


def display_results(
    bertopic: BERTopicWrapper,
    documents_df: pd.DataFrame,
    quality_metrics: dict,
    topics: dict,
):
    """Display analysis results in a readable format."""
    print("\n" + "=" * 80)
    print("🎯 TOPIC MODELING RESULTS")
    print("=" * 80)

    # Display basic stats
    print(f"📊 Total Documents: {len(documents_df)}")
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
    """Execute the topic modeling analysis."""
    # Parse command line arguments
    args = parse_arguments()

    logger.info("🚀 Starting Topic Modeling Analysis")
    if args.force_fallback:
        logger.warning("⚠️ Force fallback configuration enabled via command line")
    if args.sample_size:
        logger.warning(
            f"⚠️ Sample size limit set to {args.sample_size} documents for testing"
        )

    try:
        setup_objs = do_setup(mode=args.mode, sample_size=args.sample_size)

        documents_df: pd.DataFrame = setup_objs["documents_df"]
        date_condition_uris_map: dict[str, dict[str, set[str]]] = setup_objs[
            "date_condition_uris_map"
        ]
        uri_doc_map: pd.DataFrame = setup_objs["uri_doc_map"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        do_analysis_and_export_results(
            documents_df=documents_df,
            date_condition_uris_map=date_condition_uris_map,
            uri_doc_map=uri_doc_map,
            force_fallback=args.force_fallback,
        )
    except Exception as e:
        logger.error(f"Failed to do analysis and export results: {e}")
        raise

    logger.info("🎉 Analysis completed successfully!")


if __name__ == "__main__":
    main()
