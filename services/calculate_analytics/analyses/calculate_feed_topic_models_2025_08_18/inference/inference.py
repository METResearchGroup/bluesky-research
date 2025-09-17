"""Inference script for topic modeling - loads trained model and assigns topics.

This script loads a pre-trained BERTopic model and runs inference on documents
to assign topic labels. It can process the full dataset or new documents.
"""

import argparse
import json
import os
import time

import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.load.load_data import (
    DataLoader,
)
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.helper.topic_analysis_helpers import (
    aggregate_topic_distributions_by_slice,
    export_stratified_analysis_results,
    get_topic_evolution_analysis,
)

logger = get_logger(__name__)


def load_trained_model(model_path: str, metadata_path: str):
    """Load trained model and metadata.

    Expects the following structure:
    - model_path: Path to the model file (e.g., /path/to/timestamp/model)
    - metadata_path: Path to metadata file (e.g., /path/to/timestamp/metadata/model_metadata.json)

    Args:
        model_path: Path to trained model file
        metadata_path: Path to model metadata JSON file

    Returns:
        tuple: (bertopic_model, metadata)
    """
    logger.info(f"📂 Loading trained model from: {model_path}")

    # Use BERTopicWrapper's load_model method for proper compatibility
    # model_path already points to the model directory created by the wrapper
    bertopic = BERTopicWrapper.load_model(model_path)

    logger.info("✅ BERTopic model loaded with embedding model")

    # Load metadata
    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    logger.info("✅ Model loaded successfully")
    logger.info(
        f"📊 Model trained on {metadata['representative_docs_count']} representative documents"
    )
    logger.info(f"🏷️  Model has {metadata['total_topics']} topics")
    logger.info(
        f"📈 Training coherence: c_v={float(metadata['c_v_coherence']):.3f}, c_npmi={float(metadata['c_npmi_coherence']):.3f}"
    )

    return bertopic, metadata


def assign_topics_to_documents(bertopic, documents_df, batch_size=10000):
    """Assign topics to documents using the trained model.

    Args:
        bertopic: Trained BERTopicWrapper instance
        documents_df: DataFrame with documents to assign topics to
        batch_size: Batch size for processing

    Returns:
        list: Topic assignments for each document
    """
    logger.info(f"🎯 Assigning topics to {len(documents_df)} documents...")

    topics = []

    # Process in batches to manage memory
    for i in range(0, len(documents_df), batch_size):
        batch = documents_df.iloc[i : i + batch_size]
        batch_texts = batch["text"].tolist()

        # Use BERTopic's transform method for assignment
        batch_topics, _ = bertopic.topic_model.transform(batch_texts)
        topics.extend(batch_topics)

        if i % 50000 == 0:
            logger.info(f"Processed {i}/{len(documents_df)} documents")

    logger.info("✅ Topic assignment completed")
    return topics


def run_inference_on_exported_data(
    model_path: str, metadata_path: str, exported_data_path: str, output_dir: str = None
):
    """Run inference on pre-exported data for efficiency.

    Args:
        model_path: Path to trained model directory
        metadata_path: Path to model metadata JSON file
        exported_data_path: Path to exported data directory
        output_dir: Directory to save inference results

    Returns:
        tuple: (doc_topic_assignments, bertopic_model)
    """
    logger.info("🔄 Running inference on exported data...")
    start_time = time.time()

    # Load model and metadata
    bertopic, metadata = load_trained_model(model_path, metadata_path)

    # Load exported data
    import pandas as pd
    import json

    logger.info(f"📂 Loading exported data from: {exported_data_path}")
    documents_df = pd.read_parquet(f"{exported_data_path}/documents_df.parquet")
    uri_doc_map = pd.read_parquet(f"{exported_data_path}/uri_doc_map.parquet")

    with open(f"{exported_data_path}/date_condition_uris_map.json") as f:
        # Convert lists back to sets
        raw_map = json.load(f)
        date_condition_uris_map = {}
        for date, condition_map in raw_map.items():
            date_condition_uris_map[date] = {}
            for condition, uris in condition_map.items():
                date_condition_uris_map[date][condition] = set(uris)

    logger.info(f"✅ Exported data loaded: {len(documents_df)} documents")

    # Assign topics
    topic_assignments = assign_topics_to_documents(bertopic, documents_df)

    # Create topic assignments DataFrame
    doc_topic_assignments = pd.DataFrame(
        {"doc_id": documents_df["doc_id"], "topic_id": topic_assignments}
    )

    # Set default output directory if none provided
    if output_dir is None:
        current_dir = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )  # Go up to analysis root
        timestamp = generate_current_datetime_str()
        output_dir = os.path.join(current_dir, "results", "prod", timestamp)

    # Perform stratified analysis
    logger.info("📊 Performing stratified analysis...")
    from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.helper.topic_analysis_helpers import (
        aggregate_topic_distributions_by_slice,
    )

    slice_results = aggregate_topic_distributions_by_slice(
        date_condition_uris_map=date_condition_uris_map,
        uri_doc_map=uri_doc_map,
        doc_topic_assignments=doc_topic_assignments,
    )

    # Save results
    os.makedirs(output_dir, exist_ok=True)

    # Save topic assignments
    assignments_file = os.path.join(output_dir, "doc_topic_assignments.csv")
    doc_topic_assignments.to_csv(assignments_file, index=False)
    logger.info(f"💾 Topic assignments saved to: {assignments_file}")

    # Save slice results
    for slice_name, slice_df in slice_results.items():
        slice_file = os.path.join(output_dir, f"{slice_name}_topic_distribution.csv")
        slice_df.to_csv(slice_file, index=False)
        logger.info(f"💾 {slice_name} results saved to: {slice_file}")

    # Save metadata
    metadata_file = os.path.join(output_dir, "inference_metadata.json")
    inference_metadata = {
        "model_path": model_path,
        "metadata_path": metadata_path,
        "exported_data_path": exported_data_path,
        "inference_timestamp": generate_current_datetime_str(),
        "total_documents": len(documents_df),
        "total_slices": len(slice_results),
        "slice_names": list(slice_results.keys()),
    }

    with open(metadata_file, "w") as f:
        json.dump(inference_metadata, f, indent=2)
    logger.info(f"💾 Inference metadata saved to: {metadata_file}")

    end_time = time.time()
    logger.info(f"✅ Inference completed in {end_time - start_time:.2f} seconds")

    return doc_topic_assignments, bertopic


def run_inference_on_full_dataset(
    model_path: str, metadata_path: str, mode: str = "prod", output_dir: str = None
):
    """Run inference on full dataset using trained model.

    Args:
        model_path: Path to trained model directory
        metadata_path: Path to model metadata JSON file
        mode: Data loading mode ('local' or 'prod')
        output_dir: Directory to save inference results

    Returns:
        tuple: (doc_topic_assignments, bertopic_model)
    """
    logger.info("🔄 Running inference on full dataset...")
    start_time = time.time()

    # Load model and metadata
    bertopic, metadata = load_trained_model(model_path, metadata_path)

    # Load full dataset with inference mode (no sampling)
    try:
        dataloader = DataLoader(mode, run_mode="inference")
        date_condition_uris_map, documents_df, uri_doc_map = dataloader.load_data()
        logger.info(f"✅ Full dataset loaded: {len(documents_df)} documents")
    except Exception as e:
        logger.error(f"Failed to load full dataset: {e}")
        raise

    # Assign topics
    topic_assignments = assign_topics_to_documents(bertopic, documents_df)

    # Create topic assignments DataFrame
    doc_topic_assignments = pd.DataFrame(
        {"doc_id": documents_df["doc_id"], "topic_id": topic_assignments}
    )

    # Set default output directory if none provided
    if output_dir is None:
        current_dir = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )  # Go up to analysis root
        timestamp = generate_current_datetime_str()
        output_dir = os.path.join(current_dir, "results", mode, timestamp)

    # Perform stratified analysis if metadata available
    if "date_condition_uris_map" in metadata and "uri_doc_map" in metadata:
        logger.info("📊 Performing stratified analysis...")

        # Reconstruct data structures from metadata
        date_condition_uris_map = metadata["date_condition_uris_map"]
        uri_doc_map = pd.DataFrame(metadata["uri_doc_map"])

        # Aggregate topic distributions
        topic_distributions = aggregate_topic_distributions_by_slice(
            date_condition_uris_map, uri_doc_map, doc_topic_assignments
        )

        # Export results
        os.makedirs(output_dir, exist_ok=True)
        timestamp = generate_current_datetime_str()

        # Export stratified analysis results
        export_stratified_analysis_results(
            topic_distributions=topic_distributions,
            output_dir=output_dir,
            timestamp=timestamp,
        )

        # Topic evolution analysis
        topic_info = bertopic.get_topic_info()
        topic_evolution = get_topic_evolution_analysis(
            topic_distributions=topic_distributions, topic_info=topic_info
        )

        if not topic_evolution.empty:
            evolution_file = os.path.join(
                output_dir, f"topic_evolution_{timestamp}.csv"
            )
            topic_evolution.to_csv(evolution_file, index=False)
            logger.info(f"📈 Evolution: {os.path.basename(evolution_file)} (CSV)")

        # Export topic assignments
        assignments_file = os.path.join(
            output_dir, f"topic_assignments_{timestamp}.csv"
        )
        doc_topic_assignments.to_csv(assignments_file, index=False)
        logger.info(f"🎯 Assignments: {os.path.basename(assignments_file)} (CSV)")

        # Export topic information
        topic_file = os.path.join(output_dir, f"topics_{timestamp}.csv")
        topic_info.to_csv(topic_file, index=False)
        logger.info(f"📋 Topics: {os.path.basename(topic_file)} (CSV)")

        # Export quality metrics
        quality_metrics = bertopic.get_quality_metrics()
        metrics_file = os.path.join(output_dir, f"quality_metrics_{timestamp}.json")
        with open(metrics_file, "w") as f:
            json.dump(quality_metrics, f, indent=2, default=str)
        logger.info(f"📈 Quality: {os.path.basename(metrics_file)} (JSON)")

        logger.info(f"📊 Results exported to: {output_dir}")

    # Save inference metadata
    inference_metadata = {
        "inference_mode": mode,
        "samples_processed": len(doc_topic_assignments),
        "total_docs": len(doc_topic_assignments),
        "inference_time_seconds": time.time() - start_time,
        "total_topics": len([t for t in bertopic.get_topics().keys() if t != -1]),
        "model_path": model_path,
        "metadata_path": metadata_path,
        "inference_timestamp": generate_current_datetime_str(),
    }

    # Add topic distribution summary
    topic_counts = doc_topic_assignments["topic_id"].value_counts().to_dict()
    inference_metadata["topic_distribution"] = topic_counts

    # Save inference metadata
    metadata_dir = os.path.join(output_dir, "metadata")
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, "inference_metadata.json")
    with open(metadata_file, "w") as f:
        json.dump(inference_metadata, f, indent=2, default=str)
    logger.info(f"📋 Inference metadata saved to: {metadata_file}")

    # Export topic distribution as CSV
    topic_dist_df = pd.DataFrame(
        [
            {"topic_id": topic_id, "document_count": count}
            for topic_id, count in topic_counts.items()
        ]
    ).sort_values("topic_id")
    topic_dist_file = os.path.join(output_dir, "topic_distribution.csv")
    topic_dist_df.to_csv(topic_dist_file, index=False)
    logger.info(f"📊 Topic distribution saved to: {topic_dist_file}")

    # Export detailed topic assignments
    timestamp = generate_current_datetime_str()
    assignments_file = os.path.join(output_dir, f"topic_assignments_{timestamp}.csv")
    doc_topic_assignments.to_csv(assignments_file, index=False)
    logger.info(f"🎯 Topic assignments saved to: {assignments_file}")

    return doc_topic_assignments, bertopic


def infer_from_new_documents(
    model_path: str,
    metadata_path: str,
    new_documents_df: pd.DataFrame,
    output_dir: str = None,
):
    """Run inference on new documents (not used in training).

    Args:
        model_path: Path to trained model directory
        metadata_path: Path to model metadata JSON file
        new_documents_df: DataFrame with new documents to analyze
        output_dir: Directory to save inference results

    Returns:
        DataFrame: Results with topic assignments
    """
    logger.info("🔄 Running inference on new documents...")

    # Load model
    bertopic, metadata = load_trained_model(model_path, metadata_path)

    # Assign topics to new documents
    topic_assignments = assign_topics_to_documents(bertopic, new_documents_df)

    # Create results DataFrame
    results_df = pd.DataFrame(
        {
            "doc_id": new_documents_df["doc_id"],
            "text": new_documents_df["text"],
            "topic_id": topic_assignments,
        }
    )

    # Save results
    if output_dir is None:
        current_dir = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )  # Go up to analysis root
        timestamp = generate_current_datetime_str()
        output_dir = os.path.join(current_dir, "results", "inference", timestamp)

    os.makedirs(output_dir, exist_ok=True)
    timestamp = generate_current_datetime_str()
    results_file = os.path.join(output_dir, f"inference_results_{timestamp}.csv")
    results_df.to_csv(results_file, index=False)
    logger.info(f"💾 Results saved to: {results_file}")

    return results_df


def infer_from_file(
    model_path: str, metadata_path: str, documents_file: str, output_dir: str = None
):
    """Run inference on documents from a CSV file.

    Args:
        model_path: Path to trained model directory
        metadata_path: Path to model metadata JSON file
        documents_file: Path to CSV file with documents
        output_dir: Directory to save inference results

    Returns:
        DataFrame: Results with topic assignments
    """
    logger.info(f"📂 Loading documents from: {documents_file}")

    # Load documents from file
    documents_df = pd.read_csv(documents_file)

    # Ensure required columns exist
    if "text" not in documents_df.columns:
        raise ValueError("CSV file must contain 'text' column")

    # Add doc_id if not present
    if "doc_id" not in documents_df.columns:
        import hashlib

        documents_df["doc_id"] = documents_df["text"].apply(
            lambda x: hashlib.sha256(str(x).encode("utf-8")).hexdigest()
        )

    return infer_from_new_documents(
        model_path=model_path,
        metadata_path=metadata_path,
        new_documents_df=documents_df,
        output_dir=output_dir,
    )


def main():
    """Main inference function."""
    parser = argparse.ArgumentParser(
        description="Run topic inference using trained model"
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="Path to trained model file (e.g., /path/to/timestamp/model)",
    )
    parser.add_argument(
        "--metadata-path",
        required=True,
        help="Path to model metadata JSON file (e.g., /path/to/timestamp/metadata/model_metadata.json)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "file"],
        default="full",
        help="Inference mode: 'full' for full dataset, 'file' for CSV file",
    )
    parser.add_argument(
        "--documents-file",
        help="Path to CSV file with documents to analyze (required for 'file' mode)",
    )
    parser.add_argument(
        "--data-mode",
        choices=["local", "prod"],
        default="prod",
        help="Data loading mode for 'full' mode",
    )
    parser.add_argument(
        "--exported-data-path",
        help="Path to exported data directory (uses run_inference_on_exported_data if provided)",
    )
    parser.add_argument("--output-dir", help="Directory to save inference results")

    args = parser.parse_args()

    # Validate arguments
    if args.mode == "file" and not args.documents_file:
        raise ValueError("--documents-file is required for 'file' mode")

    try:
        if args.exported_data_path:
            # Run inference on exported data (faster)
            doc_topic_assignments, bertopic = run_inference_on_exported_data(
                model_path=args.model_path,
                metadata_path=args.metadata_path,
                exported_data_path=args.exported_data_path,
                output_dir=args.output_dir,
            )

            logger.info("🎉 Exported data inference completed successfully!")
            logger.info(f"📊 Processed {len(doc_topic_assignments)} documents")

        elif args.mode == "full":
            # Run inference on full dataset
            doc_topic_assignments, bertopic = run_inference_on_full_dataset(
                model_path=args.model_path,
                metadata_path=args.metadata_path,
                mode=args.data_mode,
                output_dir=args.output_dir,
            )

            logger.info("🎉 Full dataset inference completed successfully!")
            logger.info(f"📊 Processed {len(doc_topic_assignments)} documents")

        else:  # file mode
            # Run inference on documents from file
            results_df = infer_from_file(
                model_path=args.model_path,
                metadata_path=args.metadata_path,
                documents_file=args.documents_file,
                output_dir=args.output_dir,
            )

            logger.info("🎉 File inference completed successfully!")
            logger.info(f"📊 Processed {len(results_df)} documents")

    except Exception as e:
        logger.error(f"❌ Inference failed: {e}")
        raise


if __name__ == "__main__":
    main()
