#!/usr/bin/env python3
"""
Production Topic Proportion Visualization Runner

This script automatically finds the latest trained model and inference results
and runs structured topic proportion visualizations for production results.

Usage:
    python run_topic_proportions_prod.py

The script will:
1. Find the latest model in train/trained_models/prod/
2. Find the corresponding metadata file
3. Find the latest inference results
4. Run structured topic proportion visualizations with consistent top 10 topics
"""

import os
import sys
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_latest_prod_model():
    """
    Find the latest trained model in train/trained_models/prod/

    Returns:
        tuple: (model_path, metadata_path) or (None, None) if no model found
    """
    # Get the script directory
    script_dir = Path(__file__).parent
    train_dir = script_dir.parent / "train"
    models_dir = train_dir / "trained_models" / "prod"

    if not models_dir.exists():
        logger.error(f"Production models directory not found: {models_dir}")
        return None, None

    # Find all timestamp directories (they should be directories with timestamp names)
    # Timestamp format: YYYYMMDD_HHMMSS (15 characters with underscore)
    timestamp_dirs = [
        d
        for d in models_dir.iterdir()
        if d.is_dir() and len(d.name) == 15 and d.name.replace("_", "").isdigit()
    ]

    if not timestamp_dirs:
        logger.error(f"No production trained models found in {models_dir}")
        return None, None

    # Sort by modification time to get the latest
    latest_timestamp_dir = max(timestamp_dirs, key=lambda x: x.stat().st_mtime)

    # Check if the model file exists in the timestamp directory
    model_file_path = latest_timestamp_dir / "model"
    if not model_file_path.exists():
        logger.error(f"Model file not found: {model_file_path}")
        return None, None

    # Find the metadata file in the metadata subdirectory
    metadata_path = latest_timestamp_dir / "metadata" / "model_metadata.json"

    if not metadata_path.exists():
        logger.error(f"Metadata file not found: {metadata_path}")
        return None, None

    logger.info(f"Found latest production model: {model_file_path}")
    logger.info(f"Found metadata file: {metadata_path}")

    return str(model_file_path), str(metadata_path)


def find_latest_inference_results():
    """
    Find the latest inference results in inference/results/prod/

    Returns:
        str or None: Path to latest inference results directory if found
    """
    # Get the script directory
    script_dir = Path(__file__).parent
    inference_dir = script_dir.parent / "inference"
    results_dir = inference_dir / "results" / "prod"

    if not results_dir.exists():
        logger.error(f"Production inference results directory not found: {results_dir}")
        return None

    # Find all timestamp directories
    timestamp_dirs = [
        d
        for d in results_dir.iterdir()
        if d.is_dir() and len(d.name) == 19  # YYYY-MM-DD-HH:MM:SS format
    ]

    if not timestamp_dirs:
        logger.error(f"No production inference results found in {results_dir}")
        return None

    # Sort by modification time to get the latest
    latest_dir = max(timestamp_dirs, key=lambda x: x.stat().st_mtime)

    logger.info(f"Found latest inference results: {latest_dir}")
    return str(latest_dir)


def validate_model_and_metadata(model_path, metadata_path):
    """
    Validate that the model and metadata files exist and are readable

    Args:
        model_path: Path to the model directory
        metadata_path: Path to the metadata JSON file

    Returns:
        bool: True if valid, False otherwise
    """
    if not os.path.exists(model_path):
        logger.error(f"Model path does not exist: {model_path}")
        return False

    if not os.path.exists(metadata_path):
        logger.error(f"Metadata path does not exist: {metadata_path}")
        return False

    # Try to read the metadata file
    try:
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        logger.info(
            f"Metadata loaded successfully. Training mode: {metadata.get('training_mode', 'unknown')}"
        )
        return True
    except Exception as e:
        logger.error(f"Failed to read metadata file: {e}")
        return False


def run_topic_proportion_visualizations(model_path, metadata_path, results_path):
    """
    Run the topic proportion visualization script

    Args:
        model_path: Path to the trained model
        metadata_path: Path to the metadata file
        results_path: Path to the inference results
    """
    # Get the script directory
    script_dir = Path(__file__).parent
    visualization_script = script_dir / "topic_proportion_visualization.py"

    if not visualization_script.exists():
        logger.error(f"Visualization script not found: {visualization_script}")
        return False

    logger.info("📊 Starting topic proportion visualizations...")

    try:
        # Import the TopicProportionVisualizer class
        sys.path.append(str(script_dir))
        from topic_proportion_visualization import TopicProportionVisualizer

        # Initialize visualizer
        visualizer = TopicProportionVisualizer(
            model_path=model_path,
            metadata_path=metadata_path,
            results_path=results_path,
        )

        # Run visualization
        output_dir = visualizer.run_visualization()

        logger.info("✅ Topic proportion visualizations completed successfully!")
        logger.info(f"📁 All visualizations saved to: {output_dir}")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run topic proportion visualizations: {e}")
        return False


def main():
    """Main function to run production topic proportion visualizations"""
    logger.info("📊 Production Topic Proportion Visualization Runner")
    logger.info("=" * 65)

    # Find the latest model
    model_path, metadata_path = find_latest_prod_model()

    if not model_path or not metadata_path:
        logger.error("❌ Could not find a valid production trained model")
        sys.exit(1)

    # Validate the model and metadata
    if not validate_model_and_metadata(model_path, metadata_path):
        logger.error("❌ Model or metadata validation failed")
        sys.exit(1)

    # Find latest inference results
    results_path = find_latest_inference_results()

    if not results_path:
        logger.error("❌ Could not find inference results")
        sys.exit(1)

    # Run topic proportion visualizations
    success = run_topic_proportion_visualizations(
        model_path, metadata_path, results_path
    )

    if success:
        logger.info(
            "🎉 Production topic proportion visualizations completed successfully!"
        )
        sys.exit(0)
    else:
        logger.error("❌ Production topic proportion visualizations failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
