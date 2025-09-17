#!/usr/bin/env python3
"""
Production UMAP Visualization Runner for Topic Modeling

This script automatically finds the latest trained model and exported data
and runs sliced UMAP visualizations for production results.

Usage:
    python run_umap_prod.py

The script will:
1. Find the latest model in train/trained_models/prod/
2. Find the corresponding metadata file
3. Find the exported data directory
4. Run sliced UMAP visualizations with default settings
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

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


def find_exported_data(model_path):
    """
    Look for exported data in the same directory as the model

    Args:
        model_path: Path to the trained model

    Returns:
        str or None: Path to exported data directory if found
    """
    model_dir = Path(model_path).parent
    exported_data_dir = model_dir / "exported_data"

    if (
        exported_data_dir.exists()
        and (exported_data_dir / "documents_df.parquet").exists()
    ):
        logger.info(f"Found exported data: {exported_data_dir}")
        return str(exported_data_dir)

    # Also check for sociopolitical_posts_used_in_feeds directory
    analysis_dir = Path(__file__).parent.parent
    sociopolitical_dir = analysis_dir / "sociopolitical_posts_used_in_feeds"

    if sociopolitical_dir.exists():
        # Find the latest timestamp directory
        timestamp_dirs = [
            d
            for d in sociopolitical_dir.iterdir()
            if d.is_dir() and len(d.name) == 19  # YYYY-MM-DD_HH:MM:SS format
        ]

        if timestamp_dirs:
            latest_dir = max(timestamp_dirs, key=lambda x: x.stat().st_mtime)
            if (latest_dir / "documents_df.parquet").exists():
                logger.info(f"Found exported data: {latest_dir}")
                return str(latest_dir)

    logger.error(
        "No exported data found - sliced UMAP visualizations require exported data"
    )
    return None


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


def create_default_slice_configs():
    """
    Create default slice configurations for visualization

    Returns:
        list: List of slice configurations
    """
    return [
        # Overall visualization
        {"condition": None, "date_range": None, "title_suffix": "Overall"},
        # By condition
        {"condition": "control", "date_range": None, "title_suffix": "Control"},
        {"condition": "treatment1", "date_range": None, "title_suffix": "Treatment 1"},
        {"condition": "treatment2", "date_range": None, "title_suffix": "Treatment 2"},
        # By time period (assuming 4-week study)
        {
            "condition": None,
            "date_range": "2024-10-01_to_2024-10-07",
            "title_suffix": "Week 1",
        },
        {
            "condition": None,
            "date_range": "2024-10-08_to_2024-10-14",
            "title_suffix": "Week 2",
        },
        {
            "condition": None,
            "date_range": "2024-10-15_to_2024-10-21",
            "title_suffix": "Week 3",
        },
        {
            "condition": None,
            "date_range": "2024-10-22_to_2024-10-28",
            "title_suffix": "Week 4",
        },
        # Pre/post election
        {
            "condition": None,
            "date_range": "2024-10-01_to_2024-11-04",
            "title_suffix": "Pre-Election",
        },
        {
            "condition": None,
            "date_range": "2024-11-05_to_2024-11-28",
            "title_suffix": "Post-Election",
        },
        # Condition × Time combinations
        {
            "condition": "control",
            "date_range": "2024-10-01_to_2024-10-07",
            "title_suffix": "Control Week 1",
        },
        {
            "condition": "treatment1",
            "date_range": "2024-10-01_to_2024-10-07",
            "title_suffix": "Treatment 1 Week 1",
        },
        {
            "condition": "control",
            "date_range": "2024-11-05_to_2024-11-28",
            "title_suffix": "Control Post-Election",
        },
        {
            "condition": "treatment1",
            "date_range": "2024-11-05_to_2024-11-28",
            "title_suffix": "Treatment 1 Post-Election",
        },
    ]


def run_umap_visualizations(model_path, metadata_path, exported_data_path):
    """
    Run the sliced UMAP visualization script with default configurations

    Args:
        model_path: Path to the trained model
        metadata_path: Path to the metadata file
        exported_data_path: Path to the exported data directory
    """
    # Get the script directory
    script_dir = Path(__file__).parent
    visualization_script = script_dir / "sliced_umap_visualization.py"

    if not visualization_script.exists():
        logger.error(f"Visualization script not found: {visualization_script}")
        return False

    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    output_dir = script_dir / "results" / "prod" / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get default slice configurations
    slice_configs = create_default_slice_configs()

    logger.info("🎨 Starting sliced UMAP visualizations...")
    logger.info(f"📁 Output directory: {output_dir}")
    logger.info(f"📊 Will create {len(slice_configs)} visualizations")

    try:
        # Import the SlicedUMAPVisualizer class
        sys.path.append(str(script_dir))
        from sliced_umap_visualization import SlicedUMAPVisualizer

        # Initialize visualizer
        visualizer = SlicedUMAPVisualizer(
            model_path=model_path,
            metadata_path=metadata_path,
            exported_data_path=exported_data_path,
        )

        # Load exported data
        visualizer.load_exported_data()
        logger.info("✅ Exported data loaded successfully")

        # Load model and compute topic assignments
        visualizer.load_model_and_assignments()
        logger.info("✅ Model loaded and topic assignments computed")

        # Create all visualizations efficiently using cached embeddings
        results = visualizer.create_multiple_slice_visualizations(
            slice_configs, str(output_dir)
        )

        successful_visualizations = len(results)
        logger.info(
            f"🎉 Successfully created {successful_visualizations}/{len(slice_configs)} visualizations"
        )
        logger.info(f"📁 All visualizations saved to: {output_dir}")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run UMAP visualizations: {e}")
        return False


def main():
    """Main function to run production UMAP visualizations"""
    logger.info("🎨 Production UMAP Visualization Runner")
    logger.info("=" * 50)

    # Find the latest model
    model_path, metadata_path = find_latest_prod_model()

    if not model_path or not metadata_path:
        logger.error("❌ Could not find a valid production trained model")
        sys.exit(1)

    # Validate the model and metadata
    if not validate_model_and_metadata(model_path, metadata_path):
        logger.error("❌ Model or metadata validation failed")
        sys.exit(1)

    # Find exported data
    exported_data_path = find_exported_data(model_path)

    if not exported_data_path:
        logger.error(
            "❌ Could not find exported data - sliced UMAP visualizations require exported data"
        )
        logger.error("💡 Make sure you ran training with data export enabled")
        sys.exit(1)

    # Run UMAP visualizations
    success = run_umap_visualizations(model_path, metadata_path, exported_data_path)

    if success:
        logger.info("🎉 Production UMAP visualizations completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Production UMAP visualizations failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
