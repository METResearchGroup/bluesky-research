#!/usr/bin/env python3
"""
Local Inference Runner for Topic Modeling

This script automatically finds the latest trained model in train/trained_models/local/
and runs inference on local data using the inference.py script.

Usage:
    python run_inference_local.py

The script will:
1. Find the latest model in train/trained_models/local/
2. Find the corresponding metadata file
3. Run inference.py with the correct arguments
"""

import os
import sys
import json
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_latest_model():
    """
    Find the latest trained model in train/trained_models/local/

    Returns:
        tuple: (model_path, metadata_path) or (None, None) if no model found
    """
    # Get the script directory
    script_dir = Path(__file__).parent
    train_dir = script_dir.parent / "train"
    models_dir = train_dir / "trained_models" / "local"

    if not models_dir.exists():
        logger.error(f"Models directory not found: {models_dir}")
        return None, None

    # Find all timestamp directories (they should be directories with timestamp names)
    # Timestamp format: YYYYMMDD_HHMMSS (15 characters with underscore)
    timestamp_dirs = [
        d
        for d in models_dir.iterdir()
        if d.is_dir() and len(d.name) == 15 and d.name.replace("_", "").isdigit()
    ]

    if not timestamp_dirs:
        logger.error(f"No trained models found in {models_dir}")
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

    logger.info(f"Found latest model: {model_file_path}")
    logger.info(f"Found metadata file: {metadata_path}")

    return str(model_file_path), str(metadata_path)


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


def run_inference(model_path, metadata_path):
    """
    Run the inference.py script with the correct arguments

    Args:
        model_path: Path to the trained model
        metadata_path: Path to the metadata file
    """
    # Get the script directory
    script_dir = Path(__file__).parent
    inference_script = script_dir / "inference.py"

    if not inference_script.exists():
        logger.error(f"Inference script not found: {inference_script}")
        return False

    # Build the command
    cmd = [
        sys.executable,  # Use the same Python interpreter
        str(inference_script),
        "--model-path",
        model_path,
        "--metadata-path",
        metadata_path,
        "--mode",
        "full",  # Use full mode for complete dataset
        "--data-mode",
        "local",  # Use local data
    ]

    logger.info("🚀 Starting local inference...")
    logger.info(f"Command: {' '.join(cmd)}")

    try:
        # Run the inference script
        subprocess.run(
            cmd,
            cwd=str(script_dir),  # Run from the inference directory
            capture_output=False,  # Let output go to console
            text=True,
            check=True,
        )

        logger.info("✅ Local inference completed successfully!")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Inference failed with exit code {e.returncode}")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to run inference: {e}")
        return False


def main():
    """Main function to run local inference"""
    logger.info("🔍 Local Inference Runner")
    logger.info("=" * 50)

    # Find the latest model
    model_path, metadata_path = find_latest_model()

    if not model_path or not metadata_path:
        logger.error("❌ Could not find a valid trained model")
        sys.exit(1)

    # Validate the model and metadata
    if not validate_model_and_metadata(model_path, metadata_path):
        logger.error("❌ Model or metadata validation failed")
        sys.exit(1)

    # Run inference
    success = run_inference(model_path, metadata_path)

    if success:
        logger.info("🎉 Local inference completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Local inference failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
