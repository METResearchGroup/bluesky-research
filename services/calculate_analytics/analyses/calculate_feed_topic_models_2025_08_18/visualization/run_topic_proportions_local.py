#!/usr/bin/env python3
"""
Local Topic Proportion Visualization Runner

This script automatically finds the latest local inference results and trained model,
then runs topic proportion visualization on them. Designed for quick local testing and development.

Author: AI Agent implementing Scientific Visualization Specialist
Date: 2025-01-17
"""

import subprocess
import sys
from pathlib import Path


def find_latest_model():
    """Find the latest trained model in train/trained_models/local/"""
    models_dir = Path("train/trained_models/local")

    if not models_dir.exists():
        raise FileNotFoundError(
            "No trained models found in train/trained_models/local/"
        )

    # Find timestamped directories
    timestamp_dirs = [d for d in models_dir.iterdir() if d.is_dir()]

    if not timestamp_dirs:
        raise FileNotFoundError("No timestamped model directories found")

    # Sort by timestamp (directory name) and get the latest
    latest_dir = sorted(timestamp_dirs)[-1]

    # Check for required files
    model_path = latest_dir / "model"
    metadata_path = latest_dir / "metadata" / "model_metadata.json"

    if not model_path.exists():
        raise FileNotFoundError(f"Model directory not found: {model_path}")

    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    return str(model_path), str(metadata_path)


def find_latest_results():
    """Find the latest inference results in results/local/"""
    results_dir = Path("results/local")

    if not results_dir.exists():
        raise FileNotFoundError("No inference results found in results/local/")

    # Find timestamped directories
    timestamp_dirs = [d for d in results_dir.iterdir() if d.is_dir()]

    if not timestamp_dirs:
        raise FileNotFoundError("No timestamped results directories found")

    # Sort by timestamp (directory name) and get the latest
    latest_dir = sorted(timestamp_dirs)[-1]

    return str(latest_dir)


def main():
    """Main function to run topic proportion visualization with latest local results."""
    print("📊 Local Topic Proportion Visualization Runner")
    print("=" * 60)

    try:
        # Find latest model and results
        print("🔍 Finding latest trained model...")
        model_path, metadata_path = find_latest_model()
        print(f"✅ Found model: {model_path}")
        print(f"✅ Found metadata: {metadata_path}")

        print("🔍 Finding latest inference results...")
        results_path = find_latest_results()
        print(f"✅ Found results: {results_path}")

        # Run topic proportion visualization
        print("🚀 Starting topic proportion visualization...")
        cmd = [
            sys.executable,
            "visualization/topic_proportion_visualization.py",
            "--model-path",
            model_path,
            "--metadata-path",
            metadata_path,
            "--results-path",
            results_path,
        ]

        print(f"Command: {' '.join(cmd)}")

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            print("✅ Topic proportion visualization completed successfully!")
            print("Output:")
            print(result.stdout)
        else:
            print("❌ Topic proportion visualization failed!")
            print("Error:")
            print(result.stderr)
            return 1

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
