#!/usr/bin/env python3
"""
Example script showing how to load and use a saved BERTopic model.

This script demonstrates how to:
1. Load a previously saved BERTopic model
2. Use it to get topic assignments for new documents
3. Get topic information and representative documents

Usage:
    python use_saved_model.py <model_path> <text_file>

Example:
    python use_saved_model.py results/feed_topic_model_20250902_174500 document.txt
"""

import sys
import os
from pathlib import Path

import pandas as pd

from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper
from lib.log.logger import get_logger

logger = get_logger(__name__)


def load_and_use_model(model_path: str, texts: list[str]):
    """
    Load a saved BERTopic model and use it to analyze new texts.

    Args:
        model_path: Path to the saved model directory
        texts: List of texts to analyze

    Returns:
        Dictionary with analysis results
    """
    logger.info(f"Loading model from: {model_path}")

    # Load the saved model
    bertopic = BERTopicWrapper.load_model(model_path)
    logger.info("✅ Model loaded successfully!")

    # Get topic assignments for new documents
    logger.info(f"Analyzing {len(texts)} documents...")
    topic_assignments = bertopic.get_document_topics(texts)

    # Get topic information
    topic_info = bertopic.get_topic_info()
    topics = bertopic.get_topics()

    # Create results
    results = {
        "topic_assignments": topic_assignments,
        "topic_info": topic_info,
        "topics": topics,
        "texts": texts,
    }

    # Create a DataFrame for easy viewing
    df = pd.DataFrame({"text": texts, "topic_id": topic_assignments})

    # Add topic names if available
    if "Name" in topic_info.columns:
        topic_names = topic_info.set_index("Topic")["Name"].to_dict()
        df["topic_name"] = df["topic_id"].map(topic_names)

    logger.info("✅ Analysis complete!")
    return results, df


def main():
    """Main function to demonstrate model usage."""
    if len(sys.argv) != 3:
        print("Usage: python use_saved_model.py <model_path> <text_file>")
        print(
            "Example: python use_saved_model.py results/feed_topic_model_20250902_174500 document.txt"
        )
        sys.exit(1)

    model_path = sys.argv[1]
    text_file = sys.argv[2]

    # Check if files exist
    if not os.path.exists(model_path):
        logger.error(f"Model path does not exist: {model_path}")
        sys.exit(1)

    if not os.path.exists(text_file):
        logger.error(f"Text file does not exist: {text_file}")
        sys.exit(1)

    # Read texts from file
    with open(text_file, "r", encoding="utf-8") as f:
        texts = [line.strip() for line in f if line.strip()]

    if not texts:
        logger.error("No texts found in file")
        sys.exit(1)

    logger.info(f"Loaded {len(texts)} texts from {text_file}")

    # Analyze texts
    try:
        results, df = load_and_use_model(model_path, texts)

        # Display results
        print("\n" + "=" * 80)
        print("TOPIC ANALYSIS RESULTS")
        print("=" * 80)

        print(f"\nAnalyzed {len(texts)} documents")
        print(f"Found {len(results['topic_info'])} topics")

        print("\nDocument-Topic Assignments:")
        print(df.to_string(index=False))

        print("\nTopic Information:")
        print(results["topic_info"].to_string(index=False))

        # Save results to CSV
        output_file = f"topic_analysis_results_{Path(text_file).stem}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to: {output_file}")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
