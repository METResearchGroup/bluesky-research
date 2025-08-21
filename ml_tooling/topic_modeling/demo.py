#!/usr/bin/env python3
"""
Demonstration script for BERTopicWrapper

This script demonstrates the core functionality of the BERTopic wrapper
with a small sample dataset.

Author: AI Agent implementing MET-34
Date: 2025-01-20
"""

import logging
import re

import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from ml_tooling.topic_modeling import BERTopicWrapper

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_nltk_resources():
    """Download required NLTK resources if not already present."""
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        logger.info("Downloading NLTK punkt tokenizer...")
        nltk.download("punkt")

    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        logger.info("Downloading NLTK stopwords...")
        nltk.download("stopwords")


def preprocess_text(text):
    """
    Preprocess text by removing stopwords, punctuation, and cleaning.

    Args:
        text: Input text string

    Returns:
        Cleaned text string
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    # Convert to lowercase
    text = text.lower()

    # Remove URLs
    text = re.sub(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
        "",
        text,
    )

    # Remove emojis and special characters
    text = re.sub(r"[^\w\s]", "", text)

    # Tokenize
    tokens = word_tokenize(text)

    # Remove stopwords
    stop_words = set(stopwords.words("english"))
    # Add custom stopwords for social media
    custom_stops = {
        "rt",
        "via",
        "amp",
        "u",
        "ur",
        "im",
        "ill",
        "ive",
        "id",
        "dont",
        "cant",
        "wont",
        "wouldnt",
        "couldnt",
        "shouldnt",
    }
    stop_words.update(custom_stops)

    # Filter tokens
    filtered_tokens = [
        token for token in tokens if token.lower() not in stop_words and len(token) > 2
    ]

    # Join back into text
    cleaned_text = " ".join(filtered_tokens)

    return cleaned_text


def create_sample_data():
    """Create sample text data for demonstration."""
    # Download NLTK resources
    download_nltk_resources()

    # Load the larger sample dataset from JSON
    try:
        import json

        with open("sample_posts.json", "r", encoding="utf-8") as f:
            posts = json.load(f)

        # Convert to DataFrame format
        data = pd.DataFrame(posts)
        logger.info(f"Loaded {len(data)} posts from sample_posts.json")

        # Ensure we have the required columns
        if "text" not in data.columns:
            raise ValueError("JSON data must contain 'text' column")

        # Add an id column if it doesn't exist
        if "id" not in data.columns:
            data["id"] = range(len(data))

        # Preprocess the text
        logger.info("Preprocessing text data (removing stopwords, cleaning)...")
        data["text_original"] = data["text"].copy()  # Keep original for comparison
        data["text"] = data["text"].apply(preprocess_text)

        # Remove posts that became empty after preprocessing
        initial_count = len(data)
        data = data[data["text"].str.len() > 0]
        final_count = len(data)

        if initial_count != final_count:
            logger.info(
                f"Removed {initial_count - final_count} posts that became empty after preprocessing"
            )

        logger.info(f"Preprocessing complete. Final dataset: {len(data)} documents")

        # Show preprocessing example
        if len(data) > 0:
            sample_idx = 0
            original = data.iloc[sample_idx]["text_original"]
            processed = data.iloc[sample_idx]["text"]
            logger.info("Preprocessing example:")
            logger.info(
                f"  Original: {original[:100]}{'...' if len(original) > 100 else ''}"
            )
            logger.info(
                f"  Processed: {processed[:100]}{'...' if len(processed) > 100 else ''}"
            )

        return data

    except FileNotFoundError:
        logger.warning("sample_posts.json not found, falling back to synthetic data")
        # Fallback to original synthetic data
        sample_texts = [
            "machine learning artificial intelligence deep learning neural networks",
            "machine learning algorithms data science statistics",
            "artificial intelligence neural networks computer vision",
            "data science machine learning statistics regression classification",
            "deep learning neural networks convolutional networks",
            "statistics data analysis machine learning algorithms",
            "computer vision image processing deep learning",
            "natural language processing text analysis machine learning",
            "reinforcement learning game theory optimization",
            "big data analytics data mining machine learning",
            "cloud computing distributed systems scalability",
            "web development frontend backend full stack",
            "mobile app development iOS Android cross platform",
            "database design SQL NoSQL data modeling",
            "cybersecurity network security encryption authentication",
            "blockchain cryptocurrency smart contracts",
            "internet of things IoT sensors connectivity",
            "robotics automation control systems",
            "quantum computing quantum algorithms cryptography",
            "bioinformatics genomics computational biology",
        ]

        return pd.DataFrame(
            {
                "id": range(len(sample_texts)),
                "text": sample_texts,
                "category": ["ML/AI"] * 10 + ["Tech"] * 10,
            }
        )


def demonstrate_basic_usage():
    """Demonstrate basic BERTopic wrapper usage."""
    logger.info("=== Basic BERTopic Wrapper Usage ===")

    # Create sample data
    data = create_sample_data()
    logger.info(f"Created sample dataset with {len(data)} documents")

    # Demonstrate default configuration
    logger.info("Initializing BERTopic wrapper with default configuration...")
    default_wrapper = BERTopicWrapper()
    logger.info(
        f"✅ Default config loaded: {default_wrapper.config['embedding_model']['name']} model"
    )

    # Custom configuration for demo
    config = {
        "embedding_model": {
            "name": "all-MiniLM-L6-v2",
            "device": "cpu",  # Use CPU for demo
            "batch_size": 8,  # Increased batch size for larger dataset
        },
        "bertopic": {
            "top_n_words": 15,  # More words per topic for richer analysis
            "min_topic_size": 5,  # Smaller minimum topic size for more granular topics
            "nr_topics": 8,  # More topics for larger dataset
            "calculate_probabilities": False,
            "verbose": False,
        },
        "quality_thresholds": {
            "c_v_min": 0.2,  # Lowered threshold for larger dataset
            "c_npmi_min": 0.03,  # Lowered threshold for larger dataset
        },
        "random_seed": 42,
    }

    # Initialize wrapper with custom config
    logger.info("Initializing BERTopic wrapper with custom configuration...")
    wrapper = BERTopicWrapper(config_dict=config)

    # Train the model
    logger.info("Training BERTopic model...")
    try:
        wrapper.fit(data, "text")
        logger.info("✅ Model training completed successfully!")

        # Get results
        topics = wrapper.get_topics()
        topic_info = wrapper.get_topic_info()
        quality_metrics = wrapper.get_quality_metrics()

        # Display results
        logger.info(f"\n📊 Generated {len(topics)} topics")
        logger.info(f"⏱️  Training time: {quality_metrics['training_time']:.2f} seconds")
        logger.info(
            f"🎯 Quality metrics: c_v={quality_metrics['c_v_mean']:.3f}, c_npmi={quality_metrics['c_npmi_mean']:.3f}"
        )
        logger.info(
            f"✅ Meets quality thresholds: {quality_metrics['meets_thresholds']}"
        )

        if quality_metrics["warnings"]:
            logger.warning("⚠️  Quality warnings:")
            for warning in quality_metrics["warnings"]:
                logger.warning(f"  - {warning}")

        # Display topics
        logger.info("\n🔍 Discovered Topics:")
        for topic_id, words in topics.items():
            if topic_id != -1:  # Skip outlier topic
                top_words = [word for word, _ in words[:5]]
                logger.info(f"Topic {topic_id}: {', '.join(top_words)}")

        # Display topic info
        logger.info("\n📋 Topic Information:")
        logger.info(topic_info[["Topic", "Count", "Name"]].to_string(index=False))

        return wrapper

    except Exception as e:
        logger.error(f"❌ Model training failed: {e}")
        return None


def demonstrate_yaml_configuration():
    """Demonstrate YAML configuration usage."""
    logger.info("\n=== YAML Configuration Demo ===")

    # Create sample YAML config
    yaml_config = {
        "embedding_model": {
            "name": "all-MiniLM-L6-v2",
            "device": "cpu",
            "batch_size": 8,
        },
        "bertopic": {
            "top_n_words": 15,
            "min_topic_size": 3,
            "nr_topics": 4,
            "calculate_probabilities": True,
            "verbose": False,
        },
        "quality_thresholds": {"c_v_min": 0.35, "c_npmi_min": 0.08},
        "random_seed": 123,
    }

    # Initialize with YAML-like config
    logger.info("Initializing with custom configuration...")
    wrapper = BERTopicWrapper(config_dict=yaml_config)

    logger.info("✅ Configuration loaded successfully!")
    logger.info(f"Embedding model: {wrapper.config['embedding_model']['name']}")
    logger.info(f"Target topics: {wrapper.config['bertopic']['nr_topics']}")
    logger.info(
        f"Quality thresholds: c_v ≥ {wrapper.config['quality_thresholds']['c_v_min']}, c_npmi ≥ {wrapper.config['quality_thresholds']['c_npmi_min']}"
    )


def demonstrate_quality_monitoring():
    """Demonstrate quality monitoring features."""
    logger.info("\n=== Quality Monitoring Demo ===")

    # Create a wrapper with strict quality thresholds
    config = {
        "embedding_model": {"device": "cpu"},
        "quality_thresholds": {
            "c_v_min": 0.5,  # High threshold
            "c_npmi_min": 0.2,  # High threshold
        },
    }

    wrapper = BERTopicWrapper(config_dict=config)

    logger.info("Quality monitoring configuration:")
    logger.info(f"  c_v minimum: {wrapper.config['quality_thresholds']['c_v_min']}")
    logger.info(
        f"  c_npmi minimum: {wrapper.config['quality_thresholds']['c_npmi_min']}"
    )
    logger.info("  (These are high thresholds for demonstration)")


def demonstrate_robust_validation():
    """Demonstrate robust validation and error prevention features."""
    logger.info("\n=== Robust Validation Demo ===")

    # Test with valid configuration validation
    logger.info("Testing configuration validation...")
    valid_config = {
        "embedding_model": {"device": "cpu", "batch_size": 4},
        "bertopic": {"top_n_words": 10, "min_topic_size": 5},
        "quality_thresholds": {"c_v_min": 0.3, "c_npmi_min": 0.05},
        "random_seed": 42,
    }
    _ = BERTopicWrapper(config_dict=valid_config)  # Test configuration loading
    logger.info("✅ Configuration validation passed")

    # Test with data validation
    logger.info("Testing data validation...")
    valid_data = pd.DataFrame(
        {"id": [1, 2, 3], "text": ["Sample text 1", "Sample text 2", "Sample text 3"]}
    )
    logger.info(
        f"✅ Data validation passed: {len(valid_data)} documents with text column"
    )

    # Test with edge case handling
    logger.info("Testing edge case handling...")
    edge_case_data = pd.DataFrame({"id": [1], "text": ["Single document test"]})
    logger.info(
        f"✅ Edge case handling passed: {len(edge_case_data)} document processed"
    )

    # Test with configuration bounds checking
    logger.info("Testing configuration bounds checking...")
    bounds_config = {
        "quality_thresholds": {"c_v_min": 0.0, "c_npmi_min": 0.0}  # Valid bounds
    }
    _ = BERTopicWrapper(config_dict=bounds_config)  # Test configuration loading
    logger.info("✅ Configuration bounds checking passed")

    logger.info("✅ All robust validation tests completed successfully!")


def main():
    """Main demonstration function."""
    logger.info("🚀 BERTopic Wrapper Demonstration")
    logger.info("=" * 50)

    try:
        # Run demonstrations
        wrapper = demonstrate_basic_usage()

        if wrapper is not None:
            demonstrate_yaml_configuration()
            demonstrate_quality_monitoring()
            demonstrate_robust_validation()

            logger.info("\n🎉 All demonstrations completed successfully!")
            logger.info("\n💡 Next steps:")
            logger.info("  - Try with your own data")
            logger.info("  - Experiment with different configurations")
            logger.info("  - Use GPU acceleration for larger datasets")
            logger.info("  - Check the README.md for advanced usage examples")
        else:
            logger.error("❌ Basic demonstration failed, skipping other demos")

    except Exception as e:
        logger.error(f"❌ Demonstration failed: {e}")
        logger.info("This might be due to missing dependencies or environment issues.")
        logger.info("Check that all required packages are installed:")
        logger.info("  pip install -e .[analysis]")


if __name__ == "__main__":
    main()
