"""Training script for topic modeling - saves model for later inference.

This script trains a BERTopic model on a representative sample of documents
and saves it for later inference on the full dataset.
"""

import argparse
import json
import os


from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from ml_tooling.topic_modeling.bertopic_wrapper import BERTopicWrapper
from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.load.load_data import (
    DataLoader,
)

logger = get_logger(__name__)


def train_bertopic_model(documents_df, force_fallback=False):
    """Train BERTopic model on the documents.

    Args:
        documents_df: DataFrame with columns [doc_id, text]
        force_fallback: Whether to force conservative configuration

    Returns:
        Trained BERTopicWrapper instance
    """
    logger.info("🤖 Training BERTopic model...")

    # Get configuration based on dataset size
    from services.calculate_analytics.analyses.calculate_feed_topic_models_2025_08_18.main import (
        get_bertopic_config,
    )

    config = get_bertopic_config(len(documents_df), force_fallback)

    try:
        logger.info("🔧 Initializing BERTopicWrapper with config")
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
        raise RuntimeError(f"BERTopic training failed: {e}")

    return bertopic


def train_and_save_model(
    mode: str = "prod",
    sample_per_day: int = 500,
    model_output_dir: str = None,
    force_fallback: bool = False,
):
    """Train topic model on representative sample and save for inference.

    Args:
        mode: Data loading mode ('local' or 'prod')
        sample_per_day: Number of documents to sample per day
        model_output_dir: Directory to save trained model
        force_fallback: Whether to force conservative configuration

    Returns:
        tuple: (model_path, metadata_path)
    """
    logger.info("🚀 Starting topic model training...")
    logger.info(f"🔧 Data loading mode: {mode}")
    logger.info(f"📊 Sample per day: {sample_per_day}")

    # Load data with training mode (sampling will be applied automatically)
    try:
        dataloader = DataLoader(mode, run_mode="train")
        date_condition_uris_map, documents_df, uri_doc_map = dataloader.load_data()
        logger.info(
            f"✅ Data loaded successfully: {len(documents_df)} documents (sampled at data loading level)"
        )
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise

    # Train model on the already-sampled data
    try:
        bertopic = train_bertopic_model(documents_df, force_fallback)
        logger.info("✅ Model training completed successfully")
    except Exception as e:
        logger.error(f"Failed to train model: {e}")
        raise

    # Save model
    if model_output_dir is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        model_output_dir = os.path.join(current_dir, "trained_models")

    os.makedirs(model_output_dir, exist_ok=True)

    try:
        model_path = bertopic.save_model_with_timestamp(
            model_output_dir, "feed_topic_model"
        )
        logger.info(f"✅ Model saved to: {model_path}")
    except Exception as e:
        logger.error(f"Failed to save model: {e}")
        raise

    # Save metadata
    try:
        quality_metrics = bertopic.get_quality_metrics()
        metadata = {
            "training_mode": mode,
            "sample_per_day": sample_per_day,
            "representative_docs_count": len(documents_df),
            "total_docs_count": len(
                documents_df
            ),  # Same as representative since sampling happens at data loading
            "training_time_seconds": quality_metrics.get("training_time", 0),
            "c_v_coherence": quality_metrics.get("c_v_mean", 0),
            "c_npmi_coherence": quality_metrics.get("c_npmi_mean", 0),
            "total_topics": len([t for t in bertopic.get_topics().keys() if t != -1]),
            "model_path": model_path,
            "training_timestamp": generate_current_datetime_str(),
            "date_condition_uris_map": date_condition_uris_map,  # Save for inference
            "uri_doc_map": uri_doc_map.to_dict("records")
            if uri_doc_map is not None
            else [],  # Convert to serializable format
        }

        metadata_path = os.path.join(
            model_output_dir, f"model_metadata_{os.path.basename(model_path)}.json"
        )
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"📋 Metadata saved to: {metadata_path}")

    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")
        raise

    # Display training results
    logger.info("📊 Training Results Summary:")
    logger.info(
        f"   📈 Total Documents: {len(documents_df)} (sampled at data loading level)"
    )
    logger.info(f"   🏷️  Topics Generated: {metadata['total_topics']}")
    logger.info(f"   ⏱️  Training Time: {metadata['training_time_seconds']:.2f} seconds")
    logger.info(f"   📊 c_v Coherence: {metadata['c_v_coherence']:.3f}")
    logger.info(f"   📊 c_npmi Coherence: {metadata['c_npmi_coherence']:.3f}")

    return model_path, metadata_path


def main():
    """Main training function."""
    parser = argparse.ArgumentParser(
        description="Train topic model and save for inference"
    )
    parser.add_argument(
        "--mode",
        choices=["local", "prod"],
        default="prod",
        help="Data loading mode: 'local' for testing, 'prod' for production data",
    )
    parser.add_argument(
        "--sample-per-day",
        type=int,
        default=500,
        help="Number of documents to sample per day for representative sample",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory to save trained model (default: ./trained_models)",
    )
    parser.add_argument(
        "--force-fallback",
        action="store_true",
        help="Force use of conservative configuration for large datasets",
    )

    args = parser.parse_args()

    try:
        model_path, metadata_path = train_and_save_model(
            mode=args.mode,
            sample_per_day=args.sample_per_day,
            model_output_dir=args.output_dir,
            force_fallback=args.force_fallback,
        )

        logger.info("🎉 Training completed successfully!")
        logger.info(f"📁 Model saved to: {model_path}")
        logger.info(f"📋 Metadata saved to: {metadata_path}")

    except Exception as e:
        logger.error(f"❌ Training failed: {e}")
        raise


if __name__ == "__main__":
    main()
