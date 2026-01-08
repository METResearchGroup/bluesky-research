"""Training script for topic modeling - saves model for later inference.

This script trains a BERTopic model on a representative sample of documents
and saves it for later inference on the full dataset.
"""

import argparse
from datetime import datetime
import json
import os

import pandas as pd


from lib.datetime_utils import generate_current_datetime_str
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
        if mode == "prod":
            # if prod data, we want to export to local path.
            logger.info(
                f"✅ Date condition URIs map loaded successfully: {len(date_condition_uris_map)} dates x {len(date_condition_uris_map[list(date_condition_uris_map.keys())[0]])} conditions"
            )
            logger.info(f"✅ URI doc map loaded successfully: {len(uri_doc_map)} URIs")
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
        model_output_dir = os.path.join(current_dir, "trained_models", mode)

    os.makedirs(model_output_dir, exist_ok=True)

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamp_dir = os.path.join(model_output_dir, timestamp)

        # Create timestamped directory structure
        os.makedirs(timestamp_dir, exist_ok=True)

        # Save model using BERTopicWrapper's save method - it will create its own subdirectory
        model_file_path = os.path.join(timestamp_dir, "model")
        bertopic.save_model(model_file_path)
        logger.info(f"✅ Model saved to: {model_file_path}")

        # Create metadata directory for our custom metadata
        metadata_dir = os.path.join(timestamp_dir, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)

        # Update model_path to point to the timestamped directory for metadata saving
        model_path = timestamp_dir
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
        }

        metadata_path = os.path.join(model_path, "metadata", "model_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"📋 Metadata saved to: {metadata_path}")

        # Export topic information as separate CSV
        try:
            topic_info = bertopic.get_topic_info()
            topics_file = os.path.join(timestamp_dir, "topics.csv")
            topic_info.to_csv(topics_file, index=False)
            logger.info(f"🏷️  Topics exported to: {topics_file}")

            # Generate meaningful topic names using OpenAI
            try:
                logger.info("🤖 Generating topic names using OpenAI...")

                # Generate topic names using BERTopicWrapper method
                generated_names = bertopic.generate_topic_names()

                # Create DataFrame with topic names
                topic_names_df = pd.DataFrame(generated_names)
                topic_names_file = os.path.join(timestamp_dir, "topic_names.csv")
                topic_names_df.to_csv(topic_names_file, index=False)
                logger.info(f"🏷️  Generated topic names saved to: {topic_names_file}")

                # Also update the main topics.csv with generated names
                topic_info_with_names = topic_info.merge(
                    topic_names_df, left_on="Topic", right_on="topic_id", how="left"
                )
                # Replace missing names with original topic names
                topic_info_with_names["generated_name"] = topic_info_with_names[
                    "generated_name"
                ].fillna(topic_info_with_names["Name"])
                topic_info_with_names.to_csv(topics_file, index=False)
                logger.info("🏷️  Updated topics.csv with generated names")

            except Exception as e:
                logger.warning(f"Failed to generate topic names with OpenAI: {e}")
                logger.info("Continuing with original topic names...")

        except Exception as e:
            logger.warning(f"Failed to export topics: {e}")

    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")
        raise

    # Export data for efficient inference (production mode only)
    if mode == "prod":
        try:
            logger.info(
                "📦 Exporting sociopolitical posts data for efficient inference..."
            )

            # Create export directory next to the model
            model_dir = os.path.dirname(model_path)
            export_dir = os.path.join(model_dir, "exported_data")

            # Export the data using the same dataloader instance
            exported_path = dataloader.export_sociopolitical_posts_data(
                output_dir=export_dir, include_by_date_condition=True
            )

            logger.info(f"✅ Data exported successfully to: {exported_path}")
            logger.info("🚀 Exported data ready for fast inference!")

        except Exception as e:
            logger.warning(f"⚠️ Failed to export data: {e}")
            logger.warning(
                "Inference will use fresh data loading (slower but still functional)"
            )
    else:
        logger.info(
            "ℹ️ Local mode: Skipping data export (not needed for local inference)"
        )

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
