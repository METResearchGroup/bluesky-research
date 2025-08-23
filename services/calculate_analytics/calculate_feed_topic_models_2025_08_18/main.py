#!/usr/bin/env python3
"""
Main script for the Topic Modeling Pipeline.

This script runs the complete topic modeling pipeline using the LocalDataLoader
and BERTopic implementation. It loads real data, fits a BERTopic model,
displays the topics, and exports results to CSV.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from lib.log.logger import logger
from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.base import (
    DataLoader,
)
from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.config import (
    DataLoaderConfig,
)
from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.local import (
    LocalDataLoader,
)
from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.pipeline.topic_modeling import (
    TopicModelingPipeline,
)

# Study date constraints
STUDY_START_DATE = "2024-09-30"
STUDY_END_DATE = "2024-12-01"


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("topic_modeling_pipeline.log"),
        ],
    )


def validate_date_range(start_date: str, end_date: str) -> None:
    """Validate that the requested date range falls within study constraints."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        study_start = datetime.strptime(STUDY_START_DATE, "%Y-%m-%d")
        study_end = datetime.strptime(STUDY_END_DATE, "%Y-%m-%d")

        if start_dt < study_start:
            raise ValueError(
                f"Start date {start_date} is before study start date {STUDY_START_DATE}"
            )
        if end_dt > study_end:
            raise ValueError(
                f"End date {end_date} is after study end date {STUDY_END_DATE}"
            )
        if start_dt > end_dt:
            raise ValueError(
                f"Start date {start_date} must be before or equal to end date {end_date}"
            )

    except ValueError as e:
        if "before study start date" in str(e) or "after study end date" in str(e):
            raise ValueError(f"Error validating study date range: {e}")
        else:
            raise ValueError(f"Invalid date format: {e}")


def get_data_loader(loader_type: str, config: DataLoaderConfig) -> DataLoader:
    """Get the appropriate data loader based on type."""
    if loader_type == "local":
        return LocalDataLoader(service="preprocessed_posts", directory="cache")

    elif loader_type == "production":
        # This would be implemented when production infrastructure is available
        raise NotImplementedError("Production loader not yet implemented")

    else:
        raise ValueError(f"Unknown loader type: {loader_type}")


def run_topic_modeling_pipeline(
    start_date: str, end_date: str, loader_type: str = "local", verbose: bool = False
) -> dict:
    """Run the complete topic modeling pipeline."""
    setup_logging(verbose)
    logger = logging.getLogger(__name__)

    logger.info("🚀 Starting Topic Modeling Pipeline")
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    logger.info(f"🔧 Loader type: {loader_type}")

    try:
        # Initialize configuration
        config = DataLoaderConfig()
        logger.info(f"⚙️ Configuration loaded from: {config.config_path}")

        # Get data loader
        data_loader = get_data_loader(loader_type, config)
        logger.info(f"📊 Using data loader: {data_loader.name}")

        # Create pipeline
        pipeline = TopicModelingPipeline(data_loader)
        logger.info(f"🔗 Pipeline created: {pipeline.get_pipeline_info()}")

        # Load data
        logger.info("📥 Loading data...")
        df = pipeline.load_data(start_date, end_date)
        logger.info(f"✅ Loaded {len(df)} records")

        # Prepare data for BERTopic
        logger.info("🔧 Preparing data for BERTopic...")
        prepared_data = pipeline.prepare_for_bertopic()
        logger.info(f"✅ Prepared {len(prepared_data)} records for modeling")

        # Show sample data
        logger.info("📝 Sample texts:")
        for i, text in enumerate(prepared_data["text"].head(3)):
            logger.info(f"  {i+1}. {text[:80]}...")

        # Fit BERTopic model
        logger.info("🤖 Fitting BERTopic model...")
        results = pipeline.fit(start_date, end_date)
        logger.info("✅ BERTopic model fitted successfully!")

        return {
            "success": True,
            "pipeline": pipeline,
            "results": results,
            "data_count": len(prepared_data),
            "date_range": f"{start_date} to {end_date}",
        }

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise


def export_results(
    pipeline: TopicModelingPipeline, start_date: str, end_date: str
) -> None:
    """Export results to CSV files in the results folder."""
    # Create results directory
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Get topics and topic info
    topics = pipeline.get_topics()
    topic_info = pipeline.get_topic_info()
    quality_metrics = pipeline.get_quality_metrics()

    # Export topics summary
    topics_summary = []
    for topic_id, words in topics.items():
        if topic_id != -1:  # Skip outlier topic
            topic_row = topic_info[topic_info["Topic"] == topic_id]
            if not topic_row.empty:
                topics_summary.append(
                    {
                        "topic_id": topic_id,
                        "topic_name": f"Topic {topic_id}",
                        "keywords": ", ".join([word for word, _ in words[:10]]),
                        "topic_size": int(topic_row["Count"].iloc[0])
                        if "Count" in topic_row.columns
                        else 0,
                        "representative_text": topic_row["Representative_Docs"].iloc[0]
                        if "Representative_Docs" in topic_row.columns
                        else "",
                    }
                )

    topics_df = pd.DataFrame(topics_summary)
    topics_file = (
        results_dir / f"topics_summary_{start_date}_to_{end_date}_{timestamp}.csv"
    )
    topics_df.to_csv(topics_file, index=False)

    # Export full topic info
    topic_info_file = (
        results_dir / f"topic_info_{start_date}_to_{end_date}_{timestamp}.csv"
    )
    topic_info.to_csv(topic_info_file, index=False)

    # Export quality metrics
    quality_df = pd.DataFrame([quality_metrics])
    quality_file = (
        results_dir / f"quality_metrics_{start_date}_to_{end_date}_{timestamp}.csv"
    )
    quality_df.to_csv(quality_file, index=False)

    # Export pipeline summary
    summary = {
        "pipeline_run_timestamp": timestamp,
        "date_range_start": start_date,
        "date_range_end": end_date,
        "total_documents": pipeline.results["data_prepared"],
        "total_topics": len([tid for tid in topics.keys() if tid != -1]),
        "model_fitted": pipeline.fitted,
        "export_files": [str(topics_file), str(topic_info_file), str(quality_file)],
    }

    # Add performance metrics if available
    try:
        timing_breakdown = pipeline.get_timing_breakdown()
        summary.update(
            {
                "total_training_time_seconds": timing_breakdown.get("total", 0),
                "embedding_time_seconds": timing_breakdown.get("embeddings", 0),
                "clustering_time_seconds": timing_breakdown.get("clustering", 0),
                "quality_metrics_time_seconds": timing_breakdown.get(
                    "quality_metrics", 0
                ),
            }
        )
    except Exception as e:
        logger.warning(f"Could not add performance metrics to summary: {e}")

    summary_df = pd.DataFrame([summary])
    summary_file = (
        results_dir / f"pipeline_summary_{start_date}_to_{end_date}_{timestamp}.csv"
    )
    summary_df.to_csv(summary_file, index=False)

    print(f"\n📁 Results exported to: {results_dir}")
    print(f"   📊 Topics summary: {topics_file.name}")
    print(f"   📋 Topic info: {topic_info_file.name}")
    print(f"   📈 Quality metrics: {quality_file.name}")
    print(f"   📋 Pipeline summary: {summary_file.name}")


def display_topics(pipeline: TopicModelingPipeline) -> None:
    """Display the discovered topics in a formatted way."""
    topics = pipeline.get_topics()
    topic_info = pipeline.get_topic_info()
    quality_metrics = pipeline.get_quality_metrics()

    print("\n" + "=" * 80)
    print("🎯 DISCOVERED TOPICS")
    print("=" * 80)

    # Display topics
    for topic_id, words in topics.items():
        if topic_id != -1:  # Skip outlier topic
            topic_row = topic_info[topic_info["Topic"] == topic_id]
            if not topic_row.empty:
                count = (
                    int(topic_row["Count"].iloc[0])
                    if "Count" in topic_row.columns
                    else 0
                )
                print(f"🔸 Topic {topic_id}: {count} documents")
                print(f"   🏷️  Keywords: {[word for word, _ in words[:8]]}")
                if "Representative_Docs" in topic_row.columns:
                    rep_doc = topic_row["Representative_Docs"].iloc[0]
                    if pd.notna(rep_doc):
                        print(f"   📝 Representative: {str(rep_doc)[:100]}...")
                print()

    # Display quality metrics
    print("📈 QUALITY METRICS")
    print("-" * 40)
    if "c_v_mean" in quality_metrics:
        print(f"   c_v coherence: {quality_metrics['c_v_mean']:.3f}")
    if "c_npmi_mean" in quality_metrics:
        print(f"   c_npmi coherence: {quality_metrics['c_npmi_mean']:.3f}")
    if "training_time" in quality_metrics:
        print(f"   Training time: {quality_metrics['training_time']:.2f} seconds")

    print(f"\n📊 Total Topics: {len([tid for tid in topics.keys() if tid != -1])}")
    print(f"📄 Total Documents: {pipeline.results['data_prepared']}")
    print("=" * 80)


def main():
    """Main entry point for the topic modeling pipeline."""
    parser = argparse.ArgumentParser(
        description="Run the Topic Modeling Pipeline with Local Data Loader"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=STUDY_START_DATE,
        help=f"Start date for data loading (YYYY-MM-DD, default: {STUDY_START_DATE})",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=STUDY_END_DATE,
        help=f"End date for data loading (YYYY-MM-DD, default: {STUDY_END_DATE})",
    )
    parser.add_argument(
        "--loader-type",
        choices=["local", "production"],
        default="local",
        help="Type of data loader to use",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    try:
        # Validate date range against study constraints
        validate_date_range(args.start_date, args.end_date)

        print(f"📅 Study date range: {STUDY_START_DATE} to {STUDY_END_DATE}")
        print(f"📅 Requested date range: {args.start_date} to {args.end_date}")

        # Run the pipeline
        results = run_topic_modeling_pipeline(
            start_date=args.start_date,
            end_date=args.end_date,
            loader_type=args.loader_type,
            verbose=args.verbose,
        )

        # Display results
        display_topics(results["pipeline"])

        # Display performance summary if available
        try:
            performance_summary = results["pipeline"].get_performance_summary()
            print("\n" + performance_summary)
        except Exception as e:
            logger.warning(f"Could not display performance summary: {e}")

        # Export results
        export_results(results["pipeline"], args.start_date, args.end_date)

        print("\n🎉 Pipeline completed successfully!")
        print(f"📊 Processed {results['data_count']} documents")
        print(f"📅 Date range: {results['date_range']}")

    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ Date validation error: {e}")
        print(f"📅 Valid study date range: {STUDY_START_DATE} to {STUDY_END_DATE}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
