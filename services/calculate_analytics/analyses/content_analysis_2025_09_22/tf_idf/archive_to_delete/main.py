"""
Main script for TF-IDF analysis of political content.

This script orchestrates the complete TF-IDF analysis pipeline, including
training, visualization, and result export. It follows the modular pattern
used in other analysis modules.

Usage:
    python main.py --mode prod --topic-model-results-path /path/to/results
    python main.py --mode local --max-features 5000
"""

import argparse
import os

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.calculate_analytics.analyses.content_analysis_2025_09_22.tf_idf.train import (
    train_tfidf_model,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.tf_idf.visualize_results import (
    create_visualizations,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()

logger = get_logger(__file__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="TF-IDF analysis of political content from topic modeling results"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="prod",
        choices=["local", "prod"],
        help="Data loading mode: 'local' for testing, 'prod' for production data",
    )
    parser.add_argument(
        "--topic-model-results-path",
        type=str,
        default=None,
        help="Path to topic modeling results directory. If None, uses default path.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for results. If None, creates timestamped directory.",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=10000,
        help="Maximum number of features for TF-IDF vectorization",
    )
    parser.add_argument(
        "--ngram-range",
        type=int,
        nargs=2,
        default=[1, 2],
        help="Range of n-grams to extract (e.g., --ngram-range 1 2)",
    )
    parser.add_argument(
        "--min-df",
        type=int,
        default=2,
        help="Minimum document frequency for terms",
    )
    parser.add_argument(
        "--max-df",
        type=float,
        default=0.95,
        help="Maximum document frequency for terms",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--skip-visualization",
        action="store_true",
        help="Skip visualization generation",
    )
    return parser.parse_args()


def do_setup(args):
    """Setup steps for TF-IDF analysis."""
    logger.info("Setting up TF-IDF analysis...")

    # Validate topic model results path if provided
    if args.topic_model_results_path and not os.path.exists(
        args.topic_model_results_path
    ):
        logger.error(
            f"Topic model results path does not exist: {args.topic_model_results_path}"
        )
        raise ValueError(
            f"Invalid topic model results path: {args.topic_model_results_path}"
        )

    # Create output directory if not provided
    if args.output_dir is None:
        timestamp = generate_current_datetime_str()
        args.output_dir = os.path.join(current_dir, "results", timestamp)

    os.makedirs(args.output_dir, exist_ok=True)
    logger.info(f"Output directory: {args.output_dir}")

    return {
        "mode": args.mode,
        "topic_model_results_path": args.topic_model_results_path,
        "output_dir": args.output_dir,
        "max_features": args.max_features,
        "ngram_range": tuple(args.ngram_range),
        "min_df": args.min_df,
        "max_df": args.max_df,
        "random_state": args.random_state,
        "skip_visualization": args.skip_visualization,
    }


def do_training(setup_config):
    """Execute TF-IDF training."""
    logger.info("Starting TF-IDF training...")

    try:
        training_output_dir = train_tfidf_model(
            mode=setup_config["mode"],
            topic_model_results_path=setup_config["topic_model_results_path"],
            output_dir=setup_config["output_dir"],
            max_features=setup_config["max_features"],
            ngram_range=setup_config["ngram_range"],
            min_df=setup_config["min_df"],
            max_df=setup_config["max_df"],
            random_state=setup_config["random_state"],
        )

        logger.info(
            f"TF-IDF training completed. Results saved in: {training_output_dir}"
        )
        return training_output_dir

    except Exception as e:
        logger.error(f"TF-IDF training failed: {e}")
        raise


def do_visualization(setup_config, training_output_dir):
    """Execute visualization generation."""
    if setup_config["skip_visualization"]:
        logger.info("Skipping visualization generation as requested")
        return

    logger.info("Starting visualization generation...")

    try:
        create_visualizations()
        logger.info("Visualization generation completed")

    except Exception as e:
        logger.error(f"Visualization generation failed: {e}")
        raise


def main():
    """Execute the complete TF-IDF analysis pipeline."""
    args = parse_arguments()

    logger.info("Starting TF-IDF analysis pipeline")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Topic model results path: {args.topic_model_results_path}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Max features: {args.max_features}")
    logger.info(f"N-gram range: {args.ngram_range}")
    logger.info(f"Min DF: {args.min_df}, Max DF: {args.max_df}")
    logger.info(f"Random state: {args.random_state}")
    logger.info(f"Skip visualization: {args.skip_visualization}")

    try:
        # Setup
        setup_config = do_setup(args)

        # Training
        training_output_dir = do_training(setup_config)

        # Visualization
        do_visualization(setup_config, training_output_dir)

        logger.info("TF-IDF analysis pipeline completed successfully")

    except Exception as e:
        logger.error(f"TF-IDF analysis pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
