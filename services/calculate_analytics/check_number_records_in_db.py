"""Script to analyze the number of records in a database queue.

This script loads metadata from a specified queue, extracts batch size information,
and reports the total number of records along with individual batch sizes.

Usage:
    python check_number_records_in_db.py --queue input_ml_inference_perspective_api
"""

import argparse
import json
from typing import List, Tuple

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

logger = get_logger(__name__)


def parse_metadata_batch_sizes(df: pd.DataFrame) -> List[int]:
    """Extract batch sizes from metadata column.

    Args:
        df: DataFrame containing a metadata column with JSON strings

    Returns:
        List of batch sizes found in valid metadata entries
    """
    batch_sizes = []

    if df is None or "metadata" not in df.columns:
        return batch_sizes

    for metadata_str in df["metadata"]:
        try:
            metadata = json.loads(metadata_str)
            if "batch_size" in metadata:
                batch_sizes.append(metadata["batch_size"])
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse metadata: {e}")
            continue

    return batch_sizes


def analyze_queue_records(queue_name: str) -> Tuple[int, List[int]]:
    """Analyze records in the specified queue.

    Args:
        queue_name: Name of the queue to analyze

    Returns:
        Tuple containing:
        - Total number of records
        - List of batch sizes found in metadata
    """
    df = load_data_from_local_storage(service=queue_name, output_format="df")

    if df is None:
        logger.warning(f"No data found for queue: {queue_name}")
        return 0, []

    batch_sizes = parse_metadata_batch_sizes(df)
    total_records = sum(batch_sizes) if batch_sizes else 0

    return total_records, batch_sizes


def print_analysis_results(
    queue_name: str, total_records: int, batch_sizes: List[int]
) -> None:
    """Print analysis results in a formatted way.

    Args:
        queue_name: Name of the analyzed queue
        total_records: Total number of records found
        batch_sizes: List of batch sizes from metadata
    """
    # Extract integration name from queue name
    integration = queue_name.replace("input_", "").replace("output_", "")

    print(f"\nIntegration: {integration}")
    print(f"Total number of records: {total_records}")

    if batch_sizes:
        print("\nNumber of records per row:")
        for size in batch_sizes:
            print(f"- {size}")
    else:
        print("\nNo batch size information found in metadata")


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Analyze number of records in a database queue"
    )
    parser.add_argument(
        "--queue",
        required=True,
        help="Name of the queue to analyze (e.g., input_ml_inference_perspective_api)",
    )

    args = parser.parse_args()

    try:
        total_records, batch_sizes = analyze_queue_records(args.queue)
        print_analysis_results(args.queue, total_records, batch_sizes)
    except Exception as e:
        logger.error(f"Failed to analyze queue {args.queue}: {e}")
        raise


if __name__ == "__main__":
    main()
