"""Helper tooling for validating Parquet files."""

import os

import pyarrow.parquet as pq

from lib.log.logger import get_logger


logger = get_logger(__name__)


def validate_pq_file(filepath: str) -> bool:
    """Validate a parquet file. Does so by attempting to read it with PyArrow."""
    try:
        pq.ParquetFile(filepath)
        return True
    except Exception as e:
        logger.error(f"Error validating {filepath}: {e}")
        return False


def validated_pq_files_within_directory(directory: str) -> list[str]:
    """Validate all Parquet files in a given directory."""
    filepaths: list[str] = []
    invalidated_filepaths: list[str] = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                full_filepath = os.path.join(root, file)
                if validate_pq_file(full_filepath):
                    filepaths.append(full_filepath)
                else:
                    invalidated_filepaths.append(full_filepath)

    total_invalidated_filepaths: int = len(invalidated_filepaths)
    if filepaths:
        logger.info(f"Found {len(filepaths)} valid Parquet files in {directory}.")
    if total_invalidated_filepaths > 0:
        logger.error(
            f"Found {total_invalidated_filepaths} invalid Parquet files in {directory}."
        )
    return filepaths
