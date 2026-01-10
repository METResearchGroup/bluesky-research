"""
Data partitioning utilities for distributing input data into processable batches.

This module handles reading input data from various formats and creating
batches suitable for distributed processing.
"""

import csv
import json
import os
import uuid
from typing import Any, Iterator

import pandas as pd

from distributed_job_coordination.coordinator.models import Batch
from lib.batching_utils import create_batches
from lib.log.logger import get_logger

logger = get_logger(__name__)


class UnsupportedFormatError(Exception):
    """Exception raised for unsupported input file formats."""

    pass


class DataLoader:
    """
    Handles partitioning input data into batches for distributed processing.

    Supports multiple input formats (JSONL, CSV, Parquet) and efficient
    batch generation for large inputs.
    """

    def __init__(
        self,
        input_path: str,
        batch_size: int,
        job_id: str,
        batch_prefix: str = "batch",
    ):
        """
        Initialize partitioner with input file and batch parameters.

        Args:
            input_path: Path to input data file
            batch_size: Number of items per batch
            job_id: Job identifier for associating batches
            batch_prefix: Prefix for generated batch IDs

        Raises:
            FileNotFoundError: If input file doesn't exist
            UnsupportedFormatError: If input file format is not supported
        """
        self.input_path = input_path
        self.batch_size = batch_size
        self.job_id = job_id
        self.batch_prefix = batch_prefix

        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        self.file_format = self._detect_format()

    def _detect_format(self) -> str:
        """
        Detect input file format based on extension.

        Returns:
            String format name ('jsonl', 'csv', 'parquet', etc.)

        Raises:
            UnsupportedFormatError: If format can't be determined or is unsupported
        """
        _, ext = os.path.splitext(self.input_path)
        ext = ext.lower()

        if ext == ".jsonl" or ext == ".json":
            # For .json, we'll check if it's actually JSONL
            with open(self.input_path, "r") as f:
                first_line = f.readline().strip()
                if first_line.startswith("[") and ext == ".json":
                    # This is a JSON array, not JSONL
                    return "json"
                return "jsonl"
        elif ext == ".csv":
            return "csv"
        elif ext == ".parquet":
            return "parquet"
        else:
            raise UnsupportedFormatError(f"Unsupported input file format: {ext}")

    def read_input_data(self) -> Iterator[dict[str, Any]]:
        """
        Read and parse input data file as an iterator.

        Returns:
            Iterator over data items

        Raises:
            UnsupportedFormatError: If format can't be processed
        """
        if self.file_format == "jsonl":
            return self._read_jsonl()
        elif self.file_format == "json":
            return self._read_json()
        elif self.file_format == "csv":
            return self._read_csv()
        elif self.file_format == "parquet":
            return self._read_parquet()
        else:
            raise UnsupportedFormatError(f"Cannot read format: {self.file_format}")

    def _read_jsonl(self) -> Iterator[dict[str, Any]]:
        """Read JSONL file line by line."""
        with open(self.input_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line: {e}")

    def _read_json(self) -> Iterator[dict[str, Any]]:
        """Read JSON file with array of objects."""
        with open(self.input_path, "r") as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON file must contain an array of objects")
            for item in data:
                yield item

    def _read_csv(self) -> Iterator[dict[str, Any]]:
        """Read CSV file as dictionaries."""
        with open(self.input_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield dict(row)

    def _read_parquet(self) -> Iterator[dict[str, Any]]:
        """Read Parquet file in chunks."""
        # Use pandas to read parquet in chunks for memory efficiency
        df = pd.read_parquet(self.input_path)
        for _, row in df.iterrows():
            yield row.to_dict()

    def _generate_batch_id(self) -> str:
        """Generate a unique batch ID."""
        return f"{self.batch_prefix}-{str(uuid.uuid4())[:8]}"

    def _create_batch(self, batch_id: str, items: list[dict[str, Any]]) -> Batch:
        """
        Create a Batch object with metadata.

        Args:
            batch_id: Unique identifier for the batch
            items: List of data items for this batch

        Returns:
            Batch object
        """
        metadata = {
            "format": self.file_format,
            "source_file": self.input_path,
            "item_count": len(items),
        }

        return Batch(
            batch_id=batch_id, job_id=self.job_id, items=items, metadata=metadata
        )

    def create_batches(self) -> list[Batch]:
        """
        Create batches from input data using the helper function.

        Returns:
            List of Batch objects
        """
        # Read all items into memory
        items = list(self.read_input_data())
        logger.info(f"Read {len(items)} items from {self.input_path}")

        # Partition items.
        batched_items = create_batches(items, self.batch_size)
        logger.info(
            f"Created {len(batched_items)} batches with batch_size={self.batch_size}"
        )

        # Convert to Batch objects with metadata
        batches = []
        for i, batch_items in enumerate(batched_items):
            batch_id = f"{self.batch_prefix}-{i:06d}"
            batch_metadata = {
                "format": self.file_format,
                "source_file": self.input_path,
                "batch_size": self.batch_size,
                "actual_batch_size": len(batch_items),
            }
            batch = {
                "batch_id": batch_id,
                "batch_metadata": batch_metadata,
                "items": batch_items,
            }
            batches.append(batch)
        return batches
