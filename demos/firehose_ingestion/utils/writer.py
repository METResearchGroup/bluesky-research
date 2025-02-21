"""Batch writer for storing records from the Bluesky firehose."""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List
import aiofiles
from ..config import settings

logger = logging.getLogger(__name__)

class BatchWriter:
    """A writer that batches records and writes them to appropriate directories."""

    def __init__(self) -> None:
        """Initialize the batch writer."""
        self._batches: Dict[str, List[Dict]] = {
            record_type: []
            for record_type in settings.RECORD_TYPES
        }
        settings.create_directories()

    def _get_filename(self, record_type: str) -> Path:
        """
        Generate a filename for the current batch of records.
        
        Args:
            record_type: The type of records in the batch.
            
        Returns:
            Path: The path where the batch should be written.
        """
        timestamp = int(time.time())
        dir_name = settings.RECORD_TYPE_DIRS[record_type]
        return settings.BASE_DIR / dir_name / f"batch_{timestamp}.json"

    async def add_record(self, record: Dict) -> None:
        """
        Add a record to its appropriate batch, writing if batch is full.
        
        Args:
            record: The record to add to a batch.
        """
        record_type = record['type']
        batch = self._batches[record_type]
        batch.append(record)
        
        if len(batch) >= settings.BATCH_SIZE:
            await self._write_batch(record_type)

    async def _write_batch(self, record_type: str) -> None:
        """
        Write a batch of records to disk.
        
        Args:
            record_type: The type of records to write.
        """
        batch = self._batches[record_type]
        if not batch:
            return

        try:
            filename = self._get_filename(record_type)
            async with aiofiles.open(filename, 'w') as f:
                await f.write(json.dumps(batch, indent=2))
            logger.info(f"Wrote {len(batch)} {record_type} records to {filename}")
            self._batches[record_type] = []
        except Exception as e:
            logger.error(f"Error writing batch for {record_type}: {e}")

    async def flush_all(self) -> None:
        """Write all remaining records in all batches."""
        for record_type in self._batches:
            await self._write_batch(record_type) 