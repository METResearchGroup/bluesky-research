"""Batch writer for storing records from the Bluesky firehose using queues."""

import json
import logging
from typing import Dict, List
from lib.db.queue import Queue

from ..config import settings

logger = logging.getLogger(__name__)

class BatchWriter:
    """A writer that batches records and writes them to appropriate queues."""

    def __init__(self) -> None:
        """Initialize the batch writer with queues for each record type."""
        self._batches: Dict[str, List[Dict]] = {
            record_type: []
            for record_type in settings.RECORD_TYPES
        }
        self._queues: Dict[str, Queue] = {}
        self._initialize_queues()

    def _initialize_queues(self) -> None:
        """Initialize a queue for each record type."""
        for record_type in settings.RECORD_TYPES:
            queue_name = f"sync_firehose_{settings.RECORD_TYPE_DIRS[record_type]}"
            logger.info(f"Initializing queue: {queue_name}")
            self._queues[record_type] = Queue(queue_name, create_new_queue=True)

    async def add_record(self, record: Dict) -> None:
        """
        Add a record to its appropriate batch, writing to queue if batch is full.
        
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
        Write a batch of records to the appropriate queue.
        
        Args:
            record_type: The type of records to write.
        """
        batch = self._batches[record_type]
        if not batch:
            return

        try:
            queue = self._queues[record_type]
            # Add metadata about the batch
            metadata = {
                "source": "firehose_ingestion",
                "record_type": record_type,
                "batch_size": len(batch)
            }
            queue.batch_add_items_to_queue(
                items=batch,
                metadata=metadata,
                batch_size=settings.BATCH_SIZE
            )
            logger.info(f"Wrote {len(batch)} {record_type} records to queue")
            self._batches[record_type] = []
        except Exception as e:
            logger.error(f"Error writing batch for {record_type} to queue: {e}")

    async def flush_all(self) -> None:
        """Write all remaining records in all batches to their queues."""
        for record_type in self._batches:
            await self._write_batch(record_type) 