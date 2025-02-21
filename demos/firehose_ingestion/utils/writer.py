"""Batch writer for storing records from the Bluesky firehose using queues."""

import json
import logging
from typing import Dict, List
from lib.db.queue import Queue
from lib.db.queue_constants import NAME_TO_QUEUE_NAME_MAP

from demos.firehose_ingestion.config import settings

logger = logging.getLogger(__name__)

# Add our queue names to the mapping
for record_type in settings.RECORD_TYPES:
    queue_name = f"sync_firehose_{settings.RECORD_TYPE_DIRS[record_type]}"
    NAME_TO_QUEUE_NAME_MAP[queue_name] = queue_name

class BatchWriter:
    """A writer that batches records and writes them to appropriate queues."""

    def __init__(self) -> None:
        """Initialize the batch writer with queues for each record type."""
        self._batches = {
            record_type: {"created": [], "deleted": []}
            for record_type in settings.RECORD_TYPE_DIRS.values()
        }
        self._queues: Dict[str, Queue] = {}
        self._initialize_queues()

    def _initialize_queues(self) -> None:
        """Initialize a queue for each record type."""
        for record_type in settings.RECORD_TYPES:
            queue_name = f"sync_firehose_{settings.RECORD_TYPE_DIRS[record_type]}"
            logger.info(f"Initializing queue: {queue_name}")
            self._queues[record_type] = Queue(queue_name, create_new_queue=True)

    async def add_records(self, operations: Dict) -> None:
        """
        Add records to their appropriate batches, writing to queue if batch is full.
        
        Args:
            operations: Dictionary of operations by record type
        """
        for record_type, ops in operations.items():
            created_records = ops["created"]
            deleted_records = ops["deleted"]
            
            if created_records:
                self._batches[record_type]["created"].extend(created_records)
                if len(self._batches[record_type]["created"]) >= settings.BATCH_SIZE:
                    await self._write_batch(record_type, "created")
            
            if deleted_records:
                self._batches[record_type]["deleted"].extend(deleted_records)
                if len(self._batches[record_type]["deleted"]) >= settings.BATCH_SIZE:
                    await self._write_batch(record_type, "deleted")

    async def _write_batch(self, record_type: str, operation_type: str) -> None:
        """
        Write a batch of records to the appropriate queue.
        
        Args:
            record_type: The type of records to write
            operation_type: Either "created" or "deleted"
        """
        batch = self._batches[record_type][operation_type]
        if not batch:
            return

        try:
            # Find the corresponding record type in RECORD_TYPES
            queue_type = next(
                rt for rt, dir_name in settings.RECORD_TYPE_DIRS.items()
                if dir_name == record_type
            )
            queue = self._queues[queue_type]
            
            # Add metadata about the batch
            metadata = {
                "source": "firehose_ingestion",
                "record_type": record_type,
                "operation_type": operation_type,
                "batch_size": len(batch)
            }
            
            queue.batch_add_items_to_queue(
                items=batch,
                metadata=metadata,
                batch_size=settings.BATCH_SIZE
            )
            logger.info(f"Wrote {len(batch)} {operation_type} {record_type} records to queue")
            self._batches[record_type][operation_type] = []
        except Exception as e:
            logger.error(f"Error writing {operation_type} batch for {record_type} to queue: {e}")

    async def flush_all(self) -> None:
        """Write all remaining records in all batches to their queues."""
        for record_type in self._batches:
            await self._write_batch(record_type, "created")
            await self._write_batch(record_type, "deleted") 