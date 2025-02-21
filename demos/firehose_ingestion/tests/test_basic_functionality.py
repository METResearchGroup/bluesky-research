"""Basic functionality tests for the firehose ingestion system."""

import asyncio
import json
import os
import pytest
from typing import Dict, List
from rich.console import Console
from rich.table import Table
from lib.db.queue import Queue

from ..utils import FirehoseSubscriber, BatchWriter
from ..config import settings

console = Console()

class TestResults:
    def __init__(self):
        self.results = []

    def add_result(self, test_name: str, criteria: str, passed: bool):
        self.results.append({
            'test_name': test_name,
            'criteria': criteria,
            'passed': passed
        })

    def display_results(self):
        table = Table(title="Firehose Ingestion Test Results")
        table.add_column("Test Name", style="cyan")
        table.add_column("Criteria", style="magenta")
        table.add_column("Passed", style="green")

        for result in self.results:
            table.add_row(
                result['test_name'],
                result['criteria'],
                "✓" if result['passed'] else "✗"
            )

        console.print(table)

test_results = TestResults()

@pytest.fixture
async def subscriber():
    return FirehoseSubscriber()

@pytest.fixture
async def writer():
    return BatchWriter()

async def test_queue_creation():
    """Test queue creation and initialization."""
    writer = BatchWriter()
    
    # Check queue creation
    for record_type in settings.RECORD_TYPES:
        queue_name = f"sync_firehose_{settings.RECORD_TYPE_DIRS[record_type]}"
        queue = writer._queues[record_type]
        
        # Verify queue exists
        test_results.add_result(
            f"Queue Creation - {record_type}",
            "Queue should be created with correct name",
            isinstance(queue, Queue) and queue.queue_name == queue_name
        )
        
        # Verify SQLite file exists
        test_results.add_result(
            f"Queue File - {record_type}",
            "SQLite database file should exist",
            os.path.exists(queue.db_path)
        )

async def test_record_processing(subscriber: FirehoseSubscriber):
    """Test record processing and classification."""
    processed_records = []
    record_count = 0
    type_counts = {record_type: 0 for record_type in settings.RECORD_TYPES}
    
    async for record in subscriber.subscribe():
        processed_records.append(record)
        record_count += 1
        type_counts[record['type']] += 1
        
        if record_count >= 100:  # Test with 100 records
            break
    
    # Check record processing
    test_results.add_result(
        "Record Processing",
        "Should process at least 100 records",
        record_count >= 100
    )
    
    # Check record type classification
    for record_type in settings.RECORD_TYPES:
        test_results.add_result(
            f"Record Classification - {record_type}",
            f"Should have records of type {record_type}",
            type_counts[record_type] > 0
        )

async def test_batch_operations(writer: BatchWriter):
    """Test batch writing operations."""
    # Create test records
    test_records = []
    for i in range(2000):  # Test with 2000 records
        record_type = settings.RECORD_TYPES[i % len(settings.RECORD_TYPES)]
        test_records.append({
            'type': record_type,
            'uri': f'test_uri_{i}',
            'cid': f'test_cid_{i}',
            'author': 'test_author',
            'record': {'test': 'data'}
        })
    
    # Write records
    for record in test_records:
        await writer.add_record(record)
    
    # Verify batch sizes
    for record_type in settings.RECORD_TYPES:
        queue = writer._queues[record_type]
        items = queue.load_items_from_queue(status='pending')
        
        test_results.add_result(
            f"Batch Size - {record_type}",
            f"Batches should be {settings.BATCH_SIZE} records",
            all(len(json.loads(item.payload)) == settings.BATCH_SIZE 
                for item in items[:-1])  # Last batch might be partial
        )

async def run_all_tests():
    """Run all tests and display results."""
    await test_queue_creation()
    
    subscriber = FirehoseSubscriber()
    writer = BatchWriter()
    
    await test_record_processing(subscriber)
    await test_batch_operations(writer)
    
    test_results.display_results()

if __name__ == "__main__":
    asyncio.run(run_all_tests()) 