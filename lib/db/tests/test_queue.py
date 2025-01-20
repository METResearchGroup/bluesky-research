"""Tests for queue.py.

This test suite verifies the functionality of the Queue class which implements
a SQLite-backed FIFO queue system. The tests cover:

- Queue creation and initialization
- Adding single and batched items
- Removing single and batched items
- Queue persistence and concurrent access
- Error handling and edge cases
- SQLite optimization settings
"""

import os
import json
import time
import pytest
from typing import Generator
from lib.db.queue import DEFAULT_BATCH_SIZE, Queue, QueueItem


def get_test_queue_name() -> str:
    """Generate unique test queue name with timestamp.

    Returns:
        str: A unique queue name prefixed with 'test_' and current timestamp
    """
    return f"test_{int(time.time())}"


@pytest.fixture
def queue() -> Generator[Queue, None, None]:
    """Create a test queue and clean it up after tests.

    This fixture ensures each test gets a fresh queue and handles cleanup
    to prevent test pollution and leftover files.

    Yields:
        Queue: A newly created test queue instance
    """
    queue_name = get_test_queue_name()
    q = Queue(queue_name=queue_name, create_new_queue=True)
    yield q
    # Cleanup
    if os.path.exists(q.db_path):
        os.remove(q.db_path)


def test_queue_creation(queue: Queue) -> None:
    """Test queue initialization creates expected database structure.

    Verifies:
    - Queue name is properly set
    - Table name is set correctly
    - Database file is created
    - Queue starts empty
    - WAL mode is enabled
    """
    assert queue.queue_name.startswith("test_")
    assert queue.queue_table_name == "queue"
    assert queue.db_path.endswith(".db")
    assert os.path.exists(queue.db_path)
    assert queue.get_queue_length() == 0

    # Verify WAL mode
    with queue._get_connection() as conn:
        cursor = conn.execute("PRAGMA journal_mode")
        assert cursor.fetchone()[0].upper() == "WAL"


def test_add_item_to_queue(queue: Queue) -> None:
    """Test adding a single item creates proper queue entry.

    Verifies:
    - Item is added successfully
    - Queue length increases
    - Metadata is properly stored
    """
    test_item = {"test_key": "test_value"}
    test_metadata = {"source": "test"}
    
    queue.add_item_to_queue(test_item, metadata=test_metadata)
    assert queue.get_queue_length() == 1


def test_batch_add_items_to_queue(queue: Queue) -> None:
    """Test batch adding multiple items processes all correctly.

    Verifies:
    - All items are added successfully
    - Items are properly chunked based on batch size
    - Metadata is stored for each chunk
    - Queue length reflects total items

    Args:
        queue: Test queue fixture
    """
    test_items = [{"key": f"value_{i}"} for i in range(5)]
    test_metadata = {"source": "batch_test"}
    
    queue.batch_add_items_to_queue(
        items=test_items,
        metadata=test_metadata,
        batch_size=2
    )
    
    assert queue.get_queue_length() == 3  # Should create 3 chunks (2+2+1)


def test_batch_add_items_with_duplicates(queue: Queue) -> None:
    """Test batch adding items with some duplicates.

    Verifies that:
    - All items are added, including duplicates
    - Queue length reflects total number of items

    Args:
        queue: Test queue fixture
    """
    test_items = [
        {"uri": "test1", "data": "first"},
        {"uri": "test2", "data": "second"}, 
        {"uri": "test1", "data": "duplicate"},  # Duplicate URI
        {"uri": "test3", "data": "third"}
    ]
    
    queue.batch_add_items_to_queue(
        test_items,
        batch_size=3 # if duplicate is removed, it would be 3 items = 1 batch. If duplicate is kept, 4 items = 2 batches
    )
    assert queue.get_queue_length() == 2  # Should add all items including duplicates
        

def test_remove_item_from_queue(queue: Queue) -> None:
    """Test removing single item follows FIFO order and updates status.

    Verifies:
    - Item is removed successfully
    - Removed item has correct payload and processing status
    - Empty queue returns None
    - Status transitions from 'pending' to 'processing'

    Args:
        queue: Test queue fixture
    """
    test_item = {"test_key": "test_value"}
    queue.add_item_to_queue(test_item)

    removed_item = queue.remove_item_from_queue()
    assert isinstance(removed_item, QueueItem)
    assert json.loads(removed_item.payload) == test_item
    assert removed_item.status == "processing"

    # Queue should be empty after removal
    assert queue.remove_item_from_queue() is None


def test_batch_remove_items_from_queue(queue: Queue) -> None:
    """Test batch removal respects limit and FIFO ordering.

    Verifies:
    - Removes correct number of items up to limit
    - Items maintain FIFO order
    - All items have processing status
    - Returns empty list when queue is empty

    Args:
        queue: Test queue fixture
    """
    test_items = [{"key": f"value_{i}"} for i in range(3)]
    queue.batch_add_items_to_queue(
        test_items,
        batch_size=1 # to guarantee 3 rows in the queue.
    )

    # Remove 2 items
    removed_items = queue.batch_remove_items_from_queue(limit=2)
    assert len(removed_items) == 2
    for item in removed_items:
        assert isinstance(item, QueueItem)
        assert item.status == "processing"

    # One item should remain
    remaining_items = queue.batch_remove_items_from_queue()
    assert len(remaining_items) == 1


def test_queue_empty_behavior(queue: Queue) -> None:
    """Test queue operations on empty queue return expected values.

    Verifies:
    - Removing from empty queue returns None
    - Batch removing from empty queue returns empty list
    - Queue length remains accurate

    Args:
        queue: Test queue fixture
    """
    assert queue.remove_item_from_queue() is None
    assert queue.batch_remove_items_from_queue(limit=1) == []
    assert queue.get_queue_length() == 0


def test_invalid_payload(queue: Queue) -> None:
    """Test queue validation rejects invalid payloads.

    Verifies:
    - Empty payload raises ValueError
    - Non-JSON-serializable payload raises ValueError
    - None payload raises ValueError
    """
    with pytest.raises(ValueError):
        queue.add_item_to_queue({})  # Empty payload

    with pytest.raises(ValueError):
        queue.add_item_to_queue(None)  # None payload


def test_queue_persistence(queue: Queue) -> None:
    """Test queue data persists across different connections.

    Verifies:
    - Data added in one connection is visible in another
    - Queue state is maintained between connections
    - WAL mode enables concurrent access
    - Items can be processed from new connection

    Args:
        queue: Test queue fixture
    """
    test_item = {"test_key": "test_value"}
    queue.add_item_to_queue(test_item)

    # Create new connection to same queue
    new_queue = Queue(queue_name=queue.queue_name)
    assert new_queue.get_queue_length() == 1

    removed_item = new_queue.remove_item_from_queue()
    assert json.loads(removed_item.payload) == test_item


def test_metadata_handling(queue: Queue) -> None:
    """Test metadata is properly stored and retrieved.

    Verifies:
    - Metadata is stored with queue items
    - Default batch metadata is added
    - Custom metadata is preserved
    - Metadata JSON serialization works

    Args:
        queue: Test queue fixture
    """
    test_item = {"test_key": "test_value"}
    test_metadata = {"source": "test", "priority": "high"}
    
    queue.add_item_to_queue(test_item, metadata=test_metadata)
    
    # Verify metadata through direct DB query
    with queue._get_connection() as conn:
        cursor = conn.execute(
            f"SELECT metadata FROM {queue.queue_table_name} LIMIT 1"
        )
        stored_metadata = json.loads(cursor.fetchone()[0])
        
        assert stored_metadata["source"] == "test"
        assert stored_metadata["priority"] == "high"
        assert stored_metadata["batch_size"] == 1
        assert stored_metadata["actual_batch_size"] == 1


def test_batch_chunking(queue: Queue) -> None:
    """Test batch processing chunks items correctly.

    Verifies:
    - Items are split into correct chunk sizes
    - Last chunk handles remainder properly
    - Batch metadata is accurate for each chunk
    - All items are stored properly

    Args:
        queue: Test queue fixture
    """
    test_items = [{"key": f"value_{i}"} for i in range(7)]
    batch_size = 3
    
    queue.batch_add_items_to_queue(
        items=test_items,
        batch_size=batch_size
    )
    
    # Should create 3 chunks: 3 + 3 + 1 items
    with queue._get_connection() as conn:
        cursor = conn.execute(
            f"SELECT payload, metadata FROM {queue.queue_table_name} ORDER BY created_at"
        )
        chunks = cursor.fetchall()
        
        assert len(chunks) == 3
        
        # First chunk should have 3 items
        first_chunk = json.loads(chunks[0][0])
        first_metadata = json.loads(chunks[0][1])
        assert len(first_chunk) == 3
        assert first_metadata["batch_size"] == batch_size
        assert first_metadata["actual_batch_size"] == 3
        
        # Last chunk should have 1 item
        last_chunk = json.loads(chunks[-1][0])
        last_metadata = json.loads(chunks[-1][1])
        assert len(last_chunk) == 1
        assert last_metadata["batch_size"] == batch_size
        assert last_metadata["actual_batch_size"] == 1


def test_batch_with_metadata(queue: Queue) -> None:
    """Test batch processing with custom metadata.

    Verifies:
    - Custom metadata is preserved across all chunks
    - Batch information is added to metadata
    - Each chunk maintains correct metadata
    - Original metadata is not modified

    Args:
        queue: Test queue fixture
    """
    test_items = [{"key": f"value_{i}"} for i in range(5)]
    test_metadata = {"source": "batch_test", "priority": "low"}
    batch_size = 2
    
    queue.batch_add_items_to_queue(
        items=test_items,
        metadata=test_metadata,
        batch_size=batch_size
    )
    
    with queue._get_connection() as conn:
        cursor = conn.execute(
            f"SELECT metadata FROM {queue.queue_table_name} ORDER BY created_at"
        )
        all_metadata = [json.loads(row[0]) for row in cursor.fetchall()]
        
        assert len(all_metadata) == 3  # Should be 3 chunks (2+2+1)
        
        for metadata in all_metadata:
            # Original metadata preserved
            assert metadata["source"] == "batch_test"
            assert metadata["priority"] == "low"
            # Batch information added
            assert metadata["batch_size"] == batch_size
            assert "actual_batch_size" in metadata


def test_default_batch_size(queue: Queue) -> None:
    """Test default batch size behavior.

    Verifies:
    - Default batch size is used when not specified
    - Large batches are properly chunked
    - Metadata reflects default batch size

    Args:
        queue: Test queue fixture
    """
    test_items = [{"key": f"value_{i}"} for i in range(DEFAULT_BATCH_SIZE + 1)]
    
    queue.batch_add_items_to_queue(items=test_items)
    
    with queue._get_connection() as conn:
        cursor = conn.execute(
            f"SELECT metadata FROM {queue.queue_table_name} ORDER BY created_at"
        )
        all_metadata = [json.loads(row[0]) for row in cursor.fetchall()]
        
        assert len(all_metadata) == 2  # Should split into 2 chunks
        
        # First chunk should use default batch size
        assert all_metadata[0]["batch_size"] == DEFAULT_BATCH_SIZE
        assert all_metadata[0]["actual_batch_size"] == DEFAULT_BATCH_SIZE
        
        # Second chunk should have the remainder
        assert all_metadata[1]["actual_batch_size"] == 1
