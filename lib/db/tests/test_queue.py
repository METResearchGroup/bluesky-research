"""Tests for queue.py."""

import os
import json
import time
import pytest
from typing import Generator
from lib.db.queue import Queue, QueueItem


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


@pytest.fixture
def custom_key_queue() -> Generator[Queue, None, None]:
    """Create a test queue with custom primary key.

    Yields:
        Queue: A newly created test queue instance with custom primary key
    """
    queue_name = get_test_queue_name()
    q = Queue(queue_name=queue_name, create_new_queue=True, primary_key="custom_id")
    yield q
    if os.path.exists(q.db_path):
        os.remove(q.db_path)


def test_queue_creation(queue: Queue) -> None:
    """Test queue initialization creates expected database structure.

    Verifies that:
    - Queue name is properly prefixed
    - Table name is set correctly
    - Database file is created
    - Queue starts empty
    - Default primary key is set

    Args:
        queue: Test queue fixture
    """
    assert queue.queue_name.startswith("test_")
    assert queue.queue_table_name == "queue"
    assert queue.db_path.endswith(".db")
    assert os.path.exists(queue.db_path)
    assert queue.get_queue_length() == 0
    assert queue.primary_key == "uri"


def test_custom_primary_key_creation(custom_key_queue: Queue) -> None:
    """Test queue creation with custom primary key.

    Args:
        custom_key_queue: Test queue fixture with custom primary key
    """
    assert custom_key_queue.primary_key == "custom_id"


def test_add_item_to_queue(queue: Queue) -> None:
    """Test adding a single item creates proper queue entry.

    Verifies that:
    - Returns valid QueueItem
    - Item has correct ID, payload and status
    - Queue length increases

    Args:
        queue: Test queue fixture
    """
    test_item = {"uri": "test123", "data": "value"}
    queue_item = queue.add_item_to_queue(test_item)

    assert isinstance(queue_item, QueueItem)
    assert queue_item.id is not None
    assert json.loads(queue_item.payload) == test_item
    assert queue_item.status == "pending"
    assert queue.get_queue_length() == 1


def test_duplicate_items(queue: Queue) -> None:
    """Test handling of duplicate items based on primary key.

    Verifies that:
    - First item is added successfully
    - Duplicate item is skipped
    - Queue length remains unchanged for duplicates

    Args:
        queue: Test queue fixture
    """
    test_item1 = {"uri": "test123", "data": "first"}
    test_item2 = {"uri": "test123", "data": "second"}  # Same URI
    
    # First item should be added successfully
    first_item = queue.add_item_to_queue(test_item1)
    assert first_item is not None
    assert queue.get_queue_length() == 1
    
    # Second item with same URI should be skipped
    second_item = queue.add_item_to_queue(test_item2)
    assert second_item is None  # Should return None for duplicates
    assert queue.get_queue_length() == 1  # Length shouldn't change


def test_custom_primary_key_deduplication(custom_key_queue: Queue) -> None:
    """Test deduplication with custom primary key.

    Verifies that:
    - Deduplication works with custom primary keys
    - Only first item with unique key is added

    Args:
        custom_key_queue: Test queue fixture with custom primary key
    """
    test_item1 = {"custom_id": "abc", "data": "first"}
    test_item2 = {"custom_id": "abc", "data": "second"}
    
    first_item = custom_key_queue.add_item_to_queue(test_item1)
    assert first_item is not None
    
    second_item = custom_key_queue.add_item_to_queue(test_item2)
    assert second_item is None
    assert custom_key_queue.get_queue_length() == 1


def test_batch_add_items_to_queue(queue: Queue) -> None:
    """Test batch adding multiple items processes all correctly.

    Verifies that:
    - All items are added successfully
    - Each item has correct attributes
    - Queue length reflects total items

    Args:
        queue: Test queue fixture
    """
    test_items = [{"uri": f"test_{i}", "data": f"data_{i}"} for i in range(3)]
    queue_items = queue.batch_add_items_to_queue(test_items)

    assert len(queue_items) == 3
    assert queue.get_queue_length() == 3

    for i, item in enumerate(queue_items):
        assert isinstance(item, QueueItem)
        assert item.id is not None
        assert json.loads(item.payload) == test_items[i]
        assert item.status == "pending"


def test_batch_add_items_with_duplicates(queue: Queue) -> None:
    """Test batch adding items with some duplicates.

    Verifies that:
    - Only unique items are added
    - Duplicates are skipped
    - Queue length reflects only unique items

    Args:
        queue: Test queue fixture
    """
    test_items = [
        {"uri": "test1", "data": "first"},
        {"uri": "test2", "data": "second"},
        {"uri": "test1", "data": "duplicate"},  # Duplicate URI
        {"uri": "test3", "data": "third"}
    ]
    
    queue_items = queue.batch_add_items_to_queue(test_items)
    assert len(queue_items) == 3  # Should only add unique items
    assert queue.get_queue_length() == 3
    
    # Verify the payloads of added items
    uris = {json.loads(item.payload)["uri"] for item in queue_items}
    assert uris == {"test1", "test2", "test3"}


def test_missing_primary_key(queue: Queue) -> None:
    """Test handling of items missing the primary key.

    Verifies that:
    - Adding item without primary key raises ValueError
    - Error message indicates missing primary key

    Args:
        queue: Test queue fixture
    """
    test_item = {"data": "value"}  # Missing 'uri' field
    
    with pytest.raises(ValueError) as exc_info:
        queue.add_item_to_queue(test_item)
    assert "Could not extract primary key" in str(exc_info.value)


def test_remove_item_from_queue(queue: Queue) -> None:
    """Test removing single item follows FIFO order and updates status.

    Verifies that:
    - Item is removed successfully
    - Removed item has correct payload and processing status
    - Empty queue returns None

    Args:
        queue: Test queue fixture
    """
    test_item = {"uri": "test123", "data": "value"}
    queue.add_item_to_queue(test_item)

    removed_item = queue.remove_item_from_queue()
    assert isinstance(removed_item, QueueItem)
    assert json.loads(removed_item.payload) == test_item
    assert removed_item.status == "processing"

    # Queue should be empty after removal
    assert queue.remove_item_from_queue() is None


def test_batch_remove_items_from_queue(queue: Queue) -> None:
    """Test batch removal respects limit and FIFO ordering.

    Verifies that:
    - Removes correct number of items up to limit
    - Items have processing status
    - Remaining items can be retrieved

    Args:
        queue: Test queue fixture
    """
    test_items = [{"uri": f"test_{i}", "data": f"data_{i}"} for i in range(3)]
    queue.batch_add_items_to_queue(test_items)

    # Remove 2 items
    removed_items = queue.batch_remove_items_from_queue(limit=2)
    assert len(removed_items) == 2
    for item in removed_items:
        assert isinstance(item, QueueItem)
        assert item.status == "processing"

    # One item should remain
    assert queue.get_queue_length() == 3  # Total count includes processed items
    remaining_items = queue.batch_remove_items_from_queue(limit=2)
    assert len(remaining_items) == 1


def test_queue_empty_behavior(queue: Queue) -> None:
    """Test queue operations on empty queue return expected values.

    Verifies that:
    - Removing from empty queue returns None
    - Batch removing from empty queue returns empty list

    Args:
        queue: Test queue fixture
    """
    assert queue.remove_item_from_queue() is None
    assert queue.batch_remove_items_from_queue(limit=1) == []


def test_invalid_payload() -> None:
    """Test queue validation rejects invalid payloads.

    Verifies that:
    - Empty dictionary payload raises ValueError
    - Ensures data integrity by validating inputs
    """
    queue_name = get_test_queue_name()
    q = Queue(queue_name=queue_name, create_new_queue=True)

    with pytest.raises(ValueError):
        q.add_item_to_queue({})  # Empty payload

    os.remove(q.db_path)


def test_queue_persistence(queue: Queue) -> None:
    """Test queue data persists across different connections.

    Verifies that:
    - Data added in one connection is visible in another
    - Queue state is maintained between connections
    - Items can be processed from new connection

    Args:
        queue: Test queue fixture
    """
    test_item = {"uri": "test123", "data": "value"}
    queue.add_item_to_queue(test_item)

    # Create new connection to same queue
    new_queue = Queue(queue_name=queue.queue_name)
    assert new_queue.get_queue_length() == 1

    removed_item = new_queue.remove_item_from_queue()
    assert json.loads(removed_item.payload) == test_item