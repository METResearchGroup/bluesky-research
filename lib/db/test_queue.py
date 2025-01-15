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


def test_queue_creation(queue: Queue) -> None:
    """Test queue initialization creates expected database structure.

    Verifies that:
    - Queue name is properly prefixed
    - Table name is set correctly
    - Database file is created
    - Queue starts empty

    Args:
        queue: Test queue fixture
    """
    assert queue.queue_name.startswith("test_")
    assert queue.queue_table_name == "queue"
    assert queue.db_path.endswith(".db")
    assert os.path.exists(queue.db_path)
    assert queue.get_queue_length() == 0


def test_add_item_to_queue(queue: Queue) -> None:
    """Test adding a single item creates proper queue entry.

    Verifies that:
    - Returns valid QueueItem
    - Item has correct ID, payload and status
    - Queue length increases

    Args:
        queue: Test queue fixture
    """
    test_item = {"test": "data"}
    queue_item = queue.add_item_to_queue(test_item)

    assert isinstance(queue_item, QueueItem)
    assert queue_item.id is not None
    assert json.loads(queue_item.payload) == test_item
    assert queue_item.status == "pending"
    assert queue.get_queue_length() == 1


def test_batch_add_items_to_queue(queue: Queue) -> None:
    """Test batch adding multiple items processes all correctly.

    Verifies that:
    - All items are added successfully
    - Each item has correct attributes
    - Queue length reflects total items

    Args:
        queue: Test queue fixture
    """
    test_items = [{"test": f"data_{i}"} for i in range(3)]
    queue_items = queue.batch_add_item_to_queue(test_items)

    assert len(queue_items) == 3
    assert queue.get_queue_length() == 3

    for i, item in enumerate(queue_items):
        assert isinstance(item, QueueItem)
        assert item.id is not None
        assert json.loads(item.payload) == test_items[i]
        assert item.status == "pending"


def test_remove_item_from_queue(queue: Queue) -> None:
    """Test removing single item follows FIFO order and updates status.

    Verifies that:
    - Item is removed successfully
    - Removed item has correct payload and processing status
    - Empty queue returns None

    Args:
        queue: Test queue fixture
    """
    test_item = {"test": "data"}
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
    test_items = [{"test": f"data_{i}"} for i in range(3)]
    queue.batch_add_item_to_queue(test_items)

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
    test_item = {"test": "data"}
    queue.add_item_to_queue(test_item)

    # Create new connection to same queue
    new_queue = Queue(queue_name=queue.queue_name)
    assert new_queue.get_queue_length() == 1

    removed_item = new_queue.remove_item_from_queue()
    assert json.loads(removed_item.payload) == test_item
