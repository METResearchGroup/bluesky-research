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

from datetime import datetime, timedelta
import os
import json
import time
import pytest
from typing import Generator

from lib.db.queue import DEFAULT_BATCH_CHUNK_SIZE, DEFAULT_BATCH_WRITE_SIZE, Queue, QueueItem
from lib.log.logger import get_logger

logger = get_logger(__file__)

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
    try:
        if os.path.exists(q.db_path):
            os.remove(q.db_path)
        # Also remove WAL and SHM files if they exist
        wal_file = f"{q.db_path}-wal"
        shm_file = f"{q.db_path}-shm"
        if os.path.exists(wal_file):
            os.remove(wal_file)
        if os.path.exists(shm_file):
            os.remove(shm_file)
    except Exception as e:
        logger.warning(f"Error cleaning up test queue files: {e}")


class TestQueueCreation:
    def test_queue_creation(self, queue: Queue) -> None:
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


class TestAddItem:
    def test_add_item_to_queue(self, queue: Queue) -> None:
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


class TestBatchAdd:
    def test_batch_add_items_to_queue(self, queue: Queue) -> None:
        """Test batch adding multiple items processes all correctly.

        Verifies:
        - All items are added successfully 
        - Items are properly chunked based on batch_size
        - Chunks are written in batches based on batch_write_size
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
            batch_size=2,
            batch_write_size=1
        )
        
        assert queue.get_queue_length() == 3  # Should create 3 chunks (2+2+1)

    def test_batch_add_items_with_write_batching(self, queue: Queue) -> None:
        """Test batch adding with write batching.

        Verifies:
        - Items are properly chunked into batch_size chunks
        - Chunks are written in batch_write_size groups
        - All items are added successfully

        Args:
            queue: Test queue fixture
        """
        test_items = [{"key": f"value_{i}"} for i in range(10)]
        test_metadata = {"source": "batch_test"}

        queue.batch_add_items_to_queue(
            items=test_items,
            metadata=test_metadata,
            batch_size=2,  # Creates 5 chunks of 2 items each
            batch_write_size=2  # Writes chunks in groups of 2
        )

        assert queue.get_queue_length() == 5  # Should have 5 total chunks

    def test_batch_add_items_with_duplicates(self, queue: Queue) -> None:
        """Test batch adding items with some duplicates.

        Verifies that:
        - All items are added, including duplicates
        - Items are properly chunked and batch written
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
            items=test_items,
            batch_size=2,  # Creates 2 chunks of 2 items
            batch_write_size=1  # Write one chunk at a time
        )
        assert queue.get_queue_length() == 2  # Should have 2 chunks

class TestRemoveItem:
    def test_remove_item_from_queue(self, queue: Queue) -> None:
        """Test removing single item follows FIFO order and updates status.

        Verifies:
        - Item is removed successfully
        - Removed item has correct payload, metadata and processing status
        - Empty queue returns None
        - Status transitions from 'pending' to 'processing'

        Args:
            queue: Test queue fixture
        """
        test_item = {"test_key": "test_value"}
        test_metadata = {"source": "test"}
        queue.add_item_to_queue(test_item, metadata=test_metadata)

        removed_item = queue.remove_item_from_queue()
        assert isinstance(removed_item, QueueItem)
        assert json.loads(removed_item.payload) == test_item
        assert json.loads(removed_item.metadata)["source"] == "test"
        assert removed_item.status == "processing"

        # Queue should be empty after removal
        assert queue.remove_item_from_queue() is None


class TestBatchRemove:
    def test_batch_remove_items_from_queue(self, queue: Queue) -> None:
        """Test batch removal respects limit and FIFO ordering.

        Verifies:
        - Removes correct number of items up to limit
        - Items maintain FIFO order
        - All items have processing status and metadata
        - Returns empty list when queue is empty

        Args:
            queue: Test queue fixture
        """
        test_items = [{"key": f"value_{i}"} for i in range(3)]
        test_metadata = {"source": "batch_test"}
        queue.batch_add_items_to_queue(
            test_items,
            metadata=test_metadata,
            batch_size=1 # to guarantee 3 rows in the queue.
        )

        # Remove 2 items
        removed_items = queue.batch_remove_items_from_queue(limit=2)
        assert len(removed_items) == 2
        for item in removed_items:
            assert isinstance(item, QueueItem)
            assert item.status == "processing"
            assert json.loads(item.metadata)["source"] == "batch_test"

        # One item should remain
        remaining_items = queue.batch_remove_items_from_queue()
        assert len(remaining_items) == 1


class TestBatchDelete:
    def test_batch_delete_items_by_ids(self, queue: Queue) -> None:
        """Test batch deletion by IDs.

        Verifies:
        - Items with specified IDs are deleted
        - Returns number of deleted items
        - Non-existent IDs are ignored
        """
        # Add test items
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        queue.batch_add_items_to_queue(test_items, batch_size=1)

        # Get IDs to delete
        items = queue.load_items_from_queue()
        ids_to_delete = [items[0].id, items[2].id, 999]  # Include non-existent ID

        # Delete items
        deleted_count = queue.batch_delete_items_by_ids(ids_to_delete)
        assert deleted_count == 2  # Should only delete 2 items (ignore non-existent ID)

        # Verify remaining items
        remaining_items = queue.load_items_from_queue()
        assert len(remaining_items) == 3
        remaining_ids = [item.id for item in remaining_items]
        assert items[0].id not in remaining_ids
        assert items[2].id not in remaining_ids

    def test_delete_and_verify_remaining(self, queue: Queue) -> None:
        """Test deletion followed by verification of remaining items.

        Verifies:
        - Correct items are deleted
        - Remaining items are intact and unchanged
        - Queue length is updated correctly
        """
        # Add test items with distinct values
        test_items = [
            {"key": "keep_1", "value": 1},
            {"key": "delete_1", "value": 2},
            {"key": "keep_2", "value": 3},
            {"key": "delete_2", "value": 4},
            {"key": "keep_3", "value": 5}
        ]
        queue.batch_add_items_to_queue(test_items, batch_size=1)

        # Get items to delete (those with 'delete' in key)
        items = queue.load_items_from_queue()
        ids_to_delete = [
            item.id for item in items 
            if "delete" in json.loads(item.payload)[0]["key"]
        ]

        # Delete items
        deleted_count = queue.batch_delete_items_by_ids(ids_to_delete)
        assert deleted_count == 2

        # Verify remaining items
        remaining_items = queue.load_items_from_queue()
        assert len(remaining_items) == 3
        
        # Check that only 'keep' items remain
        for item in remaining_items:
            payload = json.loads(item.payload)[0]
            assert "keep" in payload["key"]
            assert payload["value"] in [1, 3, 5]


class TestQueueEmpty:
    def test_queue_empty_behavior(self, queue: Queue) -> None:
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


class TestInvalidPayload:
    def test_invalid_payload(self, queue: Queue) -> None:
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


class TestQueuePersistence:
    def test_queue_persistence(self, queue: Queue) -> None:
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
        test_metadata = {"source": "test"}
        queue.add_item_to_queue(test_item, metadata=test_metadata)

        # Create new connection to same queue
        new_queue = Queue(queue_name=queue.queue_name)
        assert new_queue.get_queue_length() == 1

        removed_item = new_queue.remove_item_from_queue()
        assert json.loads(removed_item.payload) == test_item
        assert json.loads(removed_item.metadata)["source"] == "test"


class TestMetadataHandling:
    def test_metadata_handling(self, queue: Queue) -> None:
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
        
        # Verify metadata through direct DB query and through load_items
        with queue._get_connection() as conn:
            cursor = conn.execute(
                f"SELECT metadata FROM {queue.queue_table_name} LIMIT 1"
            )
            stored_metadata = json.loads(cursor.fetchone()[0])
            
            assert stored_metadata["source"] == "test"
            assert stored_metadata["priority"] == "high"
            assert stored_metadata["batch_size"] == 1
            assert stored_metadata["actual_batch_size"] == 1

        loaded_items = queue.load_items_from_queue()
        assert len(loaded_items) == 1
        loaded_metadata = json.loads(loaded_items[0].metadata)
        assert loaded_metadata["source"] == "test"
        assert loaded_metadata["priority"] == "high"


class TestBatchChunking:
    def test_batch_chunking(self, queue: Queue) -> None:
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


class TestBatchMetadata:
    def test_batch_with_metadata(self, queue: Queue) -> None:
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
        
        loaded_items = queue.load_items_from_queue()
        assert len(loaded_items) == 3  # Should be 3 chunks (2+2+1)
        
        for item in loaded_items:
            metadata = json.loads(item.metadata)
            # Original metadata preserved
            assert metadata["source"] == "batch_test"
            assert metadata["priority"] == "low"
            # Batch information added
            assert metadata["batch_size"] == batch_size
            assert "actual_batch_size" in metadata


class TestDefaultBatchSize:
    def test_default_batch_size(self, queue: Queue) -> None:
        """Test default batch size behavior.

        Verifies:
        - Default batch chunk size is used when not specified
        - Default batch write size is used when not specified 
        - Large batches are properly chunked into minibatches
        - Minibatches are properly grouped into write batches
        - Metadata reflects batch sizing correctly

        Args:
            queue: Test queue fixture
        """
        # Create enough items to test both chunk size and write size defaults
        num_items = DEFAULT_BATCH_CHUNK_SIZE * (DEFAULT_BATCH_WRITE_SIZE + 1) + 1
        test_items = [{"key": f"value_{i}"} for i in range(num_items)]
        
        queue.batch_add_items_to_queue(items=test_items)
        
        loaded_items = queue.load_items_from_queue()
        
        # Should create DEFAULT_BATCH_WRITE_SIZE + 2 chunks:
        # - DEFAULT_BATCH_WRITE_SIZE full chunks of DEFAULT_BATCH_CHUNK_SIZE items
        # - 1 full chunk of DEFAULT_BATCH_CHUNK_SIZE items
        # - 1 partial chunk with remaining item
        expected_chunks = DEFAULT_BATCH_WRITE_SIZE + 2
        assert len(loaded_items) == expected_chunks
        
        # Check first chunk uses defaults
        first_metadata = json.loads(loaded_items[0].metadata)
        assert first_metadata["batch_size"] == DEFAULT_BATCH_CHUNK_SIZE
        assert first_metadata["actual_batch_size"] == DEFAULT_BATCH_CHUNK_SIZE
        
        # Check middle chunk uses defaults
        middle_metadata = json.loads(loaded_items[DEFAULT_BATCH_WRITE_SIZE].metadata)
        assert middle_metadata["batch_size"] == DEFAULT_BATCH_CHUNK_SIZE
        assert middle_metadata["actual_batch_size"] == DEFAULT_BATCH_CHUNK_SIZE
        
        # Check last chunk has remainder
        last_metadata = json.loads(loaded_items[-1].metadata)
        assert last_metadata["batch_size"] == DEFAULT_BATCH_CHUNK_SIZE
        assert last_metadata["actual_batch_size"] == 1


class TestLoadItems:
    def test_load_items_with_limit(self, queue: Queue) -> None:
        """Test loading items with limit filter.
        
        Verifies:
        - Correct number of items returned when limit specified
        - Items returned in correct order
        - Metadata is properly loaded
        """
        # Add test items
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        test_metadata = {"source": "test"}
        queue.batch_add_items_to_queue(
            items=test_items,
            metadata=test_metadata,
            batch_size=1
        )
        
        # Test with limit
        items = queue.load_items_from_queue(limit=3)
        assert len(items) == 3
        for i, item in enumerate(items):
            # payloads are lists of dicts (here, list of length 1)
            # due to batching implementation.
            assert json.loads(item.payload)[0]["key"] == f"value_{i}"
            assert json.loads(item.metadata)["source"] == "test"

    def test_load_items_with_status(self, queue: Queue) -> None:
        """Test loading items with status filter.
        
        Verifies:
        - Only items with matching status are returned
        - Metadata is preserved
        """
        # Add items with different statuses
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        test_metadata = {"source": "test"}
        queue.batch_add_items_to_queue(
            items=test_items,
            metadata=test_metadata,
            batch_size=1
        )
        
        # Mark some items as completed
        with queue._get_connection() as conn:
            conn.execute(
                f"UPDATE {queue.queue_table_name} SET status = 'completed' WHERE id IN (1, 2)"
            )
        
        # Test filtering by status
        completed_items = queue.load_items_from_queue(status="completed")
        assert len(completed_items) == 2
        for item in completed_items:
            assert json.loads(item.metadata)["source"] == "test"
        
        pending_items = queue.load_items_from_queue(status="pending")
        assert len(pending_items) == 3
        for item in pending_items:
            assert json.loads(item.metadata)["source"] == "test"

    def test_load_items_with_min_id(self, queue: Queue) -> None:
        """Test loading items with min_id filter.
        
        Verifies:
        - Only items with id > min_id are returned
        - Metadata is preserved
        """
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        test_metadata = {"source": "test"}
        queue.batch_add_items_to_queue(
            items=test_items,
            metadata=test_metadata,
            batch_size=1
        )
        
        items = queue.load_items_from_queue(min_id=2)
        assert len(items) == 3  # Should return items with id 3,4,5
        assert all(item.id > 2 for item in items)
        for item in items:
            assert json.loads(item.metadata)["source"] == "test"

    def test_load_items_with_min_timestamp(self, queue: Queue) -> None:
        """Test loading items with min_timestamp filter.
        
        Verifies:
        - Only items with timestamp > min_timestamp are returned
        - Metadata is preserved
        """
        # Add items with different timestamps
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        test_metadata = {"source": "test"}

        # add first 3 items
        queue.batch_add_items_to_queue(
            items=test_items[:3],
            metadata=test_metadata,
            batch_size=1
        )

        # wait 5 seconds, add next 2 items
        time.sleep(5)
        queue.batch_add_items_to_queue(
            items=test_items[3:],
            metadata=test_metadata,
            batch_size=1
        )
        
        # Get middle timestamp
        with queue._get_connection() as conn:
            cursor = conn.execute(
                f"SELECT created_at FROM {queue.queue_table_name} ORDER BY created_at LIMIT 1 OFFSET 2"
            )
            mid_timestamp = cursor.fetchone()[0]
        
        items = queue.load_items_from_queue(min_timestamp=mid_timestamp)
        assert len(items) == 2  # Should return 2 newest items
        for item in items:
            assert json.loads(item.metadata)["source"] == "test"


class TestLoadItemsCombinedFilters:
    def test_load_items_combined_filters_1(self, queue: Queue) -> None:
        """Test loading items with multiple filters combined.
        
        Case 1: status + limit
        Verifies metadata is preserved
        """
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        test_metadata = {"source": "test"}
        queue.batch_add_items_to_queue(
            items=test_items,
            metadata=test_metadata,
            batch_size=1
        )
        
        # Mark some items as completed
        with queue._get_connection() as conn:
            conn.execute(
                f"UPDATE {queue.queue_table_name} SET status = 'completed' WHERE id IN (1, 2, 3)"
            )
        
        items = queue.load_items_from_queue(status="completed", limit=2)
        assert len(items) == 2
        assert all(item.status == "completed" for item in items)
        for item in items:
            assert json.loads(item.metadata)["source"] == "test"

    def test_load_items_combined_filters_2(self, queue: Queue) -> None:
        """Test loading items with multiple filters combined.
        
        Case 2: min_id + min_timestamp + limit
        Verifies metadata is preserved
        """
        test_items = [{"key": f"value_{i}"} for i in range(5)]
        test_metadata = {"source": "test"}

        # add first 3 items
        queue.batch_add_items_to_queue(
            items=test_items[:3],
            metadata=test_metadata,
            batch_size=1
        )
        
        # wait 5 seconds, add next 2 items
        time.sleep(5)
        queue.batch_add_items_to_queue(
            items=test_items[3:],
            metadata=test_metadata,
            batch_size=1
        )

        # Get middle timestamp
        with queue._get_connection() as conn:
            cursor = conn.execute(
                f"SELECT created_at FROM {queue.queue_table_name} ORDER BY created_at LIMIT 1 OFFSET 2"
            )
            mid_timestamp = cursor.fetchone()[0]
        
        items = queue.load_items_from_queue(
            min_id=2,
            min_timestamp=mid_timestamp,
            limit=2
        )
        assert len(items) == 2
        assert all(item.id > 2 for item in items)
        for item in items:
            assert json.loads(item.metadata)["source"] == "test"

    def test_load_items_combined_filters_3(self, queue: Queue) -> None:
        """Test loading items with multiple filters combined.
        
        Case 3: all filters together
        Verifies metadata is preserved

        In this scenario, we have:
        - 10 items in the queue.
            - 3 are added in the first batch.
            - 7 are added in the second batch.
        - 3 items are marked as completed (the last 3).
        - We want to load the last 2 items that are not completed but are in the
        - second batch.
        - We:
            - Set the filter as just before the middle timestamp. That way, we can
            query for records that are after the middle timestamp (here, the
            records from the second batch).
            - Set the status to "pending" to query for records that are not completed.
            - Set the min_id to 6 to query for records that are in the second batch.
            - Set the limit to 2 to query for the last 2 items in the query set
            (there should be only 2 anyways).
        """
        test_items = [{"key": f"value_{i}"} for i in range(10)]
        test_metadata = {"source": "test"}

        # add first 3 items
        queue.batch_add_items_to_queue(
            items=test_items[:3],
            metadata=test_metadata,
            batch_size=1
        )

        # wait 5 seconds, add next 7 items
        time.sleep(5)
        queue.batch_add_items_to_queue(
            items=test_items[3:],
            metadata=test_metadata,
            batch_size=1
        )

        # Mark some items as completed
        with queue._get_connection() as conn:
            conn.execute(
                f"UPDATE {queue.queue_table_name} SET status = 'completed' WHERE id > 7"
            )
            # Get timestamp of the 5th item (which is before our target items)
            cursor = conn.execute(
                f"SELECT created_at FROM {queue.queue_table_name} WHERE id = 5"
            )
            mid_timestamp: str = cursor.fetchone()[0]

        # set the filter as just before the middle timestamp. That way, we can
        # query for records that are after the middle timestamp (here, the
        # records from the second batch).
        one_second_before_mid_timestamp = (
            datetime.strptime(
                mid_timestamp, "%Y-%m-%d-%H:%M:%S"
            ) - timedelta(seconds=1)
        ).strftime("%Y-%m-%d-%H:%M:%S")

        items = queue.load_items_from_queue(
            status="pending",
            min_id=5,
            min_timestamp=one_second_before_mid_timestamp,
            limit=2
        )
        assert len(items) == 2
        assert all(item.status == "pending" for item in items)
        assert all(item.id > 5 for item in items)
        for item in items:
            assert json.loads(item.metadata)["source"] == "test"


class TestLoadDictItems:
    def test_load_dict_items_from_queue(self, queue: Queue) -> None:
        """Test loading items as dictionaries.
        
        Verifies:
        - Items are returned as dictionaries
        - All filters work as expected
        - Metadata is included in dictionary format
        - Payload is properly deserialized
        """
        # Add test items with different statuses and timestamps
        test_items = [{"uri": f"post_{i}", "text": f"Sample text {i}"} for i in range(5)]
        test_metadata = {"source": "test", "priority": "high"}
        
        # Add first batch
        queue.batch_add_items_to_queue(
            items=test_items[:3],
            metadata=test_metadata,
            batch_size=1
        )
        
        # Wait and add second batch
        time.sleep(1)
        queue.batch_add_items_to_queue(
            items=test_items[3:],
            metadata=test_metadata,
            batch_size=1
        )
        
        # Mark some items as completed
        with queue._get_connection() as conn:
            conn.execute(
                f"UPDATE {queue.queue_table_name} SET status = 'completed' WHERE id IN (1, 2)"
            )
            cursor = conn.execute(
                f"SELECT created_at FROM {queue.queue_table_name} ORDER BY created_at LIMIT 1 OFFSET 2"
            )
            mid_timestamp = cursor.fetchone()[0]
        
        # Test loading with combined filters
        dict_items = queue.load_dict_items_from_queue(
            status="pending",
            min_id=2,
            min_timestamp=mid_timestamp,
            limit=2
        )
        
        assert len(dict_items) == 2
        for item in dict_items:
            # Verify dictionary structure
            assert isinstance(item, dict)
            assert "batch_id" in item
            assert "batch_metadata" in item
            
            # Verify values
            assert isinstance(item["batch_metadata"], str)
            metadata = json.loads(item["batch_metadata"])
            assert metadata["source"] == "test"
            assert metadata["priority"] == "high"
            
            # Verify content
            assert "uri" in item
            assert "text" in item
            assert item["uri"].startswith("post_")
            assert item["text"].startswith("Sample text")
