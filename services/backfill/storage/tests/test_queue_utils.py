"""Tests for export_data.py."""

import pytest
from unittest.mock import Mock, patch

from lib.db.queue import Queue
from services.backfill.storage.queue_utils import (
    write_record_type_to_cache,
    write_records_to_cache,
)
from services.backfill.core.constants import base_queue_name


class TestWriteRecordTypeToCache:
    """Tests for write_record_type_to_cache function.
    
    This test class verifies that records of a specific type are properly written to
    the appropriate queue, including:
    - Handling of empty record lists
    - Correct queue name construction
    - Batch processing of records
    """
    
    @pytest.fixture
    def mock_queue(self):
        """Fixture for mocking Queue class."""
        with patch("services.backfill.sync.export_data.Queue") as mock:
            queue_instance = Mock(spec=Queue)
            mock.return_value = queue_instance
            yield mock, queue_instance
    
    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking logger."""
        with patch("services.backfill.sync.export_data.logger") as mock:
            yield mock
    
    def test_write_record_type_to_cache_empty_records(self, mock_queue, mock_logger):
        """Test handling of empty record list.
        
        When provided with an empty list of records, the function should
        return early without creating a queue or attempting to add items.
        """
        write_record_type_to_cache(record_type="post", records=[])
        
        # Verify Queue was not instantiated and logger was not called
        mock_queue[0].assert_not_called()
        mock_logger.info.assert_not_called()
    
    def test_write_record_type_to_cache_with_records(self, mock_queue, mock_logger):
        """Test writing records to cache.
        
        When provided with records, the function should:
        - Log the operation
        - Create a queue with the correct name
        - Add the records to the queue in batches
        """
        record_type = "post"
        records = [{"text": "Post 1"}, {"text": "Post 2"}]
        batch_size = 10
        
        write_record_type_to_cache(
            record_type=record_type,
            records=records,
            batch_size=batch_size
        )
        
        # Verify logger was called with correct message
        mock_logger.info.assert_called_once_with(
            f"Adding {len(records)} records to the backfill sync queue for record type {record_type}."
        )
        
        # Verify Queue was instantiated with correct name
        expected_queue_name = f"{base_queue_name}_{record_type}"
        mock_queue[0].assert_called_once_with(
            queue_name=expected_queue_name,
            create_new_queue=True
        )
        
        # Verify records were added to queue with correct batch size
        mock_queue[1].batch_add_items_to_queue.assert_called_once_with(
            items=records,
            batch_size=batch_size
        )
    
    def test_write_record_type_to_cache_default_batch_size(self, mock_queue, mock_logger):
        """Test writing records with default batch size.
        
        When batch_size is not provided, the function should pass None as the batch_size
        to the queue's batch_add_items_to_queue method.
        """
        record_type = "like"
        records = [{"uri": "at://like1"}, {"uri": "at://like2"}]
        
        write_record_type_to_cache(
            record_type=record_type,
            records=records
        )
        
        # Verify records were added to queue with default batch size (None)
        mock_queue[1].batch_add_items_to_queue.assert_called_once_with(
            items=records,
            batch_size=None
        )


class TestWriteRecordsToCache:
    """Tests for write_records_to_cache function.
    
    This test class verifies that multiple record types are properly processed and
    written to their respective queues, including:
    - Handling of empty record maps
    - Correct delegation to write_record_type_to_cache for each record type
    """
    
    @patch("services.backfill.sync.export_data.write_record_type_to_cache")
    def test_write_records_to_cache_empty_map(self, mock_write_record_type):
        """Test handling of empty record map.
        
        When provided with an empty record map, the function should
        return early without calling write_record_type_to_cache.
        """
        write_records_to_cache(type_to_record_maps={})
        mock_write_record_type.assert_not_called()
    
    @patch("services.backfill.sync.export_data.write_record_type_to_cache")
    def test_write_records_to_cache_with_records(self, mock_write_record_type):
        """Test writing multiple record types to cache.
        
        When provided with a map of record types to records, the function should:
        - Call write_record_type_to_cache for each record type
        - Pass the correct parameters for each call
        """
        type_to_record_maps = {
            "post": [{"text": "Post 1"}, {"text": "Post 2"}],
            "like": [{"uri": "at://like1"}]
        }
        batch_size = 5
        
        write_records_to_cache(
            type_to_record_maps=type_to_record_maps,
            batch_size=batch_size
        )
        
        # Verify write_record_type_to_cache was called for each record type
        assert mock_write_record_type.call_count == 2
        
        # Verify correct parameters were passed for each record type
        mock_write_record_type.assert_any_call(
            record_type="post",
            records=type_to_record_maps["post"],
            batch_size=batch_size
        )
        mock_write_record_type.assert_any_call(
            record_type="like",
            records=type_to_record_maps["like"],
            batch_size=batch_size
        )
    
    @patch("services.backfill.sync.export_data.write_record_type_to_cache")
    def test_write_records_to_cache_default_batch_size(self, mock_write_record_type):
        """Test writing records with default batch size.
        
        When batch_size is not provided, the function should pass None as the 
        batch_size to write_record_type_to_cache.
        """
        type_to_record_maps = {
            "post": [{"text": "Post 1"}]
        }
        
        write_records_to_cache(type_to_record_maps=type_to_record_maps)
        
        # Verify batch_size=None was passed to write_record_type_to_cache
        mock_write_record_type.assert_called_once_with(
            record_type="post",
            records=type_to_record_maps["post"],
            batch_size=None
        ) 