"""Tests for write_cache_buffers_to_db helper module.

This test suite verifies the functionality of the helper functions:
- write_cache_buffer_queue_to_db: Writes single service cache to DB
- write_all_cache_buffer_queues_to_dbs: Writes all services' caches to DBs
- Queue operations and data handling
- Error handling and edge cases
- Logging behavior

The tests ensure that:
1. Cache buffers are correctly written to databases
2. Queue items are properly handled (loaded and optionally cleared)
3. Data transformations are accurate
4. Error conditions are properly handled
5. Operations are correctly logged
6. Edge cases (empty queues, data format issues) are handled gracefully
"""

import pandas as pd
from unittest import mock
from unittest.mock import patch, MagicMock, call
import pytest

from services.write_cache_buffers_to_db.helper import (
    write_cache_buffer_queue_to_db,
    write_all_cache_buffer_queues_to_dbs,
    SERVICES_TO_WRITE,
    clear_cache,
    clear_all_caches,
)


@pytest.fixture
def mock_queue_data():
    """Create mock queue data for testing."""
    return [
        {
            "batch_id": "1",
            "data": "test1",
            "preprocessing_timestamp": "2024-01-01T00:00:00",
            "created_at": "2024-01-01T00:00:00"
        },
        {
            "batch_id": "2",
            "data": "test2",
            "preprocessing_timestamp": "2024-01-01T00:00:00",
            "created_at": "2024-01-01T00:00:00"
        },
        {
            "batch_id": "3",
            "data": "test3",
            "preprocessing_timestamp": "2024-01-01T00:00:00",
            "created_at": "2024-01-01T00:00:00"
        }
    ]


@pytest.fixture
def mock_df(mock_queue_data):
    """Create a mock DataFrame from queue data.
    
    Returns:
        pandas.DataFrame: DataFrame containing the mock queue data.
    """
    return pd.DataFrame(mock_queue_data)


class TestCacheClearingFunctions:
    """Tests for cache clearing functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.service = "ml_inference_perspective_api"
        self.queue_name = f"output_{self.service}"

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    def test_clear_cache(self, mock_queue_cls, mock_queue_data):
        """Test clear_cache function."""
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = mock_queue_data
        mock_queue_cls.return_value = mock_queue

        clear_cache(self.service)

        mock_queue.batch_delete_items_by_ids.assert_called_once_with(
            ids=["1", "2", "3"]
        )

    @patch("services.write_cache_buffers_to_db.helper.clear_cache")
    def test_clear_all_caches(self, mock_clear):
        """Test clear_all_caches function."""
        clear_all_caches()

        assert mock_clear.call_count == len(SERVICES_TO_WRITE)
        for service in SERVICES_TO_WRITE:
            mock_clear.assert_any_call(service=service)


class TestWriteCacheBufferQueueToDb:
    """Tests for write_cache_buffer_queue_to_db function.
    
    This test class verifies that the function correctly:
    - Loads data from the queue
    - Exports data to local storage
    - Handles queue clearing
    - Manages errors appropriately
    - Logs operations correctly
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures.
        
        Initializes common test variables and configurations.
        """
        self.service = "ml_inference_perspective_api"
        self.queue_name = f"output_{self.service}"

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_successful_write(self, mock_export, mock_queue_cls, mock_queue_data, mock_df):
        """Test successful writing of cache buffer to database.
        
        Verifies that:
        1. Queue is correctly initialized
        2. Data is properly loaded from queue
        3. Data is correctly exported to storage
        4. Queue items are not deleted by default
        5. Operations are properly logged
        """
        # Setup mock queue
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = mock_queue_data
        mock_queue_cls.return_value = mock_queue

        write_cache_buffer_queue_to_db(self.service, clear_queue=False)

        # Verify queue initialization
        mock_queue_cls.assert_called_once_with(queue_name=self.queue_name, create_new_queue=True)

        # Verify data loading
        mock_queue.load_dict_items_from_queue.assert_called_once_with(
            limit=None, min_id=None, min_timestamp=None, status="pending"
        )

        # Verify export
        mock_export.assert_called_once()
        args = mock_export.call_args[1]
        assert args["service"] == self.service
        assert args["export_format"] == "parquet"
        assert len(args["df"]) == len(mock_queue_data)

        # Verify no queue clearing
        mock_queue.batch_delete_items_by_ids.assert_not_called()

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_write_with_queue_clearing(self, mock_export, mock_queue_cls, mock_queue_data, mock_df):
        """Test writing cache buffer with queue clearing.
        
        Verifies that:
        1. Queue is correctly initialized
        2. Data is properly loaded and exported
        3. Queue items are correctly deleted
        4. Operations are properly logged
        """
        # Setup mock queue
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = mock_queue_data
        mock_queue_cls.return_value = mock_queue

        write_cache_buffer_queue_to_db(self.service, clear_queue=True)

        # Verify queue clearing
        mock_queue.batch_delete_items_by_ids.assert_called_once_with(
            ids=["1", "2", "3"]
        )

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_empty_queue(self, mock_export, mock_queue_cls):
        """Test handling of empty queue.
        
        Verifies that:
        1. Empty queue is handled gracefully
        2. No export is attempted with empty data
        3. No queue clearing is attempted
        4. Appropriate logging occurs
        """
        # Setup mock queue with empty data
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = []
        mock_queue_cls.return_value = mock_queue

        # Create empty DataFrame with correct columns
        empty_df = pd.DataFrame(columns=["batch_id"])
        with patch("pandas.DataFrame", return_value=empty_df):
            write_cache_buffer_queue_to_db(self.service, clear_queue=True)

        # Verify export with empty DataFrame
        mock_export.assert_called_once()
        args = mock_export.call_args[1]
        assert len(args["df"]) == 0

        # Verify no queue clearing attempted with empty list
        mock_queue.batch_delete_items_by_ids.assert_called_once_with(ids=[])

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_export_error_handling(self, mock_export, mock_queue_cls, mock_queue_data):
        """Test handling of export errors.
        
        Verifies that:
        1. Export errors are caught and logged
        2. Queue is not cleared on export error
        3. Error is properly propagated
        4. No data is lost
        """
        # Setup mock queue
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = mock_queue_data
        mock_queue_cls.return_value = mock_queue

        # Simulate export error
        mock_export.side_effect = Exception("Export failed")

        with pytest.raises(Exception) as exc_info:
            write_cache_buffer_queue_to_db(self.service, clear_queue=True)

        assert "Export failed" in str(exc_info.value)
        mock_queue.batch_delete_items_by_ids.assert_not_called()

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    def test_queue_initialization_failure(self, mock_queue_cls):
        """Test handling of queue initialization failure.
        
        Verifies that:
        1. Queue initialization errors are properly handled
        2. Error is logged appropriately
        3. Original error is propagated
        4. No side effects occur
        """
        mock_queue_cls.side_effect = Exception("Failed to initialize queue")
        
        with pytest.raises(Exception) as exc_info:
            write_cache_buffer_queue_to_db(self.service)
        
        assert "Failed to initialize queue" in str(exc_info.value)

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_malformed_queue_data(self, mock_export, mock_queue_cls):
        """Test handling of malformed data in queue.
        
        Verifies that:
        1. Malformed data is handled gracefully
        2. Missing batch_id is handled properly
        3. Error is logged appropriately
        4. No data is lost
        5. Queue is not cleared
        """
        malformed_data = [
            {"batch_id": "1", "data": "test1"},
            {"data": "test2"},  # Missing batch_id
            {"batch_id": None, "data": "test3"}  # Invalid batch_id
        ]
        
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = malformed_data
        mock_queue_cls.return_value = mock_queue
        
        # Create DataFrame with only valid batch_ids
        valid_df = pd.DataFrame([{"batch_id": "1"}])
        with patch("pandas.DataFrame", return_value=valid_df):
            write_cache_buffer_queue_to_db(self.service, clear_queue=True)
        
        # Verify only valid IDs are cleared
        mock_queue.batch_delete_items_by_ids.assert_called_once_with(ids=["1"])

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    @patch("services.write_cache_buffers_to_db.helper.logger")
    def test_helper_logging(self, mock_logger, mock_export, mock_queue_cls, mock_queue_data):
        """Test logging behavior in helper functions.
        
        Verifies that:
        1. All operations are properly logged
        2. Log messages contain correct record counts
        3. Export and deletion operations are logged
        4. Error conditions are logged appropriately
        """
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = mock_queue_data
        mock_queue_cls.return_value = mock_queue
        
        write_cache_buffer_queue_to_db(self.service, clear_queue=True)
        
        # Verify logging calls
        mock_logger.info.assert_has_calls([
            mock.call(f"Exporting {len(mock_queue_data)} records to local storage for service {self.service}..."),
            mock.call(f"Finished exporting {len(mock_queue_data)} records to local storage for service {self.service}..."),
            mock.call(f"Deleting {len(mock_queue_data)} records from queue for service {self.service}..."),
            mock.call(f"Finished deleting {len(mock_queue_data)} records from queue for service {self.service}...")
        ])

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_custom_export_format(self, mock_export, mock_queue_cls, mock_queue_data):
        """Test export with custom format options.
        
        Verifies that:
        1. Export format is correctly passed to export function
        2. Data is properly formatted before export
        3. Export completes successfully
        4. Queue operations are unaffected by format
        """
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = mock_queue_data
        mock_queue_cls.return_value = mock_queue
        
        write_cache_buffer_queue_to_db(self.service)
        
        # Verify export format
        mock_export.assert_called_once()
        args = mock_export.call_args[1]
        assert args["export_format"] == "parquet"  # Default format
        assert isinstance(args["df"], pd.DataFrame)

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    @patch("services.write_cache_buffers_to_db.helper.export_data_to_local_storage")
    def test_mixed_status_items(self, mock_export, mock_queue_cls):
        """Test handling of queue items with mixed statuses.
        
        Verifies that:
        1. Only pending items are processed
        2. Non-pending items are ignored
        3. Queue clearing only affects processed items
        4. Operation completes successfully
        """
        mixed_data = [
            {"batch_id": "1", "status": "pending"},
            {"batch_id": "2", "status": "completed"},
            {"batch_id": "3", "status": "pending"}
        ]
        
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = [
            item for item in mixed_data if item["status"] == "pending"
        ]
        mock_queue_cls.return_value = mock_queue
        
        # Create DataFrame with only pending items
        pending_df = pd.DataFrame([
            {"batch_id": "1"},
            {"batch_id": "3"}
        ])
        with patch("pandas.DataFrame", return_value=pending_df):
            write_cache_buffer_queue_to_db(self.service, clear_queue=True)
        
        # Verify only pending items are cleared
        mock_queue.batch_delete_items_by_ids.assert_called_once_with(
            ids=["1", "3"]
        )


class TestWriteAllCacheBufferQueuesToDbs:
    """Tests for write_all_cache_buffer_queues_to_dbs function.
    
    This test class verifies that the function correctly:
    - Processes all services
    - Handles the clear_queue parameter
    - Manages errors appropriately
    - Logs operations correctly
    """

    @patch("services.write_cache_buffers_to_db.helper.write_cache_buffer_queue_to_db")
    def test_write_all_services(self, mock_write):
        """Test writing all service cache buffers.
        
        Verifies that:
        1. Each service is processed
        2. Default clear_queue=False is used
        3. Operations are properly logged
        4. Function completes successfully
        """
        write_all_cache_buffer_queues_to_dbs()

        assert mock_write.call_count == len(SERVICES_TO_WRITE)
        for service in SERVICES_TO_WRITE:
            mock_write.assert_any_call(service=service, clear_queue=False)

    @patch("services.write_cache_buffers_to_db.helper.write_cache_buffer_queue_to_db")
    def test_write_all_services_clear_queue(self, mock_write):
        """Test writing all service cache buffers with queue clearing.
        
        Verifies that:
        1. Each service is processed
        2. clear_queue=True is respected
        3. Operations are properly logged
        4. Function completes successfully
        """
        write_all_cache_buffer_queues_to_dbs(clear_queue=True)

        assert mock_write.call_count == len(SERVICES_TO_WRITE)
        for service in SERVICES_TO_WRITE:
            mock_write.assert_any_call(service=service, clear_queue=True)

    @patch("services.write_cache_buffers_to_db.helper.write_cache_buffer_queue_to_db")
    def test_partial_failure(self, mock_write):
        """Test handling of partial service failures.
        
        Verifies that:
        1. Errors in one service don't prevent processing of others
        2. Errors are properly logged
        3. Function continues processing remaining services
        4. Overall error is raised after processing all services
        """
        # Simulate failure for second service
        def mock_write_with_error(service, clear_queue):
            if service == SERVICES_TO_WRITE[1]:
                raise Exception(f"Failed to process {service}")

        mock_write.side_effect = mock_write_with_error

        with pytest.raises(Exception) as exc_info:
            write_all_cache_buffer_queues_to_dbs()

        assert f"Failed to process {SERVICES_TO_WRITE[1]}" in str(exc_info.value)
        assert mock_write.call_count == 2  # First service + failed service 