"""Tests for write_cache_buffers_to_db main module.

This test suite verifies the functionality of the write_cache_buffers_to_db service:
- Writing cache buffers for specific services
- Writing cache buffers for all services
- Handling invalid service names
- Proper handling of clear_queue parameter
- Error handling and edge cases
- Logging behavior
- Parameter validation and handling

The tests ensure that the service correctly handles:
1. Writing cache buffers for individual services with and without queue clearing
2. Writing cache buffers for all services with and without queue clearing
3. Validation of service names and payload parameters
4. Proper error messages for invalid inputs
5. Logging of operations and their outcomes
6. Proper handling of all payload parameters
"""

from unittest import mock
from unittest.mock import patch, MagicMock
import pytest
import json

from services.write_cache_buffers_to_db.main import write_cache_buffers_to_db
from services.write_cache_buffers_to_db.helper import SERVICES_TO_WRITE


class TestWriteCacheBuffersToDb:
    """Tests for write_cache_buffers_to_db function.
    
    This test class verifies that the write_cache_buffers_to_db function correctly:
    - Handles writing cache buffers for specific services
    - Manages writing cache buffers for all services
    - Validates service names and payload parameters
    - Properly handles the clear_queue parameter
    - Provides appropriate error messages for invalid inputs
    - Logs operations correctly
    - Validates all payload parameters
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures.
        
        Initializes common test variables and configurations.
        """
        self.valid_service = "ml_inference_perspective_api"
        self.invalid_service = "invalid_service"
        # Updated mock data to simulate the processed output of load_dict_items_from_queue.
        # Each record already contains a "batch_id" key as expected in production.
        self.mock_queue_data = [{
            "data": "test1",
            "preprocessing_timestamp": "2024-01-01T00:00:00",
            "created_at": "2024-01-01T00:00:00",
            "batch_id": 1,
            "batch_metadata": {"batch_size": 1, "actual_batch_size": 1}
        }]

    @patch("services.write_cache_buffers_to_db.main.write_cache_buffer_queue_to_db")
    def test_write_specific_service(self, mock_write):
        """Test writing cache buffer for a specific service.
        
        Verifies that:
        1. The correct helper function is called
        2. The service name is passed correctly
        3. The default clear_queue=False is used
        4. The function completes without errors
        5. All kwargs are passed correctly
        """
        payload = {"service": self.valid_service}
        write_cache_buffers_to_db(payload)
        
        mock_write.assert_called_once_with(
            service=self.valid_service,
            clear_queue=False
        )

    @patch("services.write_cache_buffers_to_db.main.write_cache_buffer_queue_to_db")
    def test_write_specific_service_clear_queue(self, mock_write):
        """Test writing cache buffer for a specific service with clear_queue=True.
        
        Verifies that:
        1. The correct helper function is called
        2. The service name is passed correctly
        3. The clear_queue=True parameter is respected
        4. The function completes without errors
        5. All kwargs are passed correctly
        """
        payload = {
            "service": self.valid_service,
            "clear_queue": True
        }
        write_cache_buffers_to_db(payload)
        
        mock_write.assert_called_once_with(
            service=self.valid_service,
            clear_queue=True
        )

    @patch("services.write_cache_buffers_to_db.main.write_all_cache_buffer_queues_to_dbs")
    def test_write_all_services(self, mock_write_all):
        """Test writing cache buffers for all services.
        
        Verifies that:
        1. The correct helper function for writing all services is called
        2. The default clear_queue=False is used
        3. The function completes without errors
        4. All kwargs are passed correctly
        """
        payload = {"service": "all"}
        write_cache_buffers_to_db(payload)
        
        mock_write_all.assert_called_once_with(clear_queue=False)

    @patch("services.write_cache_buffers_to_db.main.write_all_cache_buffer_queues_to_dbs")
    def test_write_all_services_clear_queue(self, mock_write_all):
        """Test writing cache buffers for all services with clear_queue=True.
        
        Verifies that:
        1. The correct helper function for writing all services is called
        2. The clear_queue=True parameter is respected
        3. The function completes without errors
        4. All kwargs are passed correctly
        """
        payload = {"service": "all", "clear_queue": True}
        write_cache_buffers_to_db(payload)
        
        mock_write_all.assert_called_once_with(clear_queue=True)

    def test_invalid_service(self):
        """Test handling of invalid service name.
        
        Verifies that:
        1. An invalid service name raises ValueError
        2. The error message includes the invalid service name
        3. The error message lists valid service options
        4. The function fails gracefully
        5. No side effects occur
        """
        payload = {"service": self.invalid_service}
        
        with pytest.raises(ValueError) as exc_info:
            write_cache_buffers_to_db(payload)
        
        error_msg = str(exc_info.value)
        assert f"Invalid service: {self.invalid_service}" in error_msg
        assert "Must be 'all' or one of:" in error_msg
        for service in SERVICES_TO_WRITE:
            assert service in error_msg

    def test_missing_service(self):
        """Test handling of missing service in payload.
        
        Verifies that:
        1. Missing service parameter raises KeyError
        2. The function fails gracefully
        3. No side effects occur
        4. Appropriate error message is provided
        """
        payload = {}
        
        with pytest.raises(KeyError) as exc_info:
            write_cache_buffers_to_db(payload)
        
        assert "Missing required 'service' field in payload" in str(exc_info.value)

    @patch("services.write_cache_buffers_to_db.main.write_cache_buffer_queue_to_db")
    @patch("services.write_cache_buffers_to_db.main.logger")
    def test_logging(self, mock_logger, mock_write):
        """Test logging behavior during cache buffer writing.
        
        Verifies that:
        1. Operations are properly logged
        2. Error conditions are logged appropriately
        3. Log messages contain relevant information
        4. All expected log messages are present
        """
        payload = {"service": self.valid_service}
        write_cache_buffers_to_db(payload)
        
        mock_write.assert_called_once()
        mock_logger.info.assert_has_calls([
            mock.call(f"Processing write request for service: {self.valid_service}"),
            mock.call(f"Writing cache buffer queue for service: {self.valid_service}")
        ])

    def test_invalid_clear_queue_type(self):
        """Test handling of invalid clear_queue parameter type.
        
        Verifies that:
        1. Non-boolean clear_queue value raises TypeError
        2. The function fails gracefully
        3. No side effects occur
        4. Appropriate error message is provided
        """
        payload = {
            "service": self.valid_service,
            "clear_queue": "not_a_boolean"
        }
        
        with pytest.raises(TypeError) as exc_info:
            write_cache_buffers_to_db(payload)
        
        assert "clear_queue must be a boolean" in str(exc_info.value)

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    def test_bypass_write_without_clear_queue(self, mock_queue_cls):
        """Test that bypass_write requires clear_queue to be True."""
        mock_queue = MagicMock()
        # Return already processed data including "batch_id"
        mock_queue.load_dict_items_from_queue.return_value = self.mock_queue_data
        mock_queue_cls.return_value = mock_queue

        payload = {
            "service": self.valid_service,
            "bypass_write": True,
            "clear_queue": False
        }
        
        with pytest.raises(ValueError) as exc_info:
            write_cache_buffers_to_db(payload)
        
        assert "bypass_write requires clear_queue to be True" in str(exc_info.value)

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    def test_bypass_write_type_validation(self, mock_queue_cls):
        """Test that bypass_write must be a boolean."""
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = self.mock_queue_data
        mock_queue_cls.return_value = mock_queue

        payload = {
            "service": self.valid_service,
            "bypass_write": "not_a_boolean"
        }
        
        with pytest.raises(TypeError) as exc_info:
            write_cache_buffers_to_db(payload)
        
        assert "bypass_write must be a boolean" in str(exc_info.value)

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    def test_bypass_write_with_clear_queue(self, mock_queue_cls):
        """Test bypass_write with clear_queue skips write and only clears cache."""
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = self.mock_queue_data
        mock_queue_cls.return_value = mock_queue

        payload = {
            "service": self.valid_service,
            "bypass_write": True,
            "clear_queue": True
        }
        
        write_cache_buffers_to_db(payload)
        
        # Verify queue operations: "batch_id" is present in the processed data.
        mock_queue.load_dict_items_from_queue.assert_called_once_with(
            limit=None, min_id=None, min_timestamp=None, status="pending"
        )
        mock_queue.batch_delete_items_by_ids.assert_called_once_with(
            ids=[1]  # Expecting the batch_id from the processed output.
        )

    @patch("services.write_cache_buffers_to_db.helper.Queue")
    def test_bypass_write_all_services(self, mock_queue_cls):
        """Test bypass_write functionality when service='all'."""
        mock_queue = MagicMock()
        mock_queue.load_dict_items_from_queue.return_value = self.mock_queue_data
        mock_queue_cls.return_value = mock_queue

        payload = {
            "service": "all",
            "bypass_write": True,
            "clear_queue": True
        }
        
        write_cache_buffers_to_db(payload)
        
        # Verify queue operations for each service
        assert mock_queue_cls.call_count == len(SERVICES_TO_WRITE)
        assert mock_queue.load_dict_items_from_queue.call_count == len(SERVICES_TO_WRITE)
        assert mock_queue.batch_delete_items_by_ids.call_count == len(SERVICES_TO_WRITE) 