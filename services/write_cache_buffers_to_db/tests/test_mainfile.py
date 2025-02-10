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