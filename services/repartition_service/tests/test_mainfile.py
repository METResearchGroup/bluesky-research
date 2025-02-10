"""Tests for main.py.

This test suite verifies the functionality of the repartition service:
- Proper handling of payload parameters
- Correct function calls to helper module
- Appropriate logging
"""

from unittest.mock import patch, MagicMock

import pytest

from services.repartition_service.main import repartition_service
from services.repartition_service.helper import OperationResult, OperationStatus

# Mock service metadata for testing
MOCK_SERVICE_METADATA = {
    "test_service": {
        "local_prefix": "/data/test_service",
        "timestamp_field": "created_at",
    }
}

class TestRepartitionService:
    """Tests for repartition_service function."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.default_start_date = "2024-09-28"
        self.default_end_date = "2025-12-01"
        self.default_exclude_dates = ["2024-10-08"]

    @patch("services.repartition_service.main.repartition_data_for_partition_dates_parallel")
    @patch("services.repartition_service.main.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA)
    def test_default_payload(self, mock_parallel):
        """Test function with empty payload uses default values."""
        payload = {"service": "test_service"}
        mock_parallel.return_value = {
            "2024-01-01": OperationResult(status=OperationStatus.SUCCESS)
        }
        
        repartition_service(payload)
        
        mock_parallel.assert_called_once_with(
            start_date=self.default_start_date,
            end_date=self.default_end_date,
            service="test_service",
            new_service_partition_key="created_at",
            exclude_partition_dates=self.default_exclude_dates,
            parallel_config=None
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates_parallel")
    @patch("services.repartition_service.main.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA)
    def test_custom_payload(self, mock_parallel):
        """Test function with custom payload values."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "service": "test_service",
            "new_service_partition_key": "indexed_at",
            "exclude_partition_dates": ["2024-02-01"]
        }
        mock_parallel.return_value = {
            "2024-01-01": OperationResult(status=OperationStatus.SUCCESS)
        }
        
        repartition_service(payload)
        
        mock_parallel.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-12-31",
            service="test_service",
            new_service_partition_key="indexed_at",
            exclude_partition_dates=["2024-02-01"],
            parallel_config=None
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates_parallel")
    @patch("services.repartition_service.main.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA)
    def test_partial_payload(self, mock_parallel):
        """Test function with partially specified payload uses defaults for missing values."""
        payload = {
            "start_date": "2024-01-01",
            "service": "test_service"
        }
        mock_parallel.return_value = {
            "2024-01-01": OperationResult(status=OperationStatus.SUCCESS)
        }
        
        repartition_service(payload)
        
        mock_parallel.assert_called_once_with(
            start_date="2024-01-01",
            end_date=self.default_end_date,
            service="test_service",
            new_service_partition_key="created_at",
            exclude_partition_dates=self.default_exclude_dates,
            parallel_config=None
        )

    def test_missing_required_service(self):
        """Test handling of missing required service parameter."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31"
        }
        
        with pytest.raises(KeyError, match="service"):
            repartition_service(payload)

    @patch("services.repartition_service.main.repartition_data_for_partition_dates_parallel")
    @patch("services.repartition_service.main.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA)
    @patch("services.repartition_service.main.logger")
    def test_logging(self, mock_logger, mock_parallel):
        """Test that appropriate logging occurs."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "service": "test_service",
            "new_service_partition_key": "indexed_at",
            "exclude_partition_dates": ["2024-02-01"]
        }
        mock_parallel.return_value = {
            "2024-01-01": OperationResult(status=OperationStatus.SUCCESS),
            "2024-01-02": OperationResult(status=OperationStatus.FAILED),
            "2024-01-03": OperationResult(status=OperationStatus.SKIPPED),
        }
        
        repartition_service(payload)
        
        # Verify logging calls
        mock_logger.info.assert_any_call(
            "Repartitioning test_service data from 2024-01-01 to 2024-12-31 "
            "using partition key 'indexed_at', excluding dates: ['2024-02-01']"
        )
        mock_logger.info.assert_any_call("Using parallel processing")
        mock_logger.info.assert_any_call(
            "Parallel processing complete: 1 succeeded, 1 failed, 1 skipped"
        )
        mock_logger.info.assert_any_call("Finished repartitioning test_service data.") 