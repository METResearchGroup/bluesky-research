"""Tests for main.py.

This test suite verifies the functionality of the repartition service:
- Proper handling of payload parameters
- Correct function calls to helper module
- Appropriate logging
"""

from unittest.mock import patch, MagicMock, call, ANY
import os
import datetime
import unittest
import pytest

from services.repartition_service.main import repartition_service
from services.repartition_service.helper import OperationResult, OperationStatus

# Mock service metadata for testing
MOCK_SERVICE_METADATA = {
    "test_service": {
        "local_prefix": "/data/test_service",
        "timestamp_field": "created_at",
        "partition_key": "created_at",
    }
}

@pytest.fixture(autouse=True)
def mock_service_metadata():
    """Automatically mock service metadata for all tests."""
    with patch("services.repartition_service.helper.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA):
        with patch("services.repartition_service.main.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA):
            yield

class TestRepartitionService(unittest.TestCase):
    """Tests for repartition_service function."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.default_start_date = "2024-09-28"
        self.default_end_date = "2025-12-01"
        self.default_exclude_dates = ["2024-10-08"]

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_default_payload(self, mock_repartition):
        """Test function with empty payload uses default values."""
        payload = {"service": "test_service"}
        repartition_service(payload)

        mock_repartition.assert_called_once_with(
            start_date=self.default_start_date,
            end_date=self.default_end_date,
            service="test_service",
            new_service_partition_key="created_at",
            exclude_partition_dates=self.default_exclude_dates,
            use_parallel=False,
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_custom_payload(self, mock_repartition):
        """Test function with custom payload values."""
        payload = {
            "service": "test_service",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "exclude_partition_dates": ["2024-02-01"],
            "new_service_partition_key": "indexed_at",
        }
        
        repartition_service(payload)
        
        mock_repartition.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-12-31",
            service="test_service",
            new_service_partition_key="indexed_at",
            exclude_partition_dates=["2024-02-01"],
            use_parallel=False,
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_partial_payload(self, mock_repartition):
        """Test function with partially specified payload uses defaults for missing values."""
        payload = {
            "service": "test_service",
            "start_date": "2024-01-01",
            "exclude_partition_dates": ["2024-10-08"],
        }
        repartition_service(payload)

        mock_repartition.assert_called_once_with(
            start_date="2024-01-01",
            end_date=self.default_end_date,
            service="test_service",
            new_service_partition_key="created_at",
            exclude_partition_dates=["2024-10-08"],
            use_parallel=False,
        )

    def test_missing_required_service(self):
        """Test function raises error when required service is missing."""
        payload = {}
        with self.assertRaises(KeyError):
            repartition_service(payload)

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    @patch("services.repartition_service.main.logger")
    def test_logging(self, mock_logger, mock_repartition):
        """Test that appropriate logging occurs."""
        payload = {
            "service": "test_service",
            "start_date": "2024-01-01",
        }
        repartition_service(payload)

        mock_logger.info.assert_has_calls([
            call(
                "Repartitioning test_service data from 2024-01-01 to 2025-12-01 "
                "using partition key 'created_at', excluding dates: ['2024-10-08']"
            ),
            call("Finished repartitioning test_service data."),
        ]) 