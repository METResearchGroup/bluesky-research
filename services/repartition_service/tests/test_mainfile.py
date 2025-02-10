"""Tests for main.py.

This test suite verifies the functionality of the repartition service:
- Proper handling of payload parameters
- Correct function calls to helper module
- Appropriate logging
"""

from unittest.mock import patch

import pytest

from services.repartition_service.main import repartition_service


class TestRepartitionService:
    """Tests for repartition_service function."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.default_start_date = "2024-09-28"
        self.default_end_date = "2025-12-01"
        self.default_exclude_dates = ["2024-10-08"]
        self.default_partition_key = "created_at"

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_default_payload(self, mock_repartition):
        """Test function with minimal payload uses default values."""
        payload = {
            "service": "test_service"
        }
        
        repartition_service(payload)
        
        mock_repartition.assert_called_once_with(
            start_date=self.default_start_date,
            end_date=self.default_end_date,
            service="test_service",
            new_service_partition_key=self.default_partition_key,
            exclude_partition_dates=self.default_exclude_dates
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_custom_payload(self, mock_repartition):
        """Test function with custom payload values."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "service": "test_service",
            "new_service_partition_key": "indexed_at",
            "exclude_partition_dates": ["2024-02-01"]
        }
        
        repartition_service(payload)
        
        mock_repartition.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-12-31",
            service="test_service",
            new_service_partition_key="indexed_at",
            exclude_partition_dates=["2024-02-01"]
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_partial_payload(self, mock_repartition):
        """Test function with partially specified payload uses defaults for missing values."""
        payload = {
            "service": "test_service",
            "start_date": "2024-01-01"
        }
        
        repartition_service(payload)
        
        mock_repartition.assert_called_once_with(
            start_date="2024-01-01",
            end_date=self.default_end_date,
            service="test_service",
            new_service_partition_key=self.default_partition_key,
            exclude_partition_dates=self.default_exclude_dates
        )

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    def test_missing_required_service(self, mock_repartition):
        """Test function raises error when required service parameter is missing."""
        payload = {
            "start_date": "2024-01-01"
        }
        
        with pytest.raises(KeyError):
            repartition_service(payload)
        
        mock_repartition.assert_not_called()

    @patch("services.repartition_service.main.repartition_data_for_partition_dates")
    @patch("services.repartition_service.main.logger")
    def test_logging(self, mock_logger, mock_repartition):
        """Test that appropriate logging occurs."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "service": "test_service",
            "new_service_partition_key": "indexed_at",
            "exclude_partition_dates": ["2024-02-01"]
        }
        
        repartition_service(payload)
        
        # Verify logging calls
        mock_logger.info.assert_any_call(
            "Repartitioning test_service data from 2024-01-01 to 2024-12-31 "
            "using partition key 'indexed_at', "
            "excluding dates: ['2024-02-01']"
        )
        mock_logger.info.assert_any_call("Finished repartitioning test_service data.") 