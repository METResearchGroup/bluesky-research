"""Tests for main.py.

This test suite verifies the functionality of the map_uri_to_created_at service:
- Proper handling of payload parameters
- Correct function calls to helper module
- Appropriate logging
"""

from unittest.mock import patch

import pytest

from services.map_uri_to_created_at.main import map_uri_to_created_at


class TestMapUriToCreatedAt:
    """Tests for map_uri_to_created_at function."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.default_start_date = "2024-09-28"
        self.default_end_date = "2025-12-01"
        self.default_exclude_dates = ["2024-10-08"]

    @patch("services.map_uri_to_created_at.main.map_uris_to_created_at_for_partition_dates")
    def test_default_payload(self, mock_map):
        """Test function with empty payload uses default values."""
        payload = {}
        
        map_uri_to_created_at(payload)
        
        mock_map.assert_called_once_with(
            start_date=self.default_start_date,
            end_date=self.default_end_date,
            exclude_partition_dates=self.default_exclude_dates
        )

    @patch("services.map_uri_to_created_at.main.map_uris_to_created_at_for_partition_dates")
    def test_custom_payload(self, mock_map):
        """Test function with custom payload values."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "exclude_partition_dates": ["2024-02-01"]
        }
        
        map_uri_to_created_at(payload)
        
        mock_map.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-12-31",
            exclude_partition_dates=["2024-02-01"]
        )

    @patch("services.map_uri_to_created_at.main.map_uris_to_created_at_for_partition_dates")
    def test_partial_payload(self, mock_map):
        """Test function with partially specified payload uses defaults for missing values."""
        payload = {
            "start_date": "2024-01-01"
        }
        
        map_uri_to_created_at(payload)
        
        mock_map.assert_called_once_with(
            start_date="2024-01-01",
            end_date=self.default_end_date,
            exclude_partition_dates=self.default_exclude_dates
        )

    @patch("services.map_uri_to_created_at.main.map_uris_to_created_at_for_partition_dates")
    @patch("services.map_uri_to_created_at.main.logger")
    def test_logging(self, mock_logger, mock_map):
        """Test that appropriate logging occurs."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "exclude_partition_dates": ["2024-02-01"]
        }
        
        map_uri_to_created_at(payload)
        
        # Verify logging calls
        mock_logger.info.assert_any_call(
            "Mapping URIs to creation timestamps from 2024-01-01 to 2024-12-31, "
            "excluding dates: ['2024-02-01']"
        )
        mock_logger.info.assert_any_call("Finished mapping URIs to creation timestamps.") 