"""Tests for main.py.

This test suite verifies the functionality of the get_preprocessed_posts_used_in_feeds service:
- Proper handling of payload parameters
- Correct function calls to helper module
- Appropriate logging
"""

from unittest.mock import patch, MagicMock

import pytest

from services.get_preprocessed_posts_used_in_feeds.main import get_preprocessed_posts_used_in_feeds


class TestGetPreprocessedPostsUsedInFeeds:
    """Tests for get_preprocessed_posts_used_in_feeds function."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.default_start_date = "2024-09-28"
        self.default_end_date = "2025-12-01"
        self.default_exclude_dates = ["2024-10-08"]

    @patch("services.get_preprocessed_posts_used_in_feeds.main.get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates")
    def test_default_payload(self, mock_export):
        """Test function with empty payload uses default values."""
        payload = {}
        
        get_preprocessed_posts_used_in_feeds(payload)
        
        mock_export.assert_called_once_with(
            start_date=self.default_start_date,
            end_date=self.default_end_date,
            exclude_partition_dates=self.default_exclude_dates
        )

    @patch("services.get_preprocessed_posts_used_in_feeds.main.get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates")
    def test_custom_payload(self, mock_export):
        """Test function with custom payload values."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "exclude_partition_dates": ["2024-02-01"]
        }
        
        get_preprocessed_posts_used_in_feeds(payload)
        
        mock_export.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-12-31",
            exclude_partition_dates=["2024-02-01"]
        )

    @patch("services.get_preprocessed_posts_used_in_feeds.main.get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates")
    def test_partial_payload(self, mock_export):
        """Test function with partially specified payload uses defaults for missing values."""
        payload = {
            "start_date": "2024-01-01"
        }
        
        get_preprocessed_posts_used_in_feeds(payload)
        
        mock_export.assert_called_once_with(
            start_date="2024-01-01",
            end_date=self.default_end_date,
            exclude_partition_dates=self.default_exclude_dates
        )

    @patch("services.get_preprocessed_posts_used_in_feeds.main.get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates")
    @patch("services.get_preprocessed_posts_used_in_feeds.main.logger")
    def test_logging(self, mock_logger, mock_export):
        """Test that appropriate logging occurs."""
        payload = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "exclude_partition_dates": ["2024-02-01"]
        }
        
        get_preprocessed_posts_used_in_feeds(payload)
        
        # Verify logging calls
        mock_logger.info.assert_any_call(
            "Getting preprocessed posts used in feeds from 2024-01-01 to 2024-12-31, "
            "excluding dates: ['2024-02-01']"
        )
        mock_logger.info.assert_any_call("Finished getting preprocessed posts used in feeds.") 