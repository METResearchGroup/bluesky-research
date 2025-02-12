"""Tests for helper.py.

This test suite verifies the functionality of ML inference helper functions:
- determine_backfill_latest_timestamp: Handles backfill timestamp calculation
- get_posts_to_classify: Retrieves and processes posts for classification
"""

import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

from services.ml_inference.helper import (
    determine_backfill_latest_timestamp,
    get_posts_to_classify,
)

# Create a mock datetime class that supports subtraction
class MockDateTime:
    def __init__(self, dt):
        self.dt = dt
        
    def strftime(self, fmt):
        return self.dt.strftime(fmt)
        
    def __sub__(self, other):
        if isinstance(other, datetime):
            return MockDateTime(self.dt - other)
        return MockDateTime(self.dt - other)


class TestDetermineBackfillLatestTimestamp:
    """Tests for determine_backfill_latest_timestamp function."""

    @pytest.mark.parametrize("duration,period,expected", [
        (None, "days", None),
        (None, "hours", None), 
        (7, "invalid", None),
    ])
    def test_invalid_inputs(self, duration, period, expected):
        """Test handling of invalid/None inputs."""
        result = determine_backfill_latest_timestamp(duration, period)
        assert result == expected

    @patch("services.ml_inference.helper.datetime")
    def test_backfill_days(self, mock_datetime):
        """Test backfill with days period."""
        mock_now = MockDateTime(datetime(2024, 1, 7, 12, 0, tzinfo=timezone.utc))
        mock_datetime.now.return_value = mock_now
        mock_datetime.timezone = timezone
        mock_datetime.timedelta = timedelta

        result = determine_backfill_latest_timestamp(7, "days")
        
        assert result == "2023-12-31-12:00:00"
        mock_datetime.now.assert_called_once()

    @patch("services.ml_inference.helper.datetime") 
    def test_backfill_hours(self, mock_datetime):
        """Test backfill with hours period."""
        mock_now = MockDateTime(datetime(2024, 1, 7, 12, 0, tzinfo=timezone.utc))
        mock_datetime.now.return_value = mock_now
        mock_datetime.timezone = timezone

        result = determine_backfill_latest_timestamp(24, "hours")
        
        assert result == "2024-01-06-12:00:00"
        mock_datetime.now.assert_called_once()


@pytest.fixture
def mock_queue():
    """Create a mock queue for testing."""
    with patch("services.ml_inference.helper.Queue") as mock:
        yield mock.return_value


class TestGetPostsToClassify:
    """Tests for get_posts_to_classify function."""

    def test_invalid_inference_type(self):
        """Test that invalid inference type raises ValueError."""
        with pytest.raises(ValueError):
            get_posts_to_classify("invalid")

    def test_empty_queue(self, mock_queue):
        """Test handling of empty queue."""
        mock_queue.load_dict_items_from_queue.return_value = []
        result = get_posts_to_classify("perspective_api")
        assert result == []

    def test_basic_post_loading(self, mock_queue):
        """Test basic post loading functionality."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "test post 1",
                "created_at": "2024-01-01",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"})
            }
        ]
        result = get_posts_to_classify("perspective_api")
        assert len(result) == 1
        assert result[0]["uri"] == "test1"
        assert result[0]["text"] == "test post 1"

    def test_deduplication(self, mock_queue):
        """Test that posts are properly deduplicated by URI."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {"uri": "test1", "text": "first version"},
            {"uri": "test1", "text": "second version"},
            {"uri": "test2", "text": "unique post"}
        ]
        result = get_posts_to_classify("perspective_api")
        assert len(result) == 2
        uris = {post["uri"] for post in result}
        assert uris == {"test1", "test2"}

    def test_filtering_invalid_posts(self, mock_queue):
        """Test that invalid posts are filtered out."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {"uri": "test1", "text": "", "preprocessing_timestamp": "2024-01-01"},  # Empty text
            {"uri": "test2", "text": "a", "preprocessing_timestamp": "2024-01-01"},  # Too short
            {"uri": "test3", "text": "valid post", "preprocessing_timestamp": None},  # Missing timestamp
            {"uri": "test4", "text": "valid post", "preprocessing_timestamp": "2024-01-01"}  # Valid
        ]
        result = get_posts_to_classify("perspective_api")
        assert len(result) == 1
        assert result[0]["uri"] == "test4"

    def test_custom_columns(self, mock_queue):
        """Test requesting specific columns."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "test post",
                "extra_field": "should not appear"
            }
        ]
        result = get_posts_to_classify("perspective_api", columns=["uri", "text"])
        assert len(result) == 1
        assert set(result[0].keys()) == {"uri", "text"}
        assert "extra_field" not in result[0]

    def test_previous_metadata_handling(self, mock_queue):
        """Test using previous run metadata."""
        metadata = {
            "metadata": json.dumps({
                "latest_id_classified": 123,
                "inference_timestamp": "2024-01-01"
            })
        }
        mock_queue.load_dict_items_from_queue.return_value = [
            {"uri": "test1", "text": "test post"}
        ]
        get_posts_to_classify("perspective_api", previous_run_metadata=metadata)
        mock_queue.load_dict_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=123,
            min_timestamp="2024-01-01",
            status="pending"
        )

    def test_timestamp_override(self, mock_queue):
        """Test timestamp parameter overrides metadata timestamp."""
        metadata = {
            "metadata": json.dumps({
                "latest_id_classified": 123,
                "inference_timestamp": "2024-01-01"
            })
        }
        override_timestamp = "2023-12-31"
        mock_queue.load_dict_items_from_queue.return_value = []
        get_posts_to_classify(
            "perspective_api",
            timestamp=override_timestamp,
            previous_run_metadata=metadata
        )
        mock_queue.load_dict_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=123,
            min_timestamp=override_timestamp,
            status="pending"
        )
