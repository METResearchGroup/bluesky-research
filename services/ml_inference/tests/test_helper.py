"""Tests for helper.py.

This test suite verifies the functionality of ML inference helper functions:
- determine_backfill_latest_timestamp: Handles backfill timestamp calculation
- get_posts_to_classify: Retrieves and processes posts for classification
"""

import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

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


class TestGetPostsToClassify:
    """Tests for get_posts_to_classify function."""

    @pytest.fixture
    def mock_queue(self):
        """Mock Queue dependency."""
        with patch("services.ml_inference.helper.Queue") as mock_queue:
            yield mock_queue.return_value

    def test_invalid_inference_type(self):
        """Test invalid inference type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid inference type: invalid"):
            get_posts_to_classify("invalid")

    def test_empty_queue(self, mock_queue):
        """Test handling of empty queue."""
        mock_queue.load_dict_items_from_queue.return_value = []
        
        result = get_posts_to_classify("perspective_api")
        
        assert result == []
        mock_queue.load_dict_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=None,
            min_timestamp=None,
            status="pending"
        )

    def test_with_previous_metadata(self, mock_queue):
        """Test using previous run metadata for filtering."""
        metadata = {
            "metadata": json.dumps({
                "latest_id_classified": 123,
                "inference_timestamp": "2024-01-01"
            })
        }
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test",
                "text": "test post",
                "batch_id": 124,
                "batch_metadata": json.dumps({"source": "test_source"})
            }
        ]

        result = get_posts_to_classify(
            "perspective_api",
            previous_run_metadata=metadata
        )

        assert len(result) == 1
        assert result[0]["uri"] == "test"
        assert result[0]["text"] == "test post"
        assert result[0]["batch_id"] == 124
        assert result[0]["batch_metadata"] == json.dumps({"source": "test_source"})
        mock_queue.load_dict_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=123,
            min_timestamp="2024-01-01",
            status="pending"
        )

    def test_with_timestamp_override(self, mock_queue):
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

    def test_deduplication_with_batch_info(self, mock_queue):
        """Test deduplication preserves correct batch information."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "post1",
                "batch_id": 1,
                "batch_metadata": json.dumps({"batch": "test_batch"})
            },
            {
                "uri": "test1",  # Duplicate URI
                "text": "post2",
                "batch_id": 1,
                "batch_metadata": json.dumps({"batch": "test_batch"})
            },
            {
                "uri": "test2",
                "text": "post3",
                "batch_id": 1,
                "batch_metadata": json.dumps({"batch": "test_batch"})
            }
        ]

        result = get_posts_to_classify("perspective_api")

        assert len(result) == 2  # Should be deduplicated
        uris = [post["uri"] for post in result]
        assert sorted(uris) == ["test1", "test2"]
        
        # Verify batch information
        for post in result:
            assert post["batch_id"] == 1
            assert json.loads(post["batch_metadata"]) == {"batch": "test_batch"}
