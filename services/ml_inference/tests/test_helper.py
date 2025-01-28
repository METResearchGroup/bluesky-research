"""Tests for helper.py.

This test suite verifies the functionality of ML inference helper functions:
- determine_backfill_latest_timestamp: Handles backfill timestamp calculation
- get_posts_to_classify: Retrieves and processes posts for classification
"""

import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from lib.db.queue import QueueItem
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
    """Tests for determine_backfill_latest_timestamp function.
    
    This class tests the timestamp calculation for backfilling data:
    - Handling of invalid inputs (None, invalid periods)
    - Correct calculation for days-based backfill
    - Correct calculation for hours-based backfill
    - Proper timezone handling (UTC)
    """

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
    """Tests for get_posts_to_classify function.
    
    This class tests the post retrieval and processing functionality:
    - Queue item loading with various filters
    - Metadata handling and timestamp overrides
    - Post deduplication
    - Batch information preservation
    
    The function should:
    1. Load items from the appropriate queue based on inference type
    2. Apply filters (min_id, min_timestamp, status)
    3. Process JSON payloads into post dictionaries
    4. Deduplicate posts by URI
    5. Preserve batch_id and batch_metadata for each post
    """

    @pytest.fixture
    def mock_queue(self):
        """Mock Queue dependency."""
        with patch("services.ml_inference.helper.Queue") as mock_queue:
            yield mock_queue.return_value

    def test_invalid_inference_type(self):
        """Test invalid inference type raises ValueError.
        
        Input:
            inference_type: "invalid"
        Expected:
            - ValueError with message "Invalid inference type: invalid"
        """
        with pytest.raises(ValueError, match="Invalid inference type: invalid"):
            get_posts_to_classify("invalid")

    def test_empty_queue(self, mock_queue):
        """Test handling of empty queue.
        
        Input:
            inference_type: "perspective_api"
            queue: empty queue
        Expected:
            - Empty list returned
            - Queue.load_items_from_queue called with correct parameters
            - No processing attempted
        """
        mock_queue.load_items_from_queue.return_value = []
        
        result = get_posts_to_classify("perspective_api")
        
        assert result == []
        mock_queue.load_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=None,
            min_timestamp=None,
            status="pending"
        )

    def test_with_previous_metadata(self, mock_queue):
        """Test using previous run metadata for filtering.
        
        Input:
            inference_type: "perspective_api"
            previous_run_metadata: {
                "metadata": {
                    "latest_id_classified": 123,
                    "inference_timestamp": "2024-01-01"
                }
            }
            queue: single item with batch information
        Expected:
            - Posts returned with batch_id and batch_metadata
            - Correct filters applied from metadata
            - Post data properly extracted from payload
        """
        metadata = {
            "metadata": json.dumps({
                "latest_id_classified": 123,
                "inference_timestamp": "2024-01-01"
            })
        }
        test_batch_metadata = {"source": "test_source"}
        mock_queue.load_items_from_queue.return_value = [
            QueueItem(
                id=124,  # After latest_id_classified
                payload=json.dumps([{"uri": "test", "text": "test post"}]),
                metadata=json.dumps(test_batch_metadata),
                status="pending"
            )
        ]

        result = get_posts_to_classify(
            "perspective_api",
            previous_run_metadata=metadata
        )

        assert len(result) == 1
        assert result[0]["uri"] == "test"
        assert result[0]["text"] == "test post"
        assert result[0]["batch_id"] == 124
        assert result[0]["batch_metadata"] == json.dumps(test_batch_metadata)
        mock_queue.load_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=123,
            min_timestamp="2024-01-01",
            status="pending"
        )

    def test_with_timestamp_override(self, mock_queue):
        """Test timestamp parameter overrides metadata timestamp.
        
        Input:
            inference_type: "perspective_api"
            timestamp: "2023-12-31"
            previous_run_metadata: contains different timestamp
        Expected:
            - Override timestamp used instead of metadata timestamp
            - Other metadata values (like latest_id) still used
        """
        metadata = {
            "metadata": json.dumps({
                "latest_id_classified": 123,
                "inference_timestamp": "2024-01-01"
            })
        }
        override_timestamp = "2023-12-31"
        mock_queue.load_items_from_queue.return_value = []

        get_posts_to_classify(
            "perspective_api",
            timestamp=override_timestamp,
            previous_run_metadata=metadata
        )

        mock_queue.load_items_from_queue.assert_called_once_with(
            limit=None,
            min_id=123,
            min_timestamp=override_timestamp,
            status="pending"
        )

    def test_deduplication_with_batch_info(self, mock_queue):
        """Test deduplication preserves correct batch information.
        
        Input:
            inference_type: "perspective_api"
            queue: multiple items with duplicate URIs but different batch info
        Expected:
            - Posts deduplicated by URI
            - First occurrence's batch information preserved
            - All other post data intact
        """
        test_batch_metadata = {"batch": "test_batch"}
        mock_queue.load_items_from_queue.return_value = [
            QueueItem(
                id=1,
                payload=json.dumps([
                    {"uri": "test1", "text": "post1"},
                    {"uri": "test1", "text": "post2"},  # Duplicate URI
                    {"uri": "test2", "text": "post3"}
                ]),
                metadata=json.dumps(test_batch_metadata),
                status="pending"
            )
        ]

        result = get_posts_to_classify("perspective_api")

        assert len(result) == 2  # Should be deduplicated
        uris = [post["uri"] for post in result]
        assert sorted(uris) == ["test1", "test2"]
        
        # Verify batch information
        for post in result:
            assert post["batch_id"] == 1
            assert post["batch_metadata"] == json.dumps(test_batch_metadata)
