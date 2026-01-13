"""Tests for helper.py.

This test suite verifies the functionality of ML inference helper functions:
- get_posts_to_classify: Retrieves and processes posts for classification
"""

import json
from datetime import datetime
from unittest.mock import patch

import pytest

from services.ml_inference.helper import (
    get_posts_to_classify,
)
from services.ml_inference.models import PostToLabelModel

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
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"})
            }
        ]
        result = get_posts_to_classify("perspective_api")
        assert len(result) == 1
        assert isinstance(result[0], PostToLabelModel)
        assert result[0].uri == "test1"
        assert result[0].text == "test post 1"

    def test_deduplication(self, mock_queue):
        """Test that posts are properly deduplicated by URI."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "first version",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"})
            },
            {
                "uri": "test1",
                "text": "second version",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"})
            },
            {
                "uri": "test2",
                "text": "unique post",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 2,
                "batch_metadata": json.dumps({"source": "test"})
            }
        ]
        result = get_posts_to_classify("perspective_api")
        assert len(result) == 2
        uris = {post.uri for post in result}
        assert uris == {"test1", "test2"}

    def test_filtering_invalid_posts(self, mock_queue):
        """Test that invalid posts are filtered out."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"})
            },  # Empty text
            {
                "uri": "test2",
                "text": "a",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 2,
                "batch_metadata": json.dumps({"source": "test"})
            },  # Too short
            {
                "uri": "test3",
                "text": "valid post",
                "preprocessing_timestamp": None,
                "batch_id": 3,
                "batch_metadata": json.dumps({"source": "test"})
            },  # Missing timestamp
            {
                "uri": "test4",
                "text": "valid post",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 4,
                "batch_metadata": json.dumps({"source": "test"})
            }  # Valid
        ]
        result = get_posts_to_classify("perspective_api")
        assert len(result) == 1
        assert result[0].uri == "test4"

    def test_custom_columns(self, mock_queue):
        """Test requesting specific columns."""
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "test post",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"}),
                "extra_field": "should not appear"
            }
        ]
        result = get_posts_to_classify("perspective_api", columns=["uri", "text", "preprocessing_timestamp", "batch_id", "batch_metadata"])
        assert len(result) == 1
        assert result[0].uri == "test1"
        assert result[0].text == "test post"
        assert result[0].preprocessing_timestamp == "2024-01-01-12:00:00"
        assert result[0].batch_id == 1
        assert result[0].batch_metadata == json.dumps({"source": "test"})
        assert not hasattr(result[0], "extra_field")

    def test_previous_metadata_handling(self, mock_queue):
        """Test using previous run metadata."""
        metadata = {
            "metadata": json.dumps({
                "latest_id_classified": 123,
                "inference_timestamp": "2024-01-01"
            })
        }
        mock_queue.load_dict_items_from_queue.return_value = [
            {
                "uri": "test1",
                "text": "test post",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": json.dumps({"source": "test"})
            }
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
