"""Tests for helper.py.

This test suite verifies the functionality of ML inference helper functions:
- get_posts_to_classify: Retrieves and processes posts for classification
"""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from services.ml_inference.helper import (
    get_posts_to_classify,
    orchestrate_classification,
)
from services.ml_inference.config import InferenceConfig
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


class TestOrchestrateClassification:
    """Tests for orchestrate_classification function."""

    @pytest.fixture
    def sample_posts(self) -> list[PostToLabelModel]:
        """Sample PostToLabelModel objects for testing orchestration limiting."""
        return [
            PostToLabelModel(
                uri=f"at://example/{i}",
                text=f"post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(5)
        ]

    def test_max_records_per_run_limits_posts_correctly(self, sample_posts):
        """Test that max_records_per_run slices posts before calling classification function."""
        # Arrange
        classification_func = Mock(return_value={"ok": True})
        config = InferenceConfig(
            inference_type="perspective_api",
            queue_inference_type="perspective_api",
            classification_func=classification_func,
        )

        with patch(
            "services.ml_inference.helper.determine_backfill_latest_timestamp",
            return_value=None,
        ), patch(
            "services.ml_inference.helper.get_posts_to_classify",
            return_value=sample_posts,
        ):
            # Act
            result = orchestrate_classification(
                config=config,
                max_records_per_run=2,
            )

        # Assert
        assert result.total_classified_posts == 2
        classification_func.assert_called_once()
        call_kwargs = classification_func.call_args.kwargs
        assert len(call_kwargs["posts"]) == 2

    def test_max_records_per_run_none_processes_all_posts(self, sample_posts):
        """Test that max_records_per_run=None leaves the post list unchanged."""
        # Arrange
        classification_func = Mock(return_value={"ok": True})
        config = InferenceConfig(
            inference_type="perspective_api",
            queue_inference_type="perspective_api",
            classification_func=classification_func,
        )

        with patch(
            "services.ml_inference.helper.determine_backfill_latest_timestamp",
            return_value=None,
        ), patch(
            "services.ml_inference.helper.get_posts_to_classify",
            return_value=sample_posts,
        ):
            # Act
            result = orchestrate_classification(config=config, max_records_per_run=None)

        # Assert
        assert result.total_classified_posts == 5
        call_kwargs = classification_func.call_args.kwargs
        assert len(call_kwargs["posts"]) == 5

    def test_max_records_per_run_zero_processes_no_posts(self, sample_posts):
        """Test that max_records_per_run=0 results in an early return with zero posts classified."""
        # Arrange
        classification_func = Mock(return_value={"ok": True})
        config = InferenceConfig(
            inference_type="perspective_api",
            queue_inference_type="perspective_api",
            classification_func=classification_func,
            empty_result_message="No posts to classify. Exiting...",
        )

        with patch(
            "services.ml_inference.helper.determine_backfill_latest_timestamp",
            return_value=None,
        ), patch(
            "services.ml_inference.helper.get_posts_to_classify",
            return_value=sample_posts,
        ):
            # Act
            result = orchestrate_classification(config=config, max_records_per_run=0)

        # Assert
        assert result.total_classified_posts == 0
        classification_func.assert_not_called()

    def test_max_records_per_run_negative_raises_value_error(self, sample_posts):
        """Test that max_records_per_run < 0 raises ValueError."""
        # Arrange
        classification_func = Mock(return_value={"ok": True})
        config = InferenceConfig(
            inference_type="perspective_api",
            queue_inference_type="perspective_api",
            classification_func=classification_func,
        )

        with patch(
            "services.ml_inference.helper.determine_backfill_latest_timestamp",
            return_value=None,
        ), patch(
            "services.ml_inference.helper.get_posts_to_classify",
            return_value=sample_posts,
        ):
            # Act & Assert
            with pytest.raises(ValueError, match="max_records_per_run must be >= 0"):
                orchestrate_classification(config=config, max_records_per_run=-1)

    def test_logs_when_limiting_occurs(self, sample_posts):
        """Test that orchestration logs when it limits the number of posts."""
        # Arrange
        classification_func = Mock(return_value={"ok": True})
        config = InferenceConfig(
            inference_type="perspective_api",
            queue_inference_type="perspective_api",
            classification_func=classification_func,
        )

        with patch(
            "services.ml_inference.helper.determine_backfill_latest_timestamp",
            return_value=None,
        ), patch(
            "services.ml_inference.helper.get_posts_to_classify",
            return_value=sample_posts,
        ), patch(
            "services.ml_inference.helper.logger"
        ) as mock_logger:
            # Act
            orchestrate_classification(config=config, max_records_per_run=2)

            # Assert
            assert any(
                "Limited posts from" in str(call.args[0])
                for call in mock_logger.info.call_args_list
            )
