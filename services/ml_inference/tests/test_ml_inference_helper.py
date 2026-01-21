"""Tests for helper.py.

This test suite verifies the functionality of ML inference helper functions:
- get_posts_to_classify: Retrieves and processes posts for classification
"""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from services.ml_inference.helper import (
    cap_max_records_for_run,
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


class TestCapMaxRecordsForRun:
    """Tests for cap_max_records_for_run function."""

    @pytest.fixture
    def posts_single_batch(self) -> list[PostToLabelModel]:
        """Multiple posts with same batch_id."""
        return [
            PostToLabelModel(
                uri=f"at://example/batch1_{i}",
                text=f"post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
            for i in range(5)
        ]

    @pytest.fixture
    def posts_multiple_batches(self) -> list[PostToLabelModel]:
        """Posts with different batch_ids."""
        return [
            PostToLabelModel(
                uri=f"at://example/batch{i//2}_{i%2}",
                text=f"post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i // 2,
                batch_metadata="{}",
            )
            for i in range(6)
        ]

    @pytest.fixture
    def posts_mixed_batch_sizes(self) -> list[PostToLabelModel]:
        """Posts with varying batch sizes."""
        posts = []
        # Batch 1: 3 posts
        for i in range(3):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch1_{i}",
                    text=f"batch1 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=1,
                    batch_metadata="{}",
                )
            )
        # Batch 2: 2 posts
        for i in range(2):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch2_{i}",
                    text=f"batch2 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=2,
                    batch_metadata="{}",
                )
            )
        # Batch 3: 3 posts
        for i in range(3):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch3_{i}",
                    text=f"batch3 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=3,
                    batch_metadata="{}",
                )
            )
        return posts

    def test_returns_empty_list_when_max_records_is_zero(self, posts_single_batch):
        """Test returns empty list when max_records_per_run is 0."""
        # Arrange
        max_records = 0
        expected = []

        # Act
        result = cap_max_records_for_run(posts_single_batch, max_records)

        # Assert
        assert result == expected

    def test_returns_all_posts_when_limit_exceeds_total(self, posts_single_batch):
        """Test returns all posts when limit exceeds total count."""
        # Arrange
        max_records = 10
        expected_count = len(posts_single_batch)

        # Act
        result = cap_max_records_for_run(posts_single_batch, max_records)

        # Assert
        assert len(result) == expected_count
        assert result == posts_single_batch

    def test_returns_all_posts_when_limit_equals_total(self, posts_single_batch):
        """Test returns all posts when limit equals total count."""
        # Arrange
        max_records = len(posts_single_batch)
        expected_count = len(posts_single_batch)

        # Act
        result = cap_max_records_for_run(posts_single_batch, max_records)

        # Assert
        assert len(result) == expected_count
        assert result == posts_single_batch

    def test_includes_only_complete_batches_when_capping(self, posts_mixed_batch_sizes):
        """Test includes only complete batches when capping would split a batch."""
        # Arrange
        # Posts: batch 1 (3 posts), batch 2 (2 posts), batch 3 (3 posts)
        max_records = 4
        expected_batch_ids = {1}  # Only batch 1 should be included
        expected_count = 3  # Only 3 posts from batch 1

        # Act
        result = cap_max_records_for_run(posts_mixed_batch_sizes, max_records)

        # Assert
        assert len(result) == expected_count
        result_batch_ids = {post.batch_id for post in result}
        assert result_batch_ids == expected_batch_ids
        # Verify all posts from batch 1 are included
        batch_1_posts = [p for p in posts_mixed_batch_sizes if p.batch_id == 1]
        assert result == batch_1_posts

    def test_includes_multiple_complete_batches_up_to_limit(self, posts_mixed_batch_sizes):
        """Test includes multiple complete batches up to the limit."""
        # Arrange
        # Posts: batch 1 (3 posts), batch 2 (2 posts), batch 3 (3 posts)
        max_records = 5
        expected_batch_ids = {1, 2}  # Batches 1 and 2 should be included
        expected_count = 5  # 3 + 2 = 5 posts

        # Act
        result = cap_max_records_for_run(posts_mixed_batch_sizes, max_records)

        # Assert
        assert len(result) == expected_count
        result_batch_ids = {post.batch_id for post in result}
        assert result_batch_ids == expected_batch_ids

    def test_excludes_batch_that_would_exceed_limit(self):
        """Test excludes batch that would exceed the limit."""
        # Arrange
        posts = []
        # Batch 1: 2 posts
        for i in range(2):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch1_{i}",
                    text=f"batch1 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=1,
                    batch_metadata="{}",
                )
            )
        # Batch 2: 5 posts
        for i in range(5):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch2_{i}",
                    text=f"batch2 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=2,
                    batch_metadata="{}",
                )
            )
        max_records = 3
        expected_batch_ids = {1}  # Only batch 1 should be included
        expected_count = 2  # Only 2 posts from batch 1

        # Act
        result = cap_max_records_for_run(posts, max_records)

        # Assert
        assert len(result) == expected_count
        result_batch_ids = {post.batch_id for post in result}
        assert result_batch_ids == expected_batch_ids

    def test_handles_single_post_batches(self, posts_multiple_batches):
        """Test handles single post batches correctly."""
        # Arrange
        # Each post has different batch_id (0, 0, 1, 1, 2, 2)
        # So batches are: batch 0 (2 posts), batch 1 (2 posts), batch 2 (2 posts)
        # But let's create posts where each has different batch_id
        posts = [
            PostToLabelModel(
                uri=f"at://example/{i}",
                text=f"post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(5)
        ]
        max_records = 3
        expected_count = 3  # First 3 posts (each is a complete batch)

        # Act
        result = cap_max_records_for_run(posts, max_records)

        # Assert
        assert len(result) == expected_count
        assert all(post.batch_id == i for i, post in enumerate(result))

    def test_handles_empty_input_list(self):
        """Test handles empty input list."""
        # Arrange
        posts = []
        max_records = 10
        expected = []

        # Act
        result = cap_max_records_for_run(posts, max_records)

        # Assert
        assert result == expected

    def test_handles_single_batch_larger_than_limit(self, posts_single_batch):
        """Test handles single batch larger than limit by returning empty list."""
        # Arrange
        # posts_single_batch has 5 posts all with batch_id=1
        max_records = 3
        expected = []  # Can't include partial batch, so return empty

        # Act
        result = cap_max_records_for_run(posts_single_batch, max_records)

        # Assert
        assert result == expected

    def test_preserves_batch_order(self):
        """Test preserves batch order as they appear in input."""
        # Arrange
        posts = []
        # Batch 3: 2 posts
        for i in range(2):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch3_{i}",
                    text=f"batch3 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=3,
                    batch_metadata="{}",
                )
            )
        # Batch 1: 2 posts
        for i in range(2):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch1_{i}",
                    text=f"batch1 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=1,
                    batch_metadata="{}",
                )
            )
        # Batch 2: 2 posts
        for i in range(2):
            posts.append(
                PostToLabelModel(
                    uri=f"at://example/batch2_{i}",
                    text=f"batch2 post {i}",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=2,
                    batch_metadata="{}",
                )
            )
        max_records = 5
        expected_batch_order = [3, 1]  # Batches 3 and 1 should be included in order

        # Act
        result = cap_max_records_for_run(posts, max_records)

        # Assert
        assert len(result) == 4  # 2 + 2 = 4 posts
        # Check batch order
        result_batch_ids = [post.batch_id for post in result]
        assert result_batch_ids[:2] == [3, 3]  # First batch 3
        assert result_batch_ids[2:4] == [1, 1]  # Then batch 1

    def test_preserves_post_order_within_batches(self):
        """Test preserves post order within batches."""
        # Arrange
        posts = [
            PostToLabelModel(
                uri=f"at://example/batch1_{i}",
                text=f"batch1 post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=1,
                batch_metadata="{}",
            )
            for i in range(3)
        ]
        max_records = 10
        expected_uris = [f"at://example/batch1_{i}" for i in range(3)]

        # Act
        result = cap_max_records_for_run(posts, max_records)

        # Assert
        result_uris = [post.uri for post in result]
        assert result_uris == expected_uris

    def test_raises_value_error_for_negative_max_records(self, posts_single_batch):
        """Test raises ValueError for negative max_records_per_run."""
        # Arrange
        max_records = -1
        expected_error_message = "max_records_per_run must be >= 0"

        # Act & Assert
        with pytest.raises(ValueError, match=expected_error_message):
            cap_max_records_for_run(posts_single_batch, max_records)

    def test_logs_when_limiting_occurs(self, posts_mixed_batch_sizes):
        """Test logs when limiting occurs."""
        # Arrange
        max_records = 4

        with patch("services.ml_inference.helper.logger") as mock_logger:
            # Act
            cap_max_records_for_run(posts_mixed_batch_sizes, max_records)

            # Assert
            assert mock_logger.info.called
            log_calls = [str(call.args[0]) for call in mock_logger.info.call_args_list]
            assert any("Limited posts from" in call for call in log_calls)

    def test_logs_batch_count_when_capping(self, posts_mixed_batch_sizes):
        """Test log message includes number of complete batches."""
        # Arrange
        max_records = 4

        with patch("services.ml_inference.helper.logger") as mock_logger:
            # Act
            cap_max_records_for_run(posts_mixed_batch_sizes, max_records)

            # Assert
            assert mock_logger.info.called
            log_calls = [str(call.args[0]) for call in mock_logger.info.call_args_list]
            assert any("complete batches" in call for call in log_calls)
            # Check that it mentions the batch count
            batch_count_log = [call for call in log_calls if "complete batches" in call][0]
            assert "included" in batch_count_log

    def test_does_not_log_when_no_limiting_occurs(self, posts_single_batch):
        """Test does not log when no limiting occurs."""
        # Arrange
        max_records = 10

        with patch("services.ml_inference.helper.logger") as mock_logger:
            # Act
            cap_max_records_for_run(posts_single_batch, max_records)

            # Assert
            log_calls = [str(call.args[0]) for call in mock_logger.info.call_args_list]
            assert not any("Limited posts from" in call for call in log_calls)
