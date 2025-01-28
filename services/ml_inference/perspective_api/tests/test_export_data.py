import pytest
from unittest.mock import patch

from services.ml_inference.perspective_api.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)


class TestReturnFailedLabelsToInputQueue:
    """Tests for return_failed_labels_to_input_queue function."""
    
    @pytest.fixture
    def mock_input_queue(self):
        """Mock input Queue dependency."""
        with patch("services.ml_inference.perspective_api.export_data.input_queue") as mock:
            yield mock
            
    @pytest.fixture
    def mock_generate_datetime(self):
        """Mock generate_current_datetime_str dependency."""
        with patch("services.ml_inference.perspective_api.export_data.generate_current_datetime_str") as mock:
            mock.return_value = "2024-01-01-12:00:00"
            yield mock

    def test_empty_failed_labels(self, mock_input_queue):
        """Test handling of empty failed labels list."""
        return_failed_labels_to_input_queue([])
        mock_input_queue.batch_add_items_to_queue.assert_not_called()

    def test_single_failed_label(self, mock_input_queue, mock_generate_datetime):
        """Test processing single failed label."""
        failed_model = {
            "uri": "test_uri",
            "text": "test_text",
            "reason": "API_ERROR",
            "was_successfully_labeled": False,
            "label_timestamp": "2024-01-01-12:00:00"
        }
        
        return_failed_labels_to_input_queue([failed_model])
        
        mock_input_queue.batch_add_items_to_queue.assert_called_once_with(
            items=[{"uri": "test_uri", "text": "test_text"}],
            batch_size=None,
            metadata={
                "reason": "failed_label_perspective_api",
                "model_reason": "API_ERROR",
                "label_timestamp": "2024-01-01-12:00:00"
            }
        )

    def test_multiple_failed_labels(self, mock_input_queue, mock_generate_datetime):
        """Test processing multiple failed labels."""
        failed_models = [
            {
                "uri": f"uri_{i}",
                "text": f"text_{i}", 
                "reason": "API_ERROR",
                "was_successfully_labeled": False,
                "label_timestamp": "2024-01-01-12:00:00"
            }
            for i in range(3)
        ]
        
        return_failed_labels_to_input_queue(failed_models, batch_size=2)
        
        mock_input_queue.batch_add_items_to_queue.assert_called_once_with(
            items=[{"uri": f"uri_{i}", "text": f"text_{i}"} for i in range(3)],
            batch_size=2,
            metadata={
                "reason": "failed_label_perspective_api",
                "model_reason": "API_ERROR",
                "label_timestamp": "2024-01-01-12:00:00"
            }
        )


class TestWritePostsToCache:
    """Tests for write_posts_to_cache function."""
    
    @pytest.fixture
    def mock_output_queue(self):
        """Mock output Queue dependency."""
        with patch("services.ml_inference.perspective_api.export_data.output_queue") as mock:
            yield mock

    @pytest.fixture
    def mock_input_queue(self):
        """Mock input Queue dependency."""
        with patch("services.ml_inference.perspective_api.export_data.input_queue") as mock:
            yield mock

    @pytest.fixture
    def mock_logger(self):
        """Mock logger."""
        with patch("services.ml_inference.perspective_api.export_data.logger") as mock:
            yield mock

    def test_empty_posts(self, mock_output_queue, mock_input_queue):
        """Test handling of empty posts list."""
        write_posts_to_cache([])
        mock_output_queue.batch_add_items_to_queue.assert_not_called()
        mock_input_queue.batch_delete_items_by_ids.assert_not_called()

    def test_single_post(self, mock_output_queue, mock_input_queue, mock_logger):
        """Test writing single post to cache and deleting from input queue."""
        post = {
            "uri": "test_uri",
            "text": "test_text",
            "toxicity": 0.8,
            "severe_toxicity": 0.3,
            "was_successfully_labeled": True,
            "label_timestamp": "2024-01-01-12:00:00",
            "batch_id": 123  # Add batch_id for deletion tracking
        }
        
        write_posts_to_cache([post])
        
        # Verify output queue write
        mock_output_queue.batch_add_items_to_queue.assert_called_once_with(
            items=[post],
            batch_size=None
        )
        
        # Verify input queue deletion
        mock_input_queue.batch_delete_items_by_ids.assert_called_once_with(
            ids=[123]
        )
        
        # Verify logging
        mock_logger.info.assert_any_call("Adding 1 posts to the output queue.")
        mock_logger.info.assert_any_call("Deleting 1 batch IDs from the input queue.")

    def test_multiple_posts_with_batch(self, mock_output_queue, mock_input_queue, mock_logger):
        """Test writing multiple posts with batch size and deleting from input queue."""
        posts = [
            {
                "uri": f"uri_{i}",
                "text": f"text_{i}",
                "toxicity": 0.5,
                "severe_toxicity": 0.2,
                "was_successfully_labeled": True,
                "label_timestamp": "2024-01-01-12:00:00",
                "batch_id": 100 + i  # Unique batch_id for each post
            }
            for i in range(3)
        ]
        
        write_posts_to_cache(posts, batch_size=2)
        
        # Verify output queue write
        mock_output_queue.batch_add_items_to_queue.assert_called_once_with(
            items=posts,
            batch_size=2
        )
        
        # Verify input queue deletion
        mock_input_queue.batch_delete_items_by_ids.assert_called_once_with(
            ids=[100, 101, 102]  # Should delete all batch_ids
        )
        
        # Verify logging
        mock_logger.info.assert_any_call("Adding 3 posts to the output queue.")
        mock_logger.info.assert_any_call("Deleting 3 batch IDs from the input queue.") 