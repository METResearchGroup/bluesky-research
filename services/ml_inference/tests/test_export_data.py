import pytest
from unittest.mock import Mock
from freezegun import freeze_time

from lib.db.queue import Queue
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)
from services.ml_inference.models import LabelWithBatchId


class TestReturnFailedLabelsToInputQueue:
    """Tests for return_failed_labels_to_input_queue function.
    
    This test class verifies that failed labels are properly returned to the input queue
    across all inference types (perspective_api, sociopolitical, ime).
    """

    @pytest.mark.parametrize(
        "inference_type",
        ["perspective_api", "sociopolitical", "ime", "valence_classifier"]
    )
    def test_empty_failed_labels(self, inference_type):
        """Test handling of empty failed labels list.
        
        Args:
            inference_type: The type of inference being tested
            
        Expected behavior:
            - Should return early without making any queue operations
            - Should work identically for all inference types
        """
        mock_queue = Mock(spec=Queue)
        return_failed_labels_to_input_queue(
            inference_type=inference_type,
            failed_label_models=[],
            input_queue=mock_queue
        )
        mock_queue.batch_add_items_to_queue.assert_not_called()

    @pytest.mark.parametrize(
        "inference_type",
        ["perspective_api", "sociopolitical", "ime", "valence_classifier"]
    )
    @freeze_time("2024-01-01 12:00:00")
    def test_single_failed_label(self, inference_type):
        """Test processing single failed label.
        
        Args:
            inference_type: The type of inference being tested
            
        Expected behavior:
            - Should add single failed label to input queue with correct metadata
            - Should preserve original text and URI
            - Should use the label's reason as model_reason
            - Should work identically for all inference types
        """
        failed_model = LabelWithBatchId(
            batch_id=1,
            uri="test_uri",
            text="test_text",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
            reason="API_ERROR",
            label_timestamp="2024-01-01-12:00:00"
        )
        
        mock_queue = Mock(spec=Queue)
        return_failed_labels_to_input_queue(
            inference_type=inference_type,
            failed_label_models=[failed_model],
            input_queue=mock_queue
        )
            
        mock_queue.batch_add_items_to_queue.assert_called_once_with(
            items=[{"uri": "test_uri", "text": "test_text"}],
            batch_size=None,
            metadata={
                "reason": f"failed_label_{inference_type}",
                "model_reason": "API_ERROR",
                "label_timestamp": "2024-01-01-12:00:00"
            }
        )

    @pytest.mark.parametrize(
        "inference_type",
        ["perspective_api", "sociopolitical", "ime", "valence_classifier"]
    )
    @freeze_time("2024-01-01 12:00:00")
    def test_multiple_failed_labels(self, inference_type):
        """Test processing multiple failed labels.
        
        Args:
            inference_type: The type of inference being tested
            
        Expected behavior:
            - Should add multiple failed labels to input queue with correct metadata
            - Should preserve original text and URI for all labels
            - Should use the first label's reason as model_reason
            - Should respect provided batch size
            - Should work identically for all inference types
        """
        failed_models = [
            LabelWithBatchId(
                batch_id=i,
                uri=f"uri_{i}",
                text=f"text_{i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=False,
                reason="API_ERROR",
                label_timestamp="2024-01-01-12:00:00"
            )
            for i in range(3)
        ]
        
        mock_queue = Mock(spec=Queue)
        return_failed_labels_to_input_queue(
            inference_type=inference_type,
            failed_label_models=failed_models,
            batch_size=2,
            input_queue=mock_queue
        )
        
        mock_queue.batch_add_items_to_queue.assert_called_once_with(
            items=[{"uri": f"uri_{i}", "text": f"text_{i}"} for i in range(3)],
            batch_size=2,
            metadata={
                "reason": f"failed_label_{inference_type}",
                "model_reason": "API_ERROR",
                "label_timestamp": "2024-01-01-12:00:00"
            }
        )


class TestWritePostsToCache:
    """Tests for write_posts_to_cache function.
    
    This test class verifies that successfully labeled posts are properly written to cache
    and removed from input queue across all inference types.
    """

    @pytest.mark.parametrize(
        "inference_type",
        ["perspective_api", "sociopolitical", "ime", "valence_classifier"]
    )
    def test_empty_posts(self, inference_type):
        """Test handling of empty posts list.
        
        Args:
            inference_type: The type of inference being tested
            
        Expected behavior:
            - Should return early without making any queue operations
            - Should work identically for all inference types
        """
        mock_input_queue = Mock(spec=Queue)
        mock_output_queue = Mock(spec=Queue)
        write_posts_to_cache(
            inference_type=inference_type,
            posts=[],
            input_queue=mock_input_queue,
            output_queue=mock_output_queue
        )
        mock_output_queue.batch_add_items_to_queue.assert_not_called()
        mock_input_queue.batch_delete_items_by_ids.assert_not_called()

    @pytest.mark.parametrize(
        "inference_type",
        ["perspective_api", "sociopolitical", "ime", "valence_classifier"]
    )
    def test_write_and_delete_posts(self, inference_type):
        """Test writing posts to cache and deleting from input queue.
        
        Args:
            inference_type: The type of inference being tested
            
        Expected behavior:
            - Should write posts to output queue with correct batch size
            - Should delete corresponding batch IDs from input queue
            - Should maintain data integrity during transfer
            - Should work identically for all inference types
        """
        posts = [
            LabelWithBatchId(
                batch_id=i,
                uri=f"uri_{i}",
                text=f"text_{i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=True,
                label_timestamp="2024-01-01-12:00:00"
            )
            for i in range(3)
        ]
        
        mock_input_queue = Mock(spec=Queue)
        mock_output_queue = Mock(spec=Queue)
        write_posts_to_cache(
            inference_type=inference_type,
            posts=posts,
            batch_size=2,
            input_queue=mock_input_queue,
            output_queue=mock_output_queue
        )
            
        # Verify posts were written (converted to dicts)
        mock_output_queue.batch_add_items_to_queue.assert_called_once()
        call_args = mock_output_queue.batch_add_items_to_queue.call_args
        assert call_args[1]["batch_size"] == 2
        written_items = call_args[1]["items"]
        assert len(written_items) == 3
        assert all(isinstance(item, dict) for item in written_items)

        # Verify the batch IDs were deleted, ignoring order
        actual_call = mock_input_queue.batch_delete_items_by_ids.call_args[1]["ids"]
        expected_batch_ids = {post.batch_id for post in posts}
        assert set(actual_call) == expected_batch_ids
