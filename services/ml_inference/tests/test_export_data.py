import pytest
from unittest.mock import Mock
from freezegun import freeze_time
from pydantic import ValidationError

from lib.db.queue import Queue
from services.ml_inference.export_data import (
    attach_batch_id_to_label_dicts,
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
            items=[
                {
                    "uri": "test_uri",
                    "text": "test_text",
                    "preprocessing_timestamp": "2024-01-01-12:00:00",
                }
            ],
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
            items=[
                {
                    "uri": f"uri_{i}",
                    "text": f"text_{i}",
                    "preprocessing_timestamp": "2024-01-01-12:00:00",
                }
                for i in range(3)
            ],
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


class TestAttachBatchIdToLabelDicts:
    """Tests for attach_batch_id_to_label_dicts function."""

    def test_single_label_success(self):
        """Test attaching batch_id to a single label dict.

        Expected behavior:
            - Should create a LabelWithBatchId instance with batch_id attached
            - Should preserve all fields from the original label dict
            - Should validate required fields are present
        """
        # Arrange
        labels = [
            {
                "uri": "test_uri_1",
                "text": "test text",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            }
        ]
        uri_to_batch_id = {"test_uri_1": 123}

        # Act
        result = attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], LabelWithBatchId)
        assert result[0].batch_id == 123
        assert result[0].uri == "test_uri_1"
        assert result[0].text == "test text"
        assert result[0].preprocessing_timestamp == "2024-01-01-12:00:00"
        assert result[0].was_successfully_labeled is True

    def test_multiple_labels_success(self):
        """Test attaching batch_id to multiple label dicts.

        Expected behavior:
            - Should create LabelWithBatchId instances for all labels
            - Should correctly map each URI to its corresponding batch_id
            - Should preserve all fields from original label dicts
        """
        # Arrange
        labels = [
            {
                "uri": "test_uri_1",
                "text": "text 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            },
            {
                "uri": "test_uri_2",
                "text": "text 2",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": False,
                "reason": "API_ERROR",
            },
            {
                "uri": "test_uri_3",
                "text": "text 3",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            },
        ]
        uri_to_batch_id = {
            "test_uri_1": 100,
            "test_uri_2": 200,
            "test_uri_3": 300,
        }

        # Act
        result = attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        # Assert
        assert len(result) == 3
        assert all(isinstance(label, LabelWithBatchId) for label in result)
        assert result[0].batch_id == 100
        assert result[0].uri == "test_uri_1"
        assert result[1].batch_id == 200
        assert result[1].uri == "test_uri_2"
        assert result[2].batch_id == 300
        assert result[2].uri == "test_uri_3"

    def test_labels_with_extra_fields(self):
        """Test attaching batch_id to labels with service-specific extra fields.

        Expected behavior:
            - Should preserve all extra fields from label dicts
            - Should allow LabelWithBatchId to accept extra fields via ConfigDict(extra="allow")
            - Should correctly attach batch_id while preserving all other fields
        """
        # Arrange
        labels = [
            {
                "uri": "test_uri_1",
                "text": "test text",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
                "prob_toxic": 0.8,
                "prob_severe_toxic": 0.2,
                "label_timestamp": "2024-01-01-13:00:00",
            }
        ]
        uri_to_batch_id = {"test_uri_1": 456}

        # Act
        result = attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        # Assert
        assert len(result) == 1
        assert result[0].batch_id == 456
        assert result[0].uri == "test_uri_1"
        # Verify extra fields are preserved
        assert result[0].prob_toxic == 0.8
        assert result[0].prob_severe_toxic == 0.2
        assert result[0].label_timestamp == "2024-01-01-13:00:00"

    def test_multiple_labels_same_batch_id(self):
        """Test attaching batch_id when multiple labels share the same batch_id.

        Expected behavior:
            - Should correctly assign the same batch_id to multiple labels
            - Should handle multiple labels from the same batch
        """
        # Arrange
        labels = [
            {
                "uri": "test_uri_1",
                "text": "text 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            },
            {
                "uri": "test_uri_2",
                "text": "text 2",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            },
        ]
        uri_to_batch_id = {
            "test_uri_1": 999,
            "test_uri_2": 999,  # Same batch_id
        }

        # Act
        result = attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        # Assert
        assert len(result) == 2
        assert result[0].batch_id == 999
        assert result[1].batch_id == 999
        assert result[0].uri == "test_uri_1"
        assert result[1].uri == "test_uri_2"

    def test_empty_labels_list(self):
        """Test handling of empty labels list.

        Expected behavior:
            - Should return an empty list without raising errors
            - Should handle edge case gracefully
        """
        # Arrange
        labels = []
        uri_to_batch_id = {}

        # Act
        result = attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        # Assert
        assert result == []
        assert isinstance(result, list)

    def test_uri_not_found_raises_keyerror(self):
        """Test that missing URI in mapping raises KeyError.

        Expected behavior:
            - Should raise KeyError when label URI is not found in uri_to_batch_id mapping
            - Should provide clear error message indicating the missing URI
        """
        # Arrange
        labels = [
            {
                "uri": "missing_uri",
                "text": "test text",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            }
        ]
        uri_to_batch_id = {"other_uri": 123}

        # Act & Assert
        with pytest.raises(KeyError) as exc_info:
            attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        assert "missing_uri" in str(exc_info.value)
        assert "not found in uri_to_batch_id mapping" in str(exc_info.value)

    def test_multiple_labels_one_missing_uri(self):
        """Test that KeyError is raised when one label has missing URI.

        Expected behavior:
            - Should raise KeyError even if some labels have valid URIs
            - Should fail fast on first missing URI
        """
        # Arrange
        labels = [
            {
                "uri": "valid_uri",
                "text": "text 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            },
            {
                "uri": "missing_uri",
                "text": "text 2",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
            },
        ]
        uri_to_batch_id = {"valid_uri": 123}

        # Act & Assert
        with pytest.raises(KeyError) as exc_info:
            attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        assert "missing_uri" in str(exc_info.value)

    def test_missing_required_field_raises_validation_error(self):
        """Test that missing required fields raises ValidationError.

        Expected behavior:
            - Should raise ValidationError when label dict is missing required fields
            - Required fields: uri, text, preprocessing_timestamp, was_successfully_labeled
        """
        # Arrange
        labels = [
            {
                "uri": "test_uri",
                # Missing required fields: text, preprocessing_timestamp, was_successfully_labeled
            }
        ]
        uri_to_batch_id = {"test_uri": 123}

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        # Verify error contains information about missing fields
        errors = exc_info.value.errors()
        assert len(errors) > 0
        # Check that at least one required field error is present
        field_names = [error["loc"][0] for error in errors]
        assert any(
            field in field_names
            for field in ["text", "preprocessing_timestamp", "was_successfully_labeled"]
        )

    def test_missing_uri_field_raises_key_error(self):
        """Test that missing URI field in label dict raises KeyError.

        Expected behavior:
            - Should raise KeyError when label dict doesn't have 'uri' key
            - Should fail before attempting to look up in uri_to_batch_id mapping
        """
        # Arrange
        labels = [
            {
                "text": "test text",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "was_successfully_labeled": True,
                # Missing 'uri' field
            }
        ]
        uri_to_batch_id = {"test_uri": 123}

        # Act & Assert
        with pytest.raises(KeyError) as exc_info:
            attach_batch_id_to_label_dicts(labels, uri_to_batch_id)

        assert "'uri'" in str(exc_info.value)
