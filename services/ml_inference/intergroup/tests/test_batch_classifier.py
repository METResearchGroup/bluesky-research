"""Tests for batch_classifier.py.

This test suite verifies the functionality of intergroup batch classification:
- run_batch_classification: Main batch classification orchestration
- _manage_failed_labels: Handling of failed labels
- _manage_successful_labels: Handling of successful labels
"""

import pytest
from unittest.mock import Mock, patch

from services.ml_inference.intergroup.batch_classifier import (
    _manage_failed_labels,
    _manage_successful_labels,
    run_batch_classification,
)
from services.ml_inference.intergroup.models import IntergroupLabelModel
from services.ml_inference.models import (
    BatchClassificationMetadataModel,
    PostToLabelModel,
)

class TestRunBatchClassification:
    """Tests for run_batch_classification function."""

    @pytest.fixture
    def mock_create_batches(self):
        """Mock create_batches function."""
        with patch("services.ml_inference.intergroup.batch_classifier.create_batches") as mock:
            yield mock

    @pytest.fixture
    def mock_update_batching_progress(self):
        """Mock update_batching_progress function."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.update_batching_progress"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_classifier(self):
        """Mock IntergroupClassifier."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.IntergroupClassifier"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_split_labels(self):
        """Mock split_labels_into_successful_and_failed_labels function."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.split_labels_into_successful_and_failed_labels"
        ) as mock:
            yield mock

    def test_empty_posts_list(self):
        """Test handling of empty posts list.

        Expected behavior:
            - Should return BatchClassificationMetadataModel with zero counts
            - Should not call any classification functions
        """
        # Arrange
        posts = []

        # Act
        result = run_batch_classification(posts=posts)

        # Assert
        assert isinstance(result, BatchClassificationMetadataModel)
        assert result.total_batches == 0
        assert result.total_posts_successfully_labeled == 0
        assert result.total_posts_failed_to_label == 0

    def test_single_batch_all_successful_labels(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test single batch with all successful labels.

        Expected behavior:
            - Should process batch and return correct counts
            - Should call write_posts_to_cache for successful labels
        """
        # Arrange
        post_dicts = [
            {
                "uri": "uri_1",
                "text": "test post 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": "{}",
            },
            {
                "uri": "uri_2",
                "text": "test post 2",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 2,
                "batch_metadata": "{}",
            },
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post 1",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )
        successful_label_2 = IntergroupLabelModel(
            uri="uri_2",
            text="test post 2",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=0,
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [
            successful_label,
            successful_label_2,
        ]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([successful_label, successful_label_2], [])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ) as mock_write:
            # Act
            result = run_batch_classification(posts=posts)

            # Assert
            assert result.total_batches == 1
            assert result.total_posts_successfully_labeled == 2
            assert result.total_posts_failed_to_label == 0
            mock_write.assert_called_once()

    def test_single_batch_all_failed_labels(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test single batch with all failed labels.

        Expected behavior:
            - Should process batch and return correct counts
            - Should call return_failed_labels_to_input_queue for failed labels
        """
        # Arrange
        post_dicts = [
            {
                "uri": "uri_1",
                "text": "test post 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": "{}",
            }
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        failed_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post 1",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
            label=-1,
            reason="API_ERROR",
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [failed_label]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([], [failed_label])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.return_failed_labels_to_input_queue"
        ) as mock_return_failed:
            # Act
            result = run_batch_classification(posts=posts)

            # Assert
            assert result.total_batches == 1
            assert result.total_posts_successfully_labeled == 0
            assert result.total_posts_failed_to_label == 1
            mock_return_failed.assert_called_once()

    def test_single_batch_mixed_labels(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test single batch with mixed successful and failed labels.

        Expected behavior:
            - Should handle both successful and failed labels correctly
            - Should call both write_posts_to_cache and return_failed_labels_to_input_queue
        """
        # Arrange
        post_dicts = [
            {
                "uri": "uri_1",
                "text": "test post 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": "{}",
            },
            {
                "uri": "uri_2",
                "text": "test post 2",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 2,
                "batch_metadata": "{}",
            },
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post 1",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )
        failed_label = IntergroupLabelModel(
            uri="uri_2",
            text="test post 2",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
            label=-1,
            reason="API_ERROR",
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [
            successful_label,
            failed_label,
        ]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([successful_label], [failed_label])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ) as mock_write, patch(
            "services.ml_inference.intergroup.batch_classifier.return_failed_labels_to_input_queue"
        ) as mock_return_failed:
            # Act
            result = run_batch_classification(posts=posts)

            # Assert
            assert result.total_batches == 1
            assert result.total_posts_successfully_labeled == 1
            assert result.total_posts_failed_to_label == 1
            mock_write.assert_called_once()
            mock_return_failed.assert_called_once()

    def test_multiple_batches(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test processing multiple batches.

        Expected behavior:
            - Should process all batches correctly
            - Should aggregate counts across all batches
        """
        # Arrange
        post_dicts = [
            {
                "uri": f"uri_{i}",
                "text": f"test post {i}",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": i,
                "batch_metadata": "{}",
            }
            for i in range(4)
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        batch_1 = posts[:2]
        batch_2 = posts[2:]
        mock_create_batches.return_value = [batch_1, batch_2]

        successful_labels = [
            IntergroupLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=True,
                label=1,
            )
            for i in range(4)
        ]

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.side_effect = [
            successful_labels[:2],
            successful_labels[2:],
        ]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.side_effect = [
            (successful_labels[:2], []),
            (successful_labels[2:], []),
        ]

        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ) as mock_write:
            # Act
            result = run_batch_classification(posts=posts, batch_size=2)

            # Assert
            assert result.total_batches == 2
            assert result.total_posts_successfully_labeled == 4
            assert result.total_posts_failed_to_label == 0
            assert mock_write.call_count == 2

    def test_custom_batch_size(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test custom batch_size parameter.

        Expected behavior:
            - Should use custom batch_size when provided
            - Should pass batch_size to create_batches
        """
        # Arrange
        post_dicts = [
            {
                "uri": f"uri_{i}",
                "text": f"test post {i}",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": i,
                "batch_metadata": "{}",
            }
            for i in range(5)
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        successful_label = IntergroupLabelModel(
            uri="uri_0",
            text="test post 0",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [successful_label]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([successful_label], [])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ):
            # Act
            run_batch_classification(posts=posts, batch_size=10)

            # Assert
            mock_create_batches.assert_called_once_with(
                batch_list=posts, batch_size=10
            )

    def test_classifier_called_with_post_models(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test that classifier.classify_batch is called with correct PostToLabelModel instances.

        Expected behavior:
            - Should convert dict posts to PostToLabelModel before calling classifier
            - Should pass correct PostToLabelModel instances to classifier
        """
        # Arrange
        post_dicts = [
            {
                "uri": "uri_1",
                "text": "test post 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": "{}",
            }
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post 1",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [successful_label]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([successful_label], [])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ):
            # Act
            run_batch_classification(posts=posts)

            # Assert
            mock_classifier_instance.classify_batch.assert_called_once()
            call_args = mock_classifier_instance.classify_batch.call_args[0][0]
            assert len(call_args) == 1
            assert isinstance(call_args[0], PostToLabelModel)
            assert call_args[0].uri == "uri_1"
            assert call_args[0].text == "test post 1"
            assert call_args[0].batch_id == 1

    def test_uri_to_batch_id_mapping(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test that uri_to_batch_id mapping is correctly created.

        Expected behavior:
            - Should create correct mapping from URI to batch_id
            - Should use mapping when attaching batch_ids to labels
        """
        # Arrange
        post_dicts = [
            {
                "uri": "uri_1",
                "text": "test post 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 100,
                "batch_metadata": "{}",
            },
            {
                "uri": "uri_2",
                "text": "test post 2",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 200,
                "batch_metadata": "{}",
            },
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post 1",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )
        successful_label_2 = IntergroupLabelModel(
            uri="uri_2",
            text="test post 2",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=0,
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [
            successful_label,
            successful_label_2,
        ]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([successful_label, successful_label_2], [])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.attach_batch_id_to_label_dicts"
        ) as mock_attach:
            mock_attach.return_value = []
            with patch(
                "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
            ):
                # Act
                run_batch_classification(posts=posts)

                # Assert
                mock_attach.assert_called_once()
                call_kwargs = mock_attach.call_args[1]
                uri_to_batch_id = call_kwargs["uri_to_batch_id"]
                assert uri_to_batch_id == {"uri_1": 100, "uri_2": 200}

    def test_return_value_structure(
        self, mock_create_batches, mock_update_batching_progress, mock_classifier, mock_split_labels
    ):
        """Test return value structure (BatchClassificationMetadataModel).

        Expected behavior:
            - Should return BatchClassificationMetadataModel instance
            - Should have correct field types and values
        """
        # Arrange
        post_dicts = [
            {
                "uri": "uri_1",
                "text": "test post 1",
                "preprocessing_timestamp": "2024-01-01-12:00:00",
                "batch_id": 1,
                "batch_metadata": "{}",
            }
        ]
        posts = [PostToLabelModel(**d) for d in post_dicts]
        mock_create_batches.return_value = [posts]

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post 1",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )

        mock_classifier_instance = Mock()
        mock_classifier_instance.classify_batch.return_value = [successful_label]
        mock_classifier.return_value = mock_classifier_instance

        mock_split_labels.return_value = ([successful_label], [])

        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ):
            # Act
            result = run_batch_classification(posts=posts)

            # Assert
            assert isinstance(result, BatchClassificationMetadataModel)
            assert isinstance(result.total_batches, int)
            assert isinstance(result.total_posts_successfully_labeled, int)
            assert isinstance(result.total_posts_failed_to_label, int)
            assert result.total_batches >= 0
            assert result.total_posts_successfully_labeled >= 0
            assert result.total_posts_failed_to_label >= 0


class TestManageFailedLabels:
    """Tests for _manage_failed_labels function."""

    @pytest.fixture
    def mock_attach_batch_id(self):
        """Mock attach_batch_id_to_label_dicts function."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.attach_batch_id_to_label_dicts"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_return_failed(self):
        """Mock return_failed_labels_to_input_queue function."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.return_failed_labels_to_input_queue"
        ) as mock:
            yield mock

    def test_empty_failed_labels_list(self, mock_attach_batch_id, mock_return_failed):
        """Test with empty failed_labels list.

        Expected behavior:
            - Should return total_failed_so_far unchanged
            - Should still call return_failed_labels_to_input_queue with empty list
        """
        # Arrange
        failed_labels = []
        uri_to_batch_id = {}
        total_failed_so_far = 5
        mock_attach_batch_id.return_value = []

        # Act
        result = _manage_failed_labels(
            failed_labels=failed_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_failed_so_far=total_failed_so_far,
        )

        # Assert
        assert result == 5
        mock_return_failed.assert_called_once_with(
            inference_type="intergroup",
            failed_label_models=[],
        )

    @pytest.mark.parametrize(
        "label_count,uri_to_batch_id_start,total_failed_so_far,expected_total",
        [
            (1, 123, 0, 1),
            (3, 100, 2, 5),
        ],
    )
    def test_failed_labels(
        self, mock_attach_batch_id, mock_return_failed, label_count, uri_to_batch_id_start, total_failed_so_far, expected_total
    ):
        """Test with failed labels.

        Expected behavior:
            - Should attach batch_id to labels
            - Should call return_failed_labels_to_input_queue
            - Should return updated total count
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        failed_labels = [
            IntergroupLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=False,
                label=-1,
                reason="API_ERROR",
            )
            for i in range(label_count)
        ]
        uri_to_batch_id = {f"uri_{i}": uri_to_batch_id_start + i for i in range(label_count)}

        labels_with_batch_id = [
            LabelWithBatchId(
                batch_id=uri_to_batch_id_start + i,
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=False,
            )
            for i in range(label_count)
        ]
        # Add extra fields that are allowed via model_config
        for label in labels_with_batch_id:
            label.label = -1  # type: ignore
            label.reason = "API_ERROR"  # type: ignore
        mock_attach_batch_id.return_value = labels_with_batch_id

        # Act
        result = _manage_failed_labels(
            failed_labels=failed_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_failed_so_far=total_failed_so_far,
        )

        # Assert
        assert result == expected_total
        mock_attach_batch_id.assert_called_once()
        mock_return_failed.assert_called_once_with(
            inference_type="intergroup",
            failed_label_models=labels_with_batch_id,
        )

    def test_attach_batch_id_called_correctly(self, mock_attach_batch_id, mock_return_failed):
        """Test that attach_batch_id_to_label_dicts is called correctly.

        Expected behavior:
            - Should convert labels to dicts using model_dump()
            - Should pass correct uri_to_batch_id mapping
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        failed_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
            reason="API_ERROR",
            label=-1,
        )
        failed_labels = [failed_label]
        uri_to_batch_id = {"uri_1": 999}

        label_with_batch_id = LabelWithBatchId(
            batch_id=999,
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
        )
        # Add extra fields that are allowed via model_config
        label_with_batch_id.reason = "API_ERROR"  # type: ignore
        label_with_batch_id.label = -1  # type: ignore
        mock_attach_batch_id.return_value = [label_with_batch_id]

        # Act
        _manage_failed_labels(
            failed_labels=failed_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_failed_so_far=0,
        )

        # Assert
        mock_attach_batch_id.assert_called_once()
        call_args = mock_attach_batch_id.call_args
        # attach_batch_id_to_label_dicts is called with keyword arguments
        assert len(call_args.kwargs["labels"]) == 1
        assert call_args.kwargs["uri_to_batch_id"] == uri_to_batch_id

    def test_return_failed_called_with_correct_parameters(
        self, mock_attach_batch_id, mock_return_failed
    ):
        """Test that return_failed_labels_to_input_queue is called with correct parameters.

        Expected behavior:
            - Should call with inference_type="intergroup"
            - Should pass LabelWithBatchId instances
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        failed_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
            reason="API_ERROR",
            label=-1,
        )
        failed_labels = [failed_label]
        uri_to_batch_id = {"uri_1": 123}

        label_with_batch_id = LabelWithBatchId(
            batch_id=123,
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=False,
        )
        # Add extra fields that are allowed via model_config
        label_with_batch_id.reason = "API_ERROR"  # type: ignore
        label_with_batch_id.label = -1  # type: ignore
        mock_attach_batch_id.return_value = [label_with_batch_id]

        # Act
        _manage_failed_labels(
            failed_labels=failed_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_failed_so_far=0,
        )

        # Assert
        mock_return_failed.assert_called_once_with(
            inference_type="intergroup",
            failed_label_models=[label_with_batch_id],
        )

    def test_correct_total_count_returned(self, mock_attach_batch_id, mock_return_failed):
        """Test that correct total count is returned.

        Expected behavior:
            - Should return total_failed_so_far + number of new failed labels
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        failed_labels = [
            IntergroupLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=False,
                reason="API_ERROR",
                label=-1,
            )
            for i in range(2)
        ]
        uri_to_batch_id = {f"uri_{i}": i for i in range(2)}
        total_failed_so_far = 10

        labels_with_batch_id = [
            LabelWithBatchId(
                batch_id=i,
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=False,
            )
            for i in range(2)
        ]
        # Add extra fields that are allowed via model_config
        for label in labels_with_batch_id:
            label.reason = "API_ERROR"  # type: ignore
            label.label = -1  # type: ignore
        mock_attach_batch_id.return_value = labels_with_batch_id

        # Act
        result = _manage_failed_labels(
            failed_labels=failed_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_failed_so_far=total_failed_so_far,
        )

        # Assert
        assert result == 12


class TestManageSuccessfulLabels:
    """Tests for _manage_successful_labels function."""

    @pytest.fixture
    def mock_attach_batch_id(self):
        """Mock attach_batch_id_to_label_dicts function."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.attach_batch_id_to_label_dicts"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_write_posts(self):
        """Mock write_posts_to_cache function."""
        with patch(
            "services.ml_inference.intergroup.batch_classifier.write_posts_to_cache"
        ) as mock:
            yield mock

    def test_empty_successful_labels_list(self, mock_attach_batch_id, mock_write_posts):
        """Test with empty successful_labels list.

        Expected behavior:
            - Should return total_successful_so_far unchanged
            - Should still call write_posts_to_cache with empty list
        """
        # Arrange
        successful_labels = []
        uri_to_batch_id = {}
        total_successful_so_far = 3
        mock_attach_batch_id.return_value = []

        # Act
        result = _manage_successful_labels(
            successful_labels=successful_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_successful_so_far=total_successful_so_far,
        )

        # Assert
        assert result == 3
        mock_write_posts.assert_called_once_with(
            inference_type="intergroup",
            posts=[],
        )

    def test_single_successful_label(self, mock_attach_batch_id, mock_write_posts):
        """Test with single successful label.

        Expected behavior:
            - Should attach batch_id to label
            - Should call write_posts_to_cache
            - Should return updated total count
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )
        successful_labels = [successful_label]
        uri_to_batch_id = {"uri_1": 456}
        total_successful_so_far = 0

        label_with_batch_id = LabelWithBatchId(
            batch_id=456,
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
        )
        # Add extra fields that are allowed via model_config
        label_with_batch_id.label = 1  # type: ignore
        mock_attach_batch_id.return_value = [label_with_batch_id]

        # Act
        result = _manage_successful_labels(
            successful_labels=successful_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_successful_so_far=total_successful_so_far,
        )

        # Assert
        assert result == 1
        mock_attach_batch_id.assert_called_once()
        mock_write_posts.assert_called_once_with(
            inference_type="intergroup",
            posts=[label_with_batch_id],
        )

    def test_multiple_successful_labels(self, mock_attach_batch_id, mock_write_posts):
        """Test with multiple successful labels.

        Expected behavior:
            - Should attach batch_ids to all labels
            - Should call write_posts_to_cache with all labels
            - Should return updated total count
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        successful_labels = [
            IntergroupLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=True,
                label=i % 2,
            )
            for i in range(3)
        ]
        uri_to_batch_id = {f"uri_{i}": 200 + i for i in range(3)}
        total_successful_so_far = 5

        labels_with_batch_id = [
            LabelWithBatchId(
                batch_id=200 + i,
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=True,
            )
            for i in range(3)
        ]
        # Add extra fields that are allowed via model_config
        for i, label in enumerate(labels_with_batch_id):
            label.label = i % 2  # type: ignore
        mock_attach_batch_id.return_value = labels_with_batch_id

        # Act
        result = _manage_successful_labels(
            successful_labels=successful_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_successful_so_far=total_successful_so_far,
        )

        # Assert
        assert result == 8
        mock_write_posts.assert_called_once_with(
            inference_type="intergroup",
            posts=labels_with_batch_id,
        )

    def test_attach_batch_id_called_correctly(self, mock_attach_batch_id, mock_write_posts):
        """Test that attach_batch_id_to_label_dicts is called correctly.

        Expected behavior:
            - Should convert labels to dicts using model_dump()
            - Should pass correct uri_to_batch_id mapping
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )
        successful_labels = [successful_label]
        uri_to_batch_id = {"uri_1": 777}

        label_with_batch_id = LabelWithBatchId(
            batch_id=777,
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
        )
        # Add extra fields that are allowed via model_config
        label_with_batch_id.label = 1  # type: ignore
        mock_attach_batch_id.return_value = [label_with_batch_id]

        # Act
        _manage_successful_labels(
            successful_labels=successful_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_successful_so_far=0,
        )

        # Assert
        mock_attach_batch_id.assert_called_once()
        call_args = mock_attach_batch_id.call_args
        # attach_batch_id_to_label_dicts is called with keyword arguments
        assert len(call_args.kwargs["labels"]) == 1
        assert call_args.kwargs["uri_to_batch_id"] == uri_to_batch_id

    def test_write_posts_called_with_correct_parameters(
        self, mock_attach_batch_id, mock_write_posts
    ):
        """Test that write_posts_to_cache is called with correct parameters.

        Expected behavior:
            - Should call with inference_type="intergroup"
            - Should pass LabelWithBatchId instances
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        successful_label = IntergroupLabelModel(
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
            label=1,
        )
        successful_labels = [successful_label]
        uri_to_batch_id = {"uri_1": 888}

        label_with_batch_id = LabelWithBatchId(
            batch_id=888,
            uri="uri_1",
            text="test post",
            preprocessing_timestamp="2024-01-01-12:00:00",
            was_successfully_labeled=True,
        )
        # Add extra fields that are allowed via model_config
        label_with_batch_id.label = 1  # type: ignore
        mock_attach_batch_id.return_value = [label_with_batch_id]

        # Act
        _manage_successful_labels(
            successful_labels=successful_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_successful_so_far=0,
        )

        # Assert
        mock_write_posts.assert_called_once_with(
            inference_type="intergroup",
            posts=[label_with_batch_id],
        )

    def test_correct_total_count_returned(self, mock_attach_batch_id, mock_write_posts):
        """Test that correct total count is returned.

        Expected behavior:
            - Should return total_successful_so_far + number of new successful labels
        """
        # Arrange
        from services.ml_inference.models import LabelWithBatchId

        successful_labels = [
            IntergroupLabelModel(
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=True,
                label=i % 2,
            )
            for i in range(4)
        ]
        uri_to_batch_id = {f"uri_{i}": i for i in range(4)}
        total_successful_so_far = 15

        labels_with_batch_id = [
            LabelWithBatchId(
                batch_id=i,
                uri=f"uri_{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                was_successfully_labeled=True,
            )
            for i in range(4)
        ]
        # Add extra fields that are allowed via model_config
        for i, label in enumerate(labels_with_batch_id):
            label.label = i % 2  # type: ignore
        mock_attach_batch_id.return_value = labels_with_batch_id

        # Act
        result = _manage_successful_labels(
            successful_labels=successful_labels,
            uri_to_batch_id=uri_to_batch_id,
            total_successful_so_far=total_successful_so_far,
        )

        # Assert
        assert result == 19
