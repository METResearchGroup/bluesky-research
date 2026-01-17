"""Tests for queue_manager_service.py."""

import pytest
from unittest.mock import Mock, patch

from services.backfill.exceptions import QueueManagerServiceError
from services.backfill.models import PostToEnqueueModel
from services.backfill.services.queue_manager_service import QueueManagerService


class TestQueueManagerService_insert_posts_to_queue:
    """Tests for QueueManagerService.insert_posts_to_queue method."""

    @pytest.fixture
    def service(self):
        """Create QueueManagerService instance."""
        return QueueManagerService()

    @pytest.fixture
    def sample_posts(self):
        """Sample posts for testing."""
        return [
            PostToEnqueueModel(
                uri="test_uri_1",
                text="test_text_1",
                preprocessing_timestamp="2024-01-01T00:00:00",
            ),
            PostToEnqueueModel(
                uri="test_uri_2",
                text="test_text_2",
                preprocessing_timestamp="2024-01-02T00:00:00",
            ),
        ]

    def test_inserts_posts_to_queue(self, service, sample_posts):
        """Test that posts are inserted to queue correctly with all validations."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue = Mock()
        mock_queue.batch_add_items_to_queue = Mock()

        with patch(
            "services.backfill.services.queue_manager_service.get_input_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            service.insert_posts_to_queue(
                integration_name=integration_name, posts=sample_posts
            )

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.batch_add_items_to_queue.assert_called_once()
            call_args = mock_queue.batch_add_items_to_queue.call_args
            # Verify metadata is None
            assert call_args.kwargs["metadata"] is None
            # Verify items are dicts with correct structure
            items = call_args.kwargs["items"]
            assert len(items) == 2
            assert all(isinstance(item, dict) for item in items)
            assert all(
                "uri" in item and "text" in item and "preprocessing_timestamp" in item
                for item in items
            )
            assert items[0]["uri"] == "test_uri_1"
            assert items[0]["text"] == "test_text_1"
            assert items[0]["preprocessing_timestamp"] == "2024-01-01T00:00:00"
            assert items[1]["uri"] == "test_uri_2"
            assert items[1]["text"] == "test_text_2"
            assert items[1]["preprocessing_timestamp"] == "2024-01-02T00:00:00"

    def test_inserts_empty_list_to_queue(self, service):
        """Test that empty list is handled correctly."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        posts = []
        mock_queue = Mock()
        mock_queue.batch_add_items_to_queue = Mock()

        with patch(
            "services.backfill.services.queue_manager_service.get_input_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            service.insert_posts_to_queue(
                integration_name=integration_name, posts=posts
            )

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.batch_add_items_to_queue.assert_called_once()
            call_args = mock_queue.batch_add_items_to_queue.call_args
            items = call_args.kwargs["items"]
            assert items == []
            assert len(items) == 0
            assert call_args.kwargs["metadata"] is None


class TestQueueManagerService_load_records_from_queue:
    """Tests for QueueManagerService.load_records_from_queue method."""

    @pytest.fixture
    def service(self):
        """Create QueueManagerService instance."""
        return QueueManagerService()

    def test_loads_records_from_output_queue_successfully(self, service, sample_records):
        """Test that records are loaded from output queue successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue = Mock()
        mock_queue.load_dict_items_from_queue.return_value = sample_records

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            result = service.load_records_from_queue(integration_name=integration_name)

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_dict_items_from_queue.assert_called_once()
            assert result == sample_records

    def test_loads_empty_records_successfully(self, service):
        """Test that empty records list is returned when queue is empty."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue = Mock()
        mock_queue.load_dict_items_from_queue.return_value = []

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            result = service.load_records_from_queue(integration_name=integration_name)

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_dict_items_from_queue.assert_called_once()
            assert result == []
            assert len(result) == 0

    def test_raises_error_when_get_queue_fails(self, service):
        """Test that QueueManagerServiceError is raised when get_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        test_error = Exception("Get queue error")

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.side_effect = test_error

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.load_records_from_queue(integration_name=integration_name)

            # Assert
            assert "Error loading records from queue" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Get queue error" in str(exc_info.value)

    def test_raises_error_when_load_dict_items_fails(self, service):
        """Test that QueueManagerServiceError is raised when load_dict_items_from_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue = Mock()
        test_error = Exception("Load dict items error")
        mock_queue.load_dict_items_from_queue.side_effect = test_error

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.load_records_from_queue(integration_name=integration_name)

            # Assert
            assert "Error loading records from queue" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Load dict items error" in str(exc_info.value)
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_dict_items_from_queue.assert_called_once()

    def test_passes_correct_integration_name_to_get_queue(self, service, sample_records):
        """Test that correct integration name is passed to get_output_queue_for_integration."""
        # Arrange
        integration_name = "test_integration_service"
        mock_queue = Mock()
        mock_queue.load_dict_items_from_queue.return_value = sample_records

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            service.load_records_from_queue(integration_name=integration_name)

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)


class TestQueueManagerService_load_queue_item_ids:
    """Tests for QueueManagerService.load_queue_item_ids method."""

    @pytest.fixture
    def service(self):
        """Create QueueManagerService instance."""
        return QueueManagerService()

    def test_loads_ids_from_input_queue_successfully(self, service, sample_queue_ids):
        """Test that IDs are loaded from input queue successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "input"
        mock_queue = Mock()
        mock_queue.load_item_ids_from_queue.return_value = sample_queue_ids

        with patch(
            "services.backfill.services.queue_manager_service.get_input_queue_for_integration"
        ) as mock_get_input_queue:
            mock_get_input_queue.return_value = mock_queue

            # Act
            result = service.load_queue_item_ids(
                integration_name=integration_name, queue_type=queue_type
            )

            # Assert
            mock_get_input_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_item_ids_from_queue.assert_called_once()
            assert result == sample_queue_ids

    def test_loads_ids_from_output_queue_successfully(self, service, sample_queue_ids):
        """Test that IDs are loaded from output queue successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        mock_queue.load_item_ids_from_queue.return_value = sample_queue_ids

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_output_queue:
            mock_get_output_queue.return_value = mock_queue

            # Act
            result = service.load_queue_item_ids(
                integration_name=integration_name, queue_type=queue_type
            )

            # Assert
            mock_get_output_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_item_ids_from_queue.assert_called_once()
            assert result == sample_queue_ids

    def test_loads_empty_ids_successfully(self, service):
        """Test that empty IDs list is returned when queue is empty."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        mock_queue.load_item_ids_from_queue.return_value = []

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            result = service.load_queue_item_ids(
                integration_name=integration_name, queue_type=queue_type
            )

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_item_ids_from_queue.assert_called_once()
            assert result == []
            assert len(result) == 0

    def test_raises_error_for_invalid_queue_type(self, service):
        """Test that QueueManagerServiceError is raised for invalid queue type."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "invalid_type"

        # Act & Assert
        with pytest.raises(QueueManagerServiceError) as exc_info:
            service.load_queue_item_ids(
                integration_name=integration_name, queue_type=queue_type
            )

        # Assert
        assert "Invalid queue type" in str(exc_info.value)
        assert queue_type in str(exc_info.value)

    def test_raises_error_when_get_input_queue_fails(self, service):
        """Test that QueueManagerServiceError is raised when get_input_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "input"
        test_error = Exception("Get input queue error")

        with patch(
            "services.backfill.services.queue_manager_service.get_input_queue_for_integration"
        ) as mock_get_input_queue:
            mock_get_input_queue.side_effect = test_error

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.load_queue_item_ids(
                    integration_name=integration_name, queue_type=queue_type
                )

            # Assert
            assert "Error loading queue item IDs" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Get input queue error" in str(exc_info.value)

    def test_raises_error_when_get_output_queue_fails(self, service):
        """Test that QueueManagerServiceError is raised when get_output_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        test_error = Exception("Get output queue error")

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_output_queue:
            mock_get_output_queue.side_effect = test_error

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.load_queue_item_ids(
                    integration_name=integration_name, queue_type=queue_type
                )

            # Assert
            assert "Error loading queue item IDs" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Get output queue error" in str(exc_info.value)

    def test_raises_error_when_load_item_ids_fails(self, service):
        """Test that QueueManagerServiceError is raised when load_item_ids_from_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        test_error = Exception("Load item IDs error")
        mock_queue.load_item_ids_from_queue.side_effect = test_error

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.load_queue_item_ids(
                    integration_name=integration_name, queue_type=queue_type
                )

            # Assert
            assert "Error loading queue item IDs" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Load item IDs error" in str(exc_info.value)
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.load_item_ids_from_queue.assert_called_once()


class TestQueueManagerService_delete_records_from_queue:
    """Tests for QueueManagerService.delete_records_from_queue method."""

    @pytest.fixture
    def service(self):
        """Create QueueManagerService instance."""
        return QueueManagerService()

    def test_deletes_records_with_ids_from_input_queue(self, service, sample_queue_ids):
        """Test that records are deleted with IDs from input queue successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "input"
        mock_queue = Mock()
        mock_queue.batch_delete_items_by_ids.return_value = len(sample_queue_ids)

        with patch(
            "services.backfill.services.queue_manager_service.get_input_queue_for_integration"
        ) as mock_get_input_queue:
            mock_get_input_queue.return_value = mock_queue

            # Act
            service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type=queue_type,
                queue_ids_to_delete=sample_queue_ids,
            )

            # Assert
            mock_get_input_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.batch_delete_items_by_ids.assert_called_once_with(
                ids=sample_queue_ids
            )

    def test_deletes_records_with_ids_from_output_queue(self, service, sample_queue_ids):
        """Test that records are deleted with IDs from output queue successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        mock_queue.batch_delete_items_by_ids.return_value = len(sample_queue_ids)

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_output_queue:
            mock_get_output_queue.return_value = mock_queue

            # Act
            service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type=queue_type,
                queue_ids_to_delete=sample_queue_ids,
            )

            # Assert
            mock_get_output_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.batch_delete_items_by_ids.assert_called_once_with(
                ids=sample_queue_ids
            )

    def test_deletes_all_records_when_ids_is_none_input_queue(self, service):
        """Test that all records are deleted when ids is None for input queue."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "input"
        mock_queue = Mock()
        mock_queue.clear_queue.return_value = 10

        with patch(
            "services.backfill.services.queue_manager_service.get_input_queue_for_integration"
        ) as mock_get_input_queue:
            mock_get_input_queue.return_value = mock_queue

            # Act
            service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type=queue_type,
                queue_ids_to_delete=None,
            )

            # Assert
            mock_get_input_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.clear_queue.assert_called_once()
            mock_queue.batch_delete_items_by_ids.assert_not_called()

    def test_deletes_all_records_when_ids_is_none_output_queue(self, service):
        """Test that all records are deleted when ids is None for output queue."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        mock_queue.clear_queue.return_value = 10

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_output_queue:
            mock_get_output_queue.return_value = mock_queue

            # Act
            service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type=queue_type,
                queue_ids_to_delete=None,
            )

            # Assert
            mock_get_output_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.clear_queue.assert_called_once()
            mock_queue.batch_delete_items_by_ids.assert_not_called()

    def test_deletes_empty_ids_list_successfully(self, service):
        """Test that empty IDs list is handled successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        mock_queue.batch_delete_items_by_ids.return_value = 0

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act
            service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type=queue_type,
                queue_ids_to_delete=[],
            )

            # Assert
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.batch_delete_items_by_ids.assert_called_once_with(ids=[])

    def test_raises_error_for_invalid_queue_type(self, service, sample_queue_ids):
        """Test that QueueManagerServiceError is raised for invalid queue type."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "invalid_type"

        # Act & Assert
        with pytest.raises(QueueManagerServiceError) as exc_info:
            service.delete_records_from_queue(
                integration_name=integration_name,
                queue_type=queue_type,
                queue_ids_to_delete=sample_queue_ids,
            )

        # Assert
        assert "Invalid queue type" in str(exc_info.value)
        assert queue_type in str(exc_info.value)

    def test_raises_error_when_get_queue_fails(self, service, sample_queue_ids):
        """Test that QueueManagerServiceError is raised when get_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        test_error = Exception("Get queue error")

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.side_effect = test_error

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.delete_records_from_queue(
                    integration_name=integration_name,
                    queue_type=queue_type,
                    queue_ids_to_delete=sample_queue_ids,
                )

            # Assert
            assert "Error deleting records from queue" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Get queue error" in str(exc_info.value)

    def test_raises_error_when_batch_delete_fails(self, service, sample_queue_ids):
        """Test that QueueManagerServiceError is raised when batch_delete_items_by_ids fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        test_error = Exception("Batch delete error")
        mock_queue.batch_delete_items_by_ids.side_effect = test_error

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.delete_records_from_queue(
                    integration_name=integration_name,
                    queue_type=queue_type,
                    queue_ids_to_delete=sample_queue_ids,
                )

            # Assert
            assert "Error deleting records from queue" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Batch delete error" in str(exc_info.value)
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.batch_delete_items_by_ids.assert_called_once_with(
                ids=sample_queue_ids
            )

    def test_raises_error_when_clear_queue_fails(self, service):
        """Test that QueueManagerServiceError is raised when clear_queue fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        queue_type = "output"
        mock_queue = Mock()
        test_error = Exception("Clear queue error")
        mock_queue.clear_queue.side_effect = test_error

        with patch(
            "services.backfill.services.queue_manager_service.get_output_queue_for_integration"
        ) as mock_get_queue:
            mock_get_queue.return_value = mock_queue

            # Act & Assert
            with pytest.raises(QueueManagerServiceError) as exc_info:
                service.delete_records_from_queue(
                    integration_name=integration_name,
                    queue_type=queue_type,
                    queue_ids_to_delete=None,
                )

            # Assert
            assert "Error deleting records from queue" in str(exc_info.value)
            assert integration_name in str(exc_info.value)
            assert "Clear queue error" in str(exc_info.value)
            mock_get_queue.assert_called_once_with(integration_name=integration_name)
            mock_queue.clear_queue.assert_called_once()

