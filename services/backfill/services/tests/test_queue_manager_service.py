"""Tests for queue_manager_service.py."""

import pytest
from unittest.mock import Mock, patch, MagicMock

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
        """Test that posts are inserted to queue correctly."""
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
            assert call_args.kwargs["metadata"] is None
            items = call_args.kwargs["items"]
            assert len(items) == 2
            assert isinstance(items[0], dict)
            assert isinstance(items[1], dict)
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

    def test_converts_posts_to_dicts_via_model_dump(self, service, sample_posts):
        """Test that posts are converted to dicts using model_dump()."""
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
            call_args = mock_queue.batch_add_items_to_queue.call_args
            items = call_args.kwargs["items"]
            # Verify all items are dicts (not PostToEnqueueModel instances)
            assert all(isinstance(item, dict) for item in items)
            # Verify dict structure matches model fields
            assert all(
                "uri" in item and "text" in item and "preprocessing_timestamp" in item
                for item in items
            )

    def test_passes_none_for_metadata(self, service, sample_posts):
        """Test that metadata parameter is passed as None."""
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
            call_args = mock_queue.batch_add_items_to_queue.call_args
            assert call_args.kwargs["metadata"] is None
