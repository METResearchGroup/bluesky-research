"""Tests for cache_buffer_writer_service.py."""

import pytest

from services.backfill.exceptions import CacheBufferWriterServiceError
from services.backfill.services.cache_buffer_writer_service import (
    CacheBufferWriterService,
)


class TestCacheBufferWriterService_write_cache:
    """Tests for CacheBufferWriterService.write_cache method."""

    @pytest.fixture
    def service(self, mock_repository, mock_queue_manager):
        """Create CacheBufferWriterService with mocked dependencies."""
        return CacheBufferWriterService(
            data_repository=mock_repository, queue_manager_service=mock_queue_manager
        )

    @pytest.mark.parametrize("records", [
        pytest.param([{"id": 1, "data": "record1"}, {"id": 2, "data": "record2"}], id="with_records"),
        pytest.param([], id="empty_records"),
    ])
    def test_writes_cache_successfully(self, service, mock_repository, mock_queue_manager, records):
        """Test that cache is written successfully with records."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue_manager.load_records_from_queue.return_value = records

        # Act
        service.write_cache(integration_name=integration_name)

        # Assert
        mock_queue_manager.load_records_from_queue.assert_called_once_with(
            integration_name=integration_name
        )
        mock_repository.write_records_to_storage.assert_called_once_with(
            integration_name=integration_name, records=records
        )

    def test_raises_error_when_queue_manager_load_fails(self, service, mock_queue_manager):
        """Test that CacheBufferWriterServiceError is raised when queue manager load fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        test_error = Exception("Queue load error")
        mock_queue_manager.load_records_from_queue.side_effect = test_error

        # Act & Assert
        with pytest.raises(CacheBufferWriterServiceError) as exc_info:
            service.write_cache(integration_name=integration_name)

        # Assert
        assert "Error writing cache for integration" in str(exc_info.value)
        assert integration_name in str(exc_info.value)
        assert "Queue load error" in str(exc_info.value)
        mock_queue_manager.load_records_from_queue.assert_called_once_with(
            integration_name=integration_name
        )

    def test_raises_error_when_repository_write_fails(self, service, mock_repository, mock_queue_manager, sample_records):
        """Test that CacheBufferWriterServiceError is raised when repository write fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue_manager.load_records_from_queue.return_value = sample_records
        test_error = Exception("Repository write error")
        mock_repository.write_records_to_storage.side_effect = test_error

        # Act & Assert
        with pytest.raises(CacheBufferWriterServiceError) as exc_info:
            service.write_cache(integration_name=integration_name)

        # Assert
        assert "Error writing cache for integration" in str(exc_info.value)
        assert integration_name in str(exc_info.value)
        assert "Repository write error" in str(exc_info.value)
        mock_queue_manager.load_records_from_queue.assert_called_once_with(
            integration_name=integration_name
        )
        mock_repository.write_records_to_storage.assert_called_once_with(
            integration_name=integration_name, records=sample_records
        )


class TestCacheBufferWriterService_clear_cache:
    """Tests for CacheBufferWriterService.clear_cache method."""

    @pytest.fixture
    def service(self, mock_repository, mock_queue_manager):
        """Create CacheBufferWriterService with mocked dependencies."""
        return CacheBufferWriterService(
            data_repository=mock_repository, queue_manager_service=mock_queue_manager
        )

    def test_clears_cache_successfully_with_ids(self, service, mock_queue_manager, sample_queue_ids):
        """Test that cache is cleared successfully when IDs are present."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue_manager.load_queue_item_ids.return_value = sample_queue_ids

        # Act
        service.clear_cache(integration_name=integration_name)

        # Assert
        mock_queue_manager.load_queue_item_ids.assert_called_once_with(
            integration_name=integration_name, queue_type="output"
        )
        mock_queue_manager.delete_records_from_queue.assert_called_once_with(
            integration_name=integration_name,
            queue_type="output",
            queue_ids_to_delete=sample_queue_ids,
        )

    def test_clears_cache_with_empty_ids_returns_early(self, service, mock_queue_manager):
        """Test that cache clearing returns early when no IDs are found."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue_manager.load_queue_item_ids.return_value = []

        # Act
        service.clear_cache(integration_name=integration_name)

        # Assert
        mock_queue_manager.load_queue_item_ids.assert_called_once_with(
            integration_name=integration_name, queue_type="output"
        )
        mock_queue_manager.delete_records_from_queue.assert_not_called()

    def test_raises_error_when_load_ids_fails(self, service, mock_queue_manager):
        """Test that CacheBufferWriterServiceError is raised when loading IDs fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        test_error = Exception("Load IDs error")
        mock_queue_manager.load_queue_item_ids.side_effect = test_error

        # Act & Assert
        with pytest.raises(CacheBufferWriterServiceError) as exc_info:
            service.clear_cache(integration_name=integration_name)

        # Assert
        assert "Error clearing cache for integration" in str(exc_info.value)
        assert integration_name in str(exc_info.value)
        assert "Load IDs error" in str(exc_info.value)
        mock_queue_manager.load_queue_item_ids.assert_called_once_with(
            integration_name=integration_name, queue_type="output"
        )
        mock_queue_manager.delete_records_from_queue.assert_not_called()

    def test_raises_error_when_delete_records_fails(self, service, mock_queue_manager, sample_queue_ids):
        """Test that CacheBufferWriterServiceError is raised when deleting records fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_queue_manager.load_queue_item_ids.return_value = sample_queue_ids
        test_error = Exception("Delete records error")
        mock_queue_manager.delete_records_from_queue.side_effect = test_error

        # Act & Assert
        with pytest.raises(CacheBufferWriterServiceError) as exc_info:
            service.clear_cache(integration_name=integration_name)

        # Assert
        assert "Error clearing cache for integration" in str(exc_info.value)
        assert integration_name in str(exc_info.value)
        assert "Delete records error" in str(exc_info.value)
        mock_queue_manager.load_queue_item_ids.assert_called_once_with(
            integration_name=integration_name, queue_type="output"
        )
        mock_queue_manager.delete_records_from_queue.assert_called_once_with(
            integration_name=integration_name,
            queue_type="output",
            queue_ids_to_delete=sample_queue_ids,
        )
