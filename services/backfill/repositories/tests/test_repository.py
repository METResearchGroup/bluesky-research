"""Tests for repository.py."""

import pytest

from services.backfill.repositories.repository import BackfillDataRepository


class TestBackfillDataRepository__init__:
    """Tests for BackfillDataRepository.__init__ method."""

    def test_initializes_with_valid_adapter(self, mock_adapter):
        """Test that repository initializes successfully with a valid adapter."""
        # Arrange (mock_adapter comes from conftest)

        # Act
        repository = BackfillDataRepository(adapter=mock_adapter)

        # Assert
        assert repository.adapter == mock_adapter

    @pytest.mark.parametrize("invalid_adapter", ["not_an_adapter", None])
    def test_raises_value_error_for_invalid_adapter(self, invalid_adapter):
        """Test that invalid adapter type raises ValueError."""
        # Act & Assert
        with pytest.raises(ValueError, match="Adapter must be an instance of BackfillDataAdapter"):
            BackfillDataRepository(adapter=invalid_adapter)


class TestBackfillDataRepository_load_all_posts:
    """Tests for BackfillDataRepository.load_all_posts method."""

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter, sample_posts, sample_date_range):
        """Test that load_all_posts delegates to adapter with correct parameters."""
        # Arrange
        mock_adapter.load_all_posts.return_value = sample_posts

        # Act
        result = repository.load_all_posts(
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"]
        )

        # Assert
        mock_adapter.load_all_posts.assert_called_once_with(
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"]
        )
        assert result == sample_posts

    def test_returns_empty_list_when_adapter_returns_empty(self, repository, mock_adapter, sample_date_range):
        """Test that empty list is returned when adapter returns empty list."""
        # Arrange
        mock_adapter.load_all_posts.return_value = []

        # Act
        result = repository.load_all_posts(
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"]
        )

        # Assert
        assert result == []
        assert len(result) == 0

class TestBackfillDataRepository_load_feed_posts:
    """Tests for BackfillDataRepository.load_feed_posts method."""

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter, sample_posts, sample_date_range):
        """Test that load_feed_posts delegates to adapter with correct parameters."""
        # Arrange
        expected_posts = [sample_posts[0]]
        mock_adapter.load_feed_posts.return_value = expected_posts

        # Act
        result = repository.load_feed_posts(
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"]
        )

        # Assert
        mock_adapter.load_feed_posts.assert_called_once_with(
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"]
        )
        assert result == expected_posts

    def test_returns_empty_list_when_adapter_returns_empty(self, repository, mock_adapter, sample_date_range):
        """Test that empty list is returned when adapter returns empty list."""
        # Arrange
        mock_adapter.load_feed_posts.return_value = []

        # Act
        result = repository.load_feed_posts(
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"]
        )

        # Assert
        assert result == []
        assert len(result) == 0

class TestBackfillDataRepository_get_previously_labeled_post_uris:
    """Tests for BackfillDataRepository.get_previously_labeled_post_uris method."""

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter, sample_date_range):
        """Test that get_previously_labeled_post_uris delegates to adapter with correct parameters."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        expected_uris = {"uri1", "uri2", "uri3"}
        mock_adapter.get_previously_labeled_post_uris.return_value = expected_uris

        # Act
        result = repository.get_previously_labeled_post_uris(
            service=service,
            id_field=id_field,
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"],
        )

        # Assert
        mock_adapter.get_previously_labeled_post_uris.assert_called_once_with(
            service=service,
            id_field=id_field,
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"],
        )
        assert result == expected_uris

    def test_returns_empty_set_when_adapter_returns_empty(self, repository, mock_adapter, sample_date_range):
        """Test that empty set is returned when adapter returns empty set."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        mock_adapter.get_previously_labeled_post_uris.return_value = set()

        # Act
        result = repository.get_previously_labeled_post_uris(
            service=service,
            id_field=id_field,
            start_date=sample_date_range["start_date"],
            end_date=sample_date_range["end_date"],
        )

        # Assert
        assert result == set()
        assert len(result) == 0


class TestBackfillDataRepository_write_records_to_storage:
    """Tests for BackfillDataRepository.write_records_to_storage method."""

    def test_writes_records_to_storage_successfully(self, repository, mock_adapter, sample_records):
        """Test that records are written to storage successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        mock_adapter.write_records_to_storage.return_value = None

        # Act
        repository.write_records_to_storage(
            integration_name=integration_name, records=sample_records
        )

        # Assert
        mock_adapter.write_records_to_storage.assert_called_once_with(
            integration_name=integration_name, records=sample_records
        )

    def test_writes_empty_records_successfully(self, repository, mock_adapter):
        """Test that empty records list is written successfully."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        records = []
        mock_adapter.write_records_to_storage.return_value = None

        # Act
        repository.write_records_to_storage(
            integration_name=integration_name, records=records
        )

        # Assert
        mock_adapter.write_records_to_storage.assert_called_once_with(
            integration_name=integration_name, records=records
        )
        assert records == []

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter, sample_records):
        """Test that write_records_to_storage delegates to adapter with correct parameters."""
        # Arrange
        integration_name = "test_integration_service"
        mock_adapter.write_records_to_storage.return_value = None

        # Act
        repository.write_records_to_storage(
            integration_name=integration_name, records=sample_records
        )

        # Assert
        mock_adapter.write_records_to_storage.assert_called_once_with(
            integration_name=integration_name, records=sample_records
        )

    def test_raises_error_when_adapter_write_fails(self, repository, mock_adapter, sample_records):
        """Test that error is propagated when adapter write fails."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        test_error = Exception("Adapter write error")
        mock_adapter.write_records_to_storage.side_effect = test_error

        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            repository.write_records_to_storage(
                integration_name=integration_name, records=sample_records
            )

        # Assert
        assert "Adapter write error" in str(exc_info.value)
        mock_adapter.write_records_to_storage.assert_called_once_with(
            integration_name=integration_name, records=sample_records
        )
