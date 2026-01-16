"""Tests for repository.py."""

import pytest
from unittest.mock import Mock, MagicMock

from services.backfill.models import PostToEnqueueModel
from services.backfill.repositories.base import BackfillDataAdapter
from services.backfill.repositories.repository import BackfillDataRepository


class TestBackfillDataRepository__init__:
    """Tests for BackfillDataRepository.__init__ method."""

    def test_initializes_with_valid_adapter(self):
        """Test that repository initializes successfully with a valid adapter."""
        # Arrange
        mock_adapter = Mock(spec=BackfillDataAdapter)

        # Act
        repository = BackfillDataRepository(adapter=mock_adapter)

        # Assert
        assert repository.adapter == mock_adapter
        assert repository.logger is not None

    def test_raises_value_error_for_invalid_adapter(self):
        """Test that invalid adapter type raises ValueError."""
        # Arrange
        invalid_adapter = "not_an_adapter"

        # Act & Assert
        with pytest.raises(ValueError, match="Adapter must be an instance of BackfillDataAdapter"):
            BackfillDataRepository(adapter=invalid_adapter)

    def test_raises_value_error_for_none_adapter(self):
        """Test that None adapter raises ValueError."""
        # Arrange
        invalid_adapter = None

        # Act & Assert
        with pytest.raises(ValueError, match="Adapter must be an instance of BackfillDataAdapter"):
            BackfillDataRepository(adapter=invalid_adapter)


class TestBackfillDataRepository_load_all_posts:
    """Tests for BackfillDataRepository.load_all_posts method."""

    @pytest.fixture
    def mock_adapter(self):
        """Mock BackfillDataAdapter."""
        return Mock(spec=BackfillDataAdapter)

    @pytest.fixture
    def repository(self, mock_adapter):
        """Create BackfillDataRepository with mocked adapter."""
        return BackfillDataRepository(adapter=mock_adapter)

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter):
        """Test that load_all_posts delegates to adapter with correct parameters."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        expected_posts = [
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
        mock_adapter.load_all_posts.return_value = expected_posts

        # Act
        result = repository.load_all_posts(start_date=start_date, end_date=end_date)

        # Assert
        mock_adapter.load_all_posts.assert_called_once_with(
            start_date=start_date, end_date=end_date
        )
        assert result == expected_posts

    def test_returns_empty_list_when_adapter_returns_empty(self, repository, mock_adapter):
        """Test that empty list is returned when adapter returns empty list."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        expected_posts = []
        mock_adapter.load_all_posts.return_value = expected_posts

        # Act
        result = repository.load_all_posts(start_date=start_date, end_date=end_date)

        # Assert
        assert result == expected_posts
        assert len(result) == 0


class TestBackfillDataRepository_load_feed_posts:
    """Tests for BackfillDataRepository.load_feed_posts method."""

    @pytest.fixture
    def mock_adapter(self):
        """Mock BackfillDataAdapter."""
        return Mock(spec=BackfillDataAdapter)

    @pytest.fixture
    def repository(self, mock_adapter):
        """Create BackfillDataRepository with mocked adapter."""
        return BackfillDataRepository(adapter=mock_adapter)

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter):
        """Test that load_feed_posts delegates to adapter with correct parameters."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        expected_posts = [
            PostToEnqueueModel(
                uri="test_uri_1",
                text="test_text_1",
                preprocessing_timestamp="2024-01-01T00:00:00",
            ),
        ]
        mock_adapter.load_feed_posts.return_value = expected_posts

        # Act
        result = repository.load_feed_posts(start_date=start_date, end_date=end_date)

        # Assert
        mock_adapter.load_feed_posts.assert_called_once_with(
            start_date=start_date, end_date=end_date
        )
        assert result == expected_posts

    def test_returns_empty_list_when_adapter_returns_empty(self, repository, mock_adapter):
        """Test that empty list is returned when adapter returns empty list."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        expected_posts = []
        mock_adapter.load_feed_posts.return_value = expected_posts

        # Act
        result = repository.load_feed_posts(start_date=start_date, end_date=end_date)

        # Assert
        assert result == expected_posts
        assert len(result) == 0


class TestBackfillDataRepository_get_previously_labeled_post_uris:
    """Tests for BackfillDataRepository.get_previously_labeled_post_uris method."""

    @pytest.fixture
    def mock_adapter(self):
        """Mock BackfillDataAdapter."""
        return Mock(spec=BackfillDataAdapter)

    @pytest.fixture
    def repository(self, mock_adapter):
        """Create BackfillDataRepository with mocked adapter."""
        return BackfillDataRepository(adapter=mock_adapter)

    def test_delegates_to_adapter_with_correct_parameters(self, repository, mock_adapter):
        """Test that get_previously_labeled_post_uris delegates to adapter with correct parameters."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        expected_uris = {"uri1", "uri2", "uri3"}
        mock_adapter.get_previously_labeled_post_uris.return_value = expected_uris

        # Act
        result = repository.get_previously_labeled_post_uris(
            service=service,
            id_field=id_field,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        mock_adapter.get_previously_labeled_post_uris.assert_called_once_with(
            service=service,
            id_field=id_field,
            start_date=start_date,
            end_date=end_date,
        )
        assert result == expected_uris

    def test_returns_empty_set_when_adapter_returns_empty(self, repository, mock_adapter):
        """Test that empty set is returned when adapter returns empty set."""
        # Arrange
        service = "ml_inference_perspective_api"
        id_field = "uri"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        expected_uris = set()
        mock_adapter.get_previously_labeled_post_uris.return_value = expected_uris

        # Act
        result = repository.get_previously_labeled_post_uris(
            service=service,
            id_field=id_field,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        assert result == expected_uris
        assert len(result) == 0
