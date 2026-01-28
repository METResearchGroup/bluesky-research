"""Tests for backfill_data_loader_service.py."""

import pytest
from unittest.mock import Mock, patch

from services.backfill.models import PostScope, PostToEnqueueModel
from services.backfill.repositories.adapters import LocalStorageAdapter
from services.backfill.repositories.repository import BackfillDataRepository
from services.backfill.services.backfill_data_loader_service import (
    BackfillDataLoaderService,
)


class TestBackfillDataLoaderService__init__:
    """Tests for BackfillDataLoaderService.__init__ method."""

    def test_initializes_with_default_repository(self):
        """Test that service initializes with default repository using LocalStorageAdapter."""
        # Arrange & Act
        service = BackfillDataLoaderService()

        # Assert
        assert service.data_repository is not None
        assert isinstance(service.data_repository, BackfillDataRepository)
        assert isinstance(service.data_repository.adapter, LocalStorageAdapter)

    def test_initializes_with_custom_repository(self):
        """Test that service initializes with custom repository when provided."""
        # Arrange
        mock_repository = Mock(spec=BackfillDataRepository)

        # Act
        service = BackfillDataLoaderService(data_repository=mock_repository)

        # Assert
        assert service.data_repository == mock_repository


class TestBackfillDataLoaderService_load_posts_to_enqueue:
    """Tests for BackfillDataLoaderService.load_posts_to_enqueue method."""

    @pytest.fixture
    def service(self, mock_repository):
        """Create BackfillDataLoaderService with mocked repository."""
        return BackfillDataLoaderService(data_repository=mock_repository)

    def test_loads_and_filters_posts_for_all_posts_scope(
        self, service, mock_repository, sample_posts
    ):
        """Test that posts are loaded and filtered for ALL_POSTS scope."""
        # Arrange
        post_scope = PostScope.ALL_POSTS
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        filtered_posts = [sample_posts[0]]

        with patch.object(service, "_load_posts") as mock_load, patch.object(
            service, "_filter_posts"
        ) as mock_filter:
            mock_load.return_value = sample_posts
            mock_filter.return_value = filtered_posts

            # Act
            result = service.load_posts_to_enqueue(
                post_scope=post_scope,
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            mock_load.assert_called_once_with(
                post_scope=post_scope,
                start_date=start_date,
                end_date=end_date,
            )
            mock_filter.assert_called_once_with(
                posts=sample_posts,
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )
            assert result == filtered_posts

    def test_loads_and_filters_posts_for_feed_posts_scope(
        self, service, mock_repository, sample_posts
    ):
        """Test that posts are loaded and filtered for FEED_POSTS scope."""
        # Arrange
        post_scope = PostScope.FEED_POSTS
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        filtered_posts = sample_posts

        with patch.object(service, "_load_posts") as mock_load, patch.object(
            service, "_filter_posts"
        ) as mock_filter:
            mock_load.return_value = sample_posts
            mock_filter.return_value = filtered_posts

            # Act
            result = service.load_posts_to_enqueue(
                post_scope=post_scope,
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            mock_load.assert_called_once_with(
                post_scope=post_scope,
                start_date=start_date,
                end_date=end_date,
            )
            assert result == filtered_posts

    def test_returns_empty_list_when_no_posts_after_filtering(
        self, service, mock_repository
    ):
        """Test that empty list is returned when no posts remain after filtering."""
        # Arrange
        post_scope = PostScope.ALL_POSTS
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        with patch.object(service, "_load_posts") as mock_load, patch.object(
            service, "_filter_posts"
        ) as mock_filter:
            mock_load.return_value = []
            mock_filter.return_value = []

            # Act
            result = service.load_posts_to_enqueue(
                post_scope=post_scope,
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            assert result == []
            assert len(result) == 0


class TestBackfillDataLoaderService_load_posts:
    """Tests for BackfillDataLoaderService._load_posts method."""

    @pytest.fixture
    def service(self, mock_repository):
        """Create BackfillDataLoaderService with mocked repository."""
        return BackfillDataLoaderService(data_repository=mock_repository)

    @pytest.mark.parametrize("post_scope,expected_method", [
        (PostScope.ALL_POSTS, "_load_all_posts"),
        (PostScope.FEED_POSTS, "_load_feed_posts"),
    ])
    def test_routes_to_correct_loader(
        self, service, mock_repository, sample_posts, post_scope, expected_method
    ):
        """Test that _load_posts routes to correct loader based on post scope."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        mock_method = Mock(return_value=sample_posts[:1])  # Use single post for consistency
        service._post_scope_loaders[post_scope] = mock_method

        # Act
        result = service._load_posts(
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        mock_method.assert_called_once_with(start_date=start_date, end_date=end_date)
        assert result == sample_posts[:1]

    def test_raises_value_error_for_invalid_scope(self, service, mock_repository):
        """Test that invalid post scope raises ValueError."""
        # Arrange
        post_scope = "invalid_scope"
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid post scope: invalid_scope"):
            service._load_posts(
                post_scope=post_scope,
                start_date=start_date,
                end_date=end_date,
            )


class TestBackfillDataLoaderService_load_all_posts:
    """Tests for BackfillDataLoaderService._load_all_posts method."""

    @pytest.fixture
    def service(self, mock_repository):
        """Create BackfillDataLoaderService with mocked repository."""
        return BackfillDataLoaderService(data_repository=mock_repository)

    def test_delegates_to_repository_load_all_posts(
        self, service, mock_repository, sample_posts
    ):
        """Test that _load_all_posts delegates to repository."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_repository.load_all_posts.return_value = sample_posts

        # Act
        result = service._load_all_posts(start_date=start_date, end_date=end_date)

        # Assert
        mock_repository.load_all_posts.assert_called_once_with(
            start_date=start_date, end_date=end_date
        )
        assert result == sample_posts


class TestBackfillDataLoaderService_load_feed_posts:
    """Tests for BackfillDataLoaderService._load_feed_posts method."""

    @pytest.fixture
    def service(self, mock_repository):
        """Create BackfillDataLoaderService with mocked repository."""
        return BackfillDataLoaderService(data_repository=mock_repository)

    def test_delegates_to_repository_load_feed_posts(
        self, service, mock_repository, sample_posts
    ):
        """Test that _load_feed_posts delegates to repository."""
        # Arrange
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_repository.load_feed_posts.return_value = sample_posts

        # Act
        result = service._load_feed_posts(start_date=start_date, end_date=end_date)

        # Assert
        mock_repository.load_feed_posts.assert_called_once_with(
            start_date=start_date, end_date=end_date
        )
        assert result == sample_posts


class TestBackfillDataLoaderService_filter_posts:
    """Tests for BackfillDataLoaderService._filter_posts method."""

    @pytest.fixture
    def service(self, mock_repository):
        """Create BackfillDataLoaderService with mocked repository."""
        return BackfillDataLoaderService(data_repository=mock_repository)

    def test_delegates_to_remove_previously_classified_posts(
        self, service, mock_repository
    ):
        """Test that _filter_posts delegates to _remove_previously_classified_posts."""
        # Arrange
        posts = [
            PostToEnqueueModel(
                uri="test_uri_1",
                text="test_text_1",
                preprocessing_timestamp="2024-01-01T00:00:00",
            ),
        ]
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        filtered_posts = posts

        with patch.object(
            service, "_remove_previously_classified_posts"
        ) as mock_remove:
            mock_remove.return_value = filtered_posts

            # Act
            result = service._filter_posts(
                posts=posts,
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            mock_remove.assert_called_once_with(
                posts=posts,
                integration_name=integration_name,
                start_date=start_date,
                end_date=end_date,
            )
            assert result == filtered_posts


class TestBackfillDataLoaderService_remove_previously_classified_posts:
    """Tests for BackfillDataLoaderService._remove_previously_classified_posts method."""

    @pytest.fixture
    def service(self, mock_repository):
        """Create BackfillDataLoaderService with mocked repository."""
        return BackfillDataLoaderService(data_repository=mock_repository)

    @pytest.fixture
    def sample_posts_for_filtering(self):
        """Sample posts for filtering tests."""
        return [
            PostToEnqueueModel(
                uri="uri1",
                text="text1",
                preprocessing_timestamp="2024-01-01T00:00:00",
            ),
            PostToEnqueueModel(
                uri="uri2",
                text="text2",
                preprocessing_timestamp="2024-01-02T00:00:00",
            ),
            PostToEnqueueModel(
                uri="uri3",
                text="text3",
                preprocessing_timestamp="2024-01-03T00:00:00",
            ),
        ]

    def test_filters_out_previously_classified_posts(
        self, service, mock_repository, sample_posts_for_filtering
    ):
        """Test that posts with URIs in classified set are filtered out."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        classified_uris = {"uri1", "uri3"}
        expected_posts = [sample_posts_for_filtering[1]]  # Only uri2 remains

        mock_repository.get_previously_labeled_post_uris.return_value = classified_uris

        # Act
        result = service._remove_previously_classified_posts(
            posts=sample_posts_for_filtering,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        mock_repository.get_previously_labeled_post_uris.assert_called_once_with(
            service=integration_name,
            id_field="uri",
            start_date=start_date,
            end_date=end_date,
        )
        assert len(result) == 1
        assert result[0].uri == "uri2"

    def test_returns_all_posts_when_none_classified(
        self, service, mock_repository, sample_posts_for_filtering
    ):
        """Test that all posts are returned when none are classified."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        classified_uris = set()

        mock_repository.get_previously_labeled_post_uris.return_value = classified_uris

        # Act
        result = service._remove_previously_classified_posts(
            posts=sample_posts_for_filtering,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        assert result == sample_posts_for_filtering
        assert len(result) == 3

    def test_returns_empty_list_when_all_posts_classified(
        self, service, mock_repository, sample_posts_for_filtering
    ):
        """Test that empty list is returned when all posts are classified."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        classified_uris = {"uri1", "uri2", "uri3"}

        mock_repository.get_previously_labeled_post_uris.return_value = classified_uris

        # Act
        result = service._remove_previously_classified_posts(
            posts=sample_posts_for_filtering,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        assert result == []
        assert len(result) == 0

    def test_returns_empty_list_when_input_posts_empty(self, service, mock_repository):
        """Test that empty list is returned when input posts list is empty."""
        # Arrange
        posts = []
        integration_name = "ml_inference_perspective_api"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        classified_uris = {"uri1"}

        mock_repository.get_previously_labeled_post_uris.return_value = classified_uris

        # Act
        result = service._remove_previously_classified_posts(
            posts=posts,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        assert result == []
        assert len(result) == 0
