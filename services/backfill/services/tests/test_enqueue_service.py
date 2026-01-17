"""Tests for enqueue_service.py."""

import pytest
from unittest.mock import Mock, patch

from services.backfill.models import (
    EnqueueServicePayload,
    PostScope,
    PostToEnqueueModel,
)
from services.backfill.exceptions import EnqueueServiceError
from services.backfill.services.backfill_data_loader_service import (
    BackfillDataLoaderService,
)
from services.backfill.services.queue_manager_service import QueueManagerService
from services.backfill.services.enqueue_service import EnqueueService


class TestEnqueueService__init__:
    """Tests for EnqueueService.__init__ method."""

    def test_initializes_with_default_services(self):
        """Test that service initializes with default services."""
        # Arrange & Act
        service = EnqueueService()

        # Assert
        assert service.backfill_data_loader_service is not None
        assert isinstance(
            service.backfill_data_loader_service, BackfillDataLoaderService
        )
        assert service.queue_manager_service is not None
        assert isinstance(service.queue_manager_service, QueueManagerService)

    def test_initializes_with_custom_services(self):
        """Test that service initializes with custom services when provided."""
        # Arrange
        mock_data_loader = Mock(spec=BackfillDataLoaderService)
        mock_queue_manager = Mock(spec=QueueManagerService)

        # Act
        service = EnqueueService(
            backfill_data_loader_service=mock_data_loader,
            queue_manager_service=mock_queue_manager,
        )

        # Assert
        assert service.backfill_data_loader_service == mock_data_loader
        assert service.queue_manager_service == mock_queue_manager


class TestEnqueueService_enqueue_records:
    """Tests for EnqueueService.enqueue_records method."""

    @pytest.fixture
    def mock_data_loader(self):
        """Mock BackfillDataLoaderService."""
        return Mock(spec=BackfillDataLoaderService)

    @pytest.fixture
    def mock_queue_manager(self):
        """Mock QueueManagerService."""
        return Mock(spec=QueueManagerService)

    @pytest.fixture
    def service(self, mock_data_loader, mock_queue_manager):
        """Create EnqueueService with mocked services."""
        return EnqueueService(
            backfill_data_loader_service=mock_data_loader,
            queue_manager_service=mock_queue_manager,
        )

    @pytest.fixture
    def sample_payload(self):
        """Sample EnqueueServicePayload for testing."""
        return EnqueueServicePayload(
            record_type="all_posts",
            integrations=["ml_inference_perspective_api", "ml_inference_sociopolitical"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

    @pytest.fixture
    def sample_posts(self):
        """Sample posts for testing."""
        return [
            PostToEnqueueModel(
                uri="test_uri_1",
                text="test_text_1",
                preprocessing_timestamp="2024-01-01T00:00:00",
            ),
        ]

    def test_enqueues_records_for_multiple_integrations(
        self, service, mock_data_loader, mock_queue_manager, sample_payload, sample_posts
    ):
        """Test that records are enqueued for all specified integrations."""
        # Arrange
        mock_data_loader.load_posts_to_enqueue.return_value = sample_posts

        with patch("services.backfill.services.enqueue_service.logger") as mock_logger:
            # Act
            service.enqueue_records(payload=sample_payload)

            # Assert
            assert mock_data_loader.load_posts_to_enqueue.call_count == 2
            assert mock_queue_manager.insert_posts_to_queue.call_count == 2
            # Verify first integration
            first_call_args = mock_data_loader.load_posts_to_enqueue.call_args_list[0]
            assert first_call_args.kwargs["integration_name"] == "ml_inference_perspective_api"
            assert first_call_args.kwargs["post_scope"] == PostScope.ALL_POSTS
            # Verify second integration
            second_call_args = mock_data_loader.load_posts_to_enqueue.call_args_list[1]
            assert second_call_args.kwargs["integration_name"] == "ml_inference_sociopolitical"
            assert second_call_args.kwargs["post_scope"] == PostScope.ALL_POSTS

    def test_logs_progress_for_each_integration(
        self, service, mock_data_loader, mock_queue_manager, sample_payload, sample_posts
    ):
        """Test that progress is logged for each integration."""
        # Arrange
        mock_data_loader.load_posts_to_enqueue.return_value = sample_posts

        with patch("services.backfill.services.enqueue_service.logger") as mock_logger:
            # Act
            service.enqueue_records(payload=sample_payload)

            # Assert
            assert mock_logger.info.call_count >= 4  # Start and completion for each integration, plus final completion
            log_messages = [str(call) for call in mock_logger.info.call_args_list]
            # Check that progress messages are logged
            assert any("Progress" in log_message for log_message in log_messages)

    def test_handles_single_integration(
        self, service, mock_data_loader, mock_queue_manager, sample_posts
    ):
        """Test that single integration is handled correctly."""
        # Arrange
        payload = EnqueueServicePayload(
            record_type="feed_posts",
            integrations=["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        mock_data_loader.load_posts_to_enqueue.return_value = sample_posts

        # Act
        service.enqueue_records(payload=payload)

        # Assert
        mock_data_loader.load_posts_to_enqueue.assert_called_once()
        mock_queue_manager.insert_posts_to_queue.assert_called_once()

    def test_raises_enqueue_service_error_on_exception(
        self, service, mock_data_loader, mock_queue_manager, sample_payload
    ):
        """Test that EnqueueServiceError is raised when exception occurs."""
        # Arrange
        mock_data_loader.load_posts_to_enqueue.side_effect = Exception("Test error")

        with patch("services.backfill.services.enqueue_service.logger") as mock_logger:
            # Act & Assert
            with pytest.raises(EnqueueServiceError) as exc_info:
                service.enqueue_records(payload=sample_payload)

            # Assert
            assert "Error enqueuing records" in str(exc_info.value)
            mock_logger.error.assert_called_once()

    def test_processes_integrations_sequentially(
        self, service, mock_data_loader, mock_queue_manager, sample_payload, sample_posts
    ):
        """Test that integrations are processed sequentially."""
        # Arrange
        call_order = []
        mock_data_loader.load_posts_to_enqueue.side_effect = lambda **kwargs: call_order.append(
            ("load", kwargs["integration_name"])
        ) or sample_posts
        mock_queue_manager.insert_posts_to_queue.side_effect = lambda **kwargs: call_order.append(
            ("insert", kwargs["integration_name"])
        ) or None

        # Act
        service.enqueue_records(payload=sample_payload)

        # Assert
        # Verify order: load, insert, load, insert
        expected_order = [
            ("load", "ml_inference_perspective_api"),
            ("insert", "ml_inference_perspective_api"),
            ("load", "ml_inference_sociopolitical"),
            ("insert", "ml_inference_sociopolitical"),
        ]
        assert call_order == expected_order


class TestEnqueueService_enqueue_records_for_single_integration:
    """Tests for EnqueueService._enqueue_records_for_single_integration method."""

    @pytest.fixture
    def mock_data_loader(self):
        """Mock BackfillDataLoaderService."""
        return Mock(spec=BackfillDataLoaderService)

    @pytest.fixture
    def mock_queue_manager(self):
        """Mock QueueManagerService."""
        return Mock(spec=QueueManagerService)

    @pytest.fixture
    def service(self, mock_data_loader, mock_queue_manager):
        """Create EnqueueService with mocked services."""
        return EnqueueService(
            backfill_data_loader_service=mock_data_loader,
            queue_manager_service=mock_queue_manager,
        )

    @pytest.fixture
    def sample_posts(self):
        """Sample posts for testing."""
        return [
            PostToEnqueueModel(
                uri="test_uri_1",
                text="test_text_1",
                preprocessing_timestamp="2024-01-01T00:00:00",
            ),
        ]

    def test_enqueues_posts_for_single_integration(
        self, service, mock_data_loader, mock_queue_manager, sample_posts
    ):
        """Test that posts are loaded and inserted for a single integration."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        post_scope = PostScope.ALL_POSTS
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_data_loader.load_posts_to_enqueue.return_value = sample_posts

        # Act
        service._enqueue_records_for_single_integration(
            integration_name=integration_name,
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        mock_data_loader.load_posts_to_enqueue.assert_called_once_with(
            integration_name=integration_name,
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )
        mock_queue_manager.insert_posts_to_queue.assert_called_once_with(
            integration_name=integration_name,
            posts=sample_posts,
        )

    def test_logs_warning_and_returns_when_no_posts(
        self, service, mock_data_loader, mock_queue_manager
    ):
        """Test that warning is logged and method returns when no posts to enqueue."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        post_scope = PostScope.ALL_POSTS
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_data_loader.load_posts_to_enqueue.return_value = []

        with patch("services.backfill.services.enqueue_service.logger") as mock_logger:
            # Act
            service._enqueue_records_for_single_integration(
                integration_name=integration_name,
                post_scope=post_scope,
                start_date=start_date,
                end_date=end_date,
            )

            # Assert
            mock_data_loader.load_posts_to_enqueue.assert_called_once()
            mock_queue_manager.insert_posts_to_queue.assert_not_called()
            mock_logger.warning.assert_called_once()
            warning_message = str(mock_logger.warning.call_args)
            assert "No posts to enqueue" in warning_message
            assert integration_name in warning_message

    def test_inserts_to_queue_when_posts_available(
        self, service, mock_data_loader, mock_queue_manager, sample_posts
    ):
        """Test that posts are inserted to queue when available."""
        # Arrange
        integration_name = "ml_inference_perspective_api"
        post_scope = PostScope.FEED_POSTS
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        mock_data_loader.load_posts_to_enqueue.return_value = sample_posts

        # Act
        service._enqueue_records_for_single_integration(
            integration_name=integration_name,
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )

        # Assert
        mock_queue_manager.insert_posts_to_queue.assert_called_once_with(
            integration_name=integration_name,
            posts=sample_posts,
        )
