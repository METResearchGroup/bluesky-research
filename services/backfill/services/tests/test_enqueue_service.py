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
from services.backfill.services.enqueue_service import (
    EnqueueService,
    _deterministically_sample_posts_by_uri,
)


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
            record_type="posts",
            integrations=["ml_inference_perspective_api", "ml_inference_sociopolitical"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

    def test_enqueues_records_for_multiple_integrations(
        self, service, mock_data_loader, mock_queue_manager, sample_payload, sample_posts
    ):
        """Test that records are enqueued for all specified integrations."""
        # Arrange
        mock_data_loader.load_posts_by_scope.return_value = sample_posts
        mock_data_loader.filter_out_previously_classified_posts.return_value = (
            sample_posts
        )

        with patch("services.backfill.services.enqueue_service.logger") as mock_logger:
            # Act
            service.enqueue_records(payload=sample_payload)

            # Assert
            mock_data_loader.load_posts_by_scope.assert_called_once_with(
                post_scope=PostScope.ALL_POSTS,
                start_date=sample_payload.start_date,
                end_date=sample_payload.end_date,
            )
            assert (
                mock_data_loader.filter_out_previously_classified_posts.call_count == 2
            )
            assert mock_queue_manager.insert_posts_to_queue.call_count == 2
            # Verify first integration
            first_call_args = (
                mock_data_loader.filter_out_previously_classified_posts.call_args_list[0]
            )
            assert (
                first_call_args.kwargs["integration_name"]
                == "ml_inference_perspective_api"
            )
            assert first_call_args.kwargs["posts"] == sample_posts
            # Verify second integration
            second_call_args = (
                mock_data_loader.filter_out_previously_classified_posts.call_args_list[
                    1
                ]
            )
            assert (
                second_call_args.kwargs["integration_name"]
                == "ml_inference_sociopolitical"
            )
            assert second_call_args.kwargs["posts"] == sample_posts

    def test_logs_progress_for_each_integration(
        self, service, mock_data_loader, mock_queue_manager, sample_payload, sample_posts
    ):
        """Test that progress is logged for each integration."""
        # Arrange
        mock_data_loader.load_posts_by_scope.return_value = sample_posts
        mock_data_loader.filter_out_previously_classified_posts.return_value = (
            sample_posts
        )

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
            record_type="posts_used_in_feeds",
            integrations=["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        mock_data_loader.load_posts_by_scope.return_value = sample_posts
        mock_data_loader.filter_out_previously_classified_posts.return_value = (
            sample_posts
        )

        # Act
        service.enqueue_records(payload=payload)

        # Assert
        mock_data_loader.load_posts_by_scope.assert_called_once()
        mock_data_loader.filter_out_previously_classified_posts.assert_called_once()
        mock_queue_manager.insert_posts_to_queue.assert_called_once()

    def test_raises_enqueue_service_error_on_exception(
        self, service, mock_data_loader, mock_queue_manager, sample_payload
    ):
        """Test that EnqueueServiceError is raised when exception occurs."""
        # Arrange
        mock_data_loader.load_posts_by_scope.side_effect = Exception("Test error")

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
        mock_data_loader.load_posts_by_scope.return_value = sample_posts
        mock_data_loader.filter_out_previously_classified_posts.side_effect = (
            lambda **kwargs: call_order.append(("filter", kwargs["integration_name"]))
            or sample_posts
        )
        mock_queue_manager.insert_posts_to_queue.side_effect = (
            lambda **kwargs: call_order.append(("insert", kwargs["integration_name"]))
            or None
        )

        # Act
        service.enqueue_records(payload=sample_payload)

        # Assert
        # Verify order: filter, insert, filter, insert
        expected_order = [
            ("filter", "ml_inference_perspective_api"),
            ("insert", "ml_inference_perspective_api"),
            ("filter", "ml_inference_sociopolitical"),
            ("insert", "ml_inference_sociopolitical"),
        ]
        assert call_order == expected_order

    def test_sampling_is_applied_once_and_shared_across_integrations(
        self, service, mock_data_loader, mock_queue_manager, sample_posts
    ):
        """Test that sampling happens once and the sampled base is passed to each integration filter.

        This guards the requirement that the *same* sample is used across integrations.
        """
        # Arrange
        payload = EnqueueServicePayload(
            record_type="posts",
            integrations=["ml_inference_perspective_api", "ml_inference_sociopolitical"],
            start_date="2024-01-01",
            end_date="2024-01-31",
            sample_records=True,
            sample_proportion=0.123,
        )
        mock_data_loader.load_posts_by_scope.return_value = sample_posts
        expected_sample = [sample_posts[0]]
        mock_data_loader.filter_out_previously_classified_posts.return_value = (
            expected_sample
        )

        with patch(
            "services.backfill.services.enqueue_service._deterministically_sample_posts_by_uri",
            return_value=expected_sample,
        ) as mock_sample:
            # Act
            service.enqueue_records(payload=payload)

            # Assert
            mock_sample.assert_called_once_with(
                posts=sample_posts,
                sample_proportion=float(payload.sample_proportion),
            )
            first_filter_call = (
                mock_data_loader.filter_out_previously_classified_posts.call_args_list[
                    0
                ]
            )
            second_filter_call = (
                mock_data_loader.filter_out_previously_classified_posts.call_args_list[
                    1
                ]
            )
            assert first_filter_call.kwargs["posts"] == expected_sample
            assert second_filter_call.kwargs["posts"] == expected_sample
            assert mock_queue_manager.insert_posts_to_queue.call_count == 2


class Test_deterministically_sample_posts_by_uri:
    """Tests for _deterministically_sample_posts_by_uri helper."""

    def test_returns_empty_list_for_zero_proportion(self, sample_posts):
        """Test that 0.0 returns an empty sample."""
        # Arrange
        sample_proportion = 0.0

        # Act
        result = _deterministically_sample_posts_by_uri(
            posts=sample_posts, sample_proportion=sample_proportion
        )

        # Assert
        assert result == []

    def test_returns_all_posts_for_one_proportion(self, sample_posts):
        """Test that 1.0 returns the full input list."""
        # Arrange
        sample_proportion = 1.0

        # Act
        result = _deterministically_sample_posts_by_uri(
            posts=sample_posts, sample_proportion=sample_proportion
        )

        # Assert
        assert result == sample_posts

    def test_is_deterministic_across_repeated_calls(self, sample_posts):
        """Test that sampling is deterministic for the same inputs."""
        # Arrange
        sample_proportion = 0.5

        # Act
        first = _deterministically_sample_posts_by_uri(
            posts=sample_posts, sample_proportion=sample_proportion
        )
        second = _deterministically_sample_posts_by_uri(
            posts=sample_posts, sample_proportion=sample_proportion
        )

        # Assert
        assert first == second
        assert all(isinstance(p, PostToEnqueueModel) for p in first)
