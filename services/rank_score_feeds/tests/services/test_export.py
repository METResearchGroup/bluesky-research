"""Tests for DataExporterService."""

from unittest.mock import Mock

import pytest

from services.rank_score_feeds.models import (
    CustomFeedPost,
    FeedGenerationSessionAnalytics,
    FeedWithMetadata,
    StoredFeedModel,
)
from services.rank_score_feeds.repositories.feed_repo import FeedStorageRepository
from services.rank_score_feeds.services.export import DataExporterService
from services.rank_score_feeds.storage.base import FeedStorageAdapter
from services.rank_score_feeds.storage.exceptions import StorageError


class _FakeAdapter(FeedStorageAdapter):
    """Concrete adapter for constructing a real FeedStorageRepository in tests."""

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        return None

    def write_feed_generation_session_analytics(
        self, feed_generation_session_analytics: FeedGenerationSessionAnalytics, timestamp: str
    ) -> None:
        return None


class TestDataExporterService:
    """Tests for DataExporterService."""

    def test_init_accepts_valid_repository_and_rejects_invalid(self):
        """Constructor accepts FeedStorageRepository and rejects invalid type."""
        # Arrange
        repo = FeedStorageRepository(adapter=_FakeAdapter())

        # Act
        service = DataExporterService(feed_storage_repository=repo)

        # Assert
        assert isinstance(service, DataExporterService)

        # Invalid type: not a FeedStorageRepository
        with pytest.raises(ValueError, match="feed_storage_repository must be an instance of FeedStorageRepository"):
            DataExporterService(feed_storage_repository=object())  # type: ignore[arg-type]

    def test_export_feeds_transforms_and_calls_repository(self):
        """export_feeds transforms inputs into StoredFeedModel list and calls repository."""
        # Arrange
        repo = FeedStorageRepository(adapter=_FakeAdapter())
        repo.write_feeds = Mock()  # type: ignore[method-assign]
        service = DataExporterService(feed_storage_repository=repo)
        timestamp = "2024-01-02T03:04:05"

        user_to_ranked_feed_map = {
            "did:1": FeedWithMetadata(
                feed=[CustomFeedPost(item="p1", is_in_network=True)],
                bluesky_handle="handle1",
                user_did="did:1",
                condition="engagement",
                feed_statistics='{"prop_in_network":1.0}',
            ),
            "did:2": FeedWithMetadata(
                feed=[CustomFeedPost(item="p2", is_in_network=False)],
                bluesky_handle="handle2",
                user_did="did:2",
                condition="representative_diversification",
                feed_statistics='{"prop_in_network":0.0}',
            ),
        }

        # Act
        service.export_feeds(user_to_ranked_feed_map=user_to_ranked_feed_map, timestamp=timestamp)

        # Assert
        repo.write_feeds.assert_called_once()
        args, kwargs = repo.write_feeds.call_args
        written_models: list[StoredFeedModel] = kwargs["feeds"]
        written_timestamp = kwargs["timestamp"]

        assert written_timestamp == timestamp
        assert isinstance(written_models, list)
        assert len(written_models) == 2
        # Validate a couple fields on each transformed model
        model1 = next(m for m in written_models if m.user == "did:1")
        assert model1.feed_id == f"did:1::{timestamp}"
        assert model1.bluesky_handle == "handle1"
        assert model1.condition == "engagement"
        assert model1.feed[0].item == "p1"
        assert model1.feed_statistics == '{"prop_in_network":1.0}'

        model2 = next(m for m in written_models if m.user == "did:2")
        assert model2.feed_id == f"did:2::{timestamp}"
        assert model2.bluesky_handle == "handle2"
        assert model2.condition == "representative_diversification"
        assert model2.feed[0].item == "p2"
        assert model2.feed_statistics == '{"prop_in_network":0.0}'

    def test_export_feeds_propagates_storage_error_on_failure(self):
        """export_feeds propagates StorageError when repository write fails."""
        # Arrange
        repo = FeedStorageRepository(adapter=_FakeAdapter())
        repo.write_feeds = Mock(side_effect=StorageError("boom"))  # type: ignore[method-assign]
        service = DataExporterService(feed_storage_repository=repo)
        timestamp = "2024-01-02T03:04:05"
        user_to_ranked_feed_map = {
            "did:1": FeedWithMetadata(
                feed=[CustomFeedPost(item="p1", is_in_network=True)],
                bluesky_handle="handle1",
                user_did="did:1",
                condition="engagement",
                feed_statistics="{}",
            )
        }

        # Act & Assert
        with pytest.raises(StorageError, match="boom"):
            service.export_feeds(user_to_ranked_feed_map=user_to_ranked_feed_map, timestamp=timestamp)

    def test_export_session_analytics_calls_repository(self):
        """export_feed_generation_session_analytics forwards call with same params."""
        # Arrange
        repo = FeedStorageRepository(adapter=_FakeAdapter())
        repo.write_feed_generation_session_analytics = Mock()  # type: ignore[method-assign]
        service = DataExporterService(feed_storage_repository=repo)
        timestamp = "2024-01-02T03:04:05"
        analytics = FeedGenerationSessionAnalytics(
            total_feeds=2,
            total_posts=5,
            total_in_network_posts=3,
            total_in_network_posts_prop=0.6,
            total_unique_engagement_uris=3,
            total_unique_treatment_uris=4,
            prop_overlap_treatment_uris_in_engagement_uris=0.5,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={
                "representative_diversification": 1,
                "engagement": 1,
                "reverse_chronological": 0,
            },
            session_timestamp="2024-01-01T00:00:00",
        )

        # Act
        service.export_feed_generation_session_analytics(
            feed_generation_session_analytics=analytics, timestamp=timestamp
        )

        # Assert
        repo.write_feed_generation_session_analytics.assert_called_once_with(
            feed_generation_session_analytics=analytics, timestamp=timestamp
        )

    def test_export_session_analytics_propagates_storage_error_on_failure(self):
        """export_feed_generation_session_analytics propagates StorageError on failure."""
        # Arrange
        repo = FeedStorageRepository(adapter=_FakeAdapter())
        repo.write_feed_generation_session_analytics = Mock(side_effect=StorageError("down"))  # type: ignore[method-assign]
        service = DataExporterService(feed_storage_repository=repo)
        timestamp = "2024-01-02T03:04:05"
        analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=2,
            total_in_network_posts=1,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=1,
            total_unique_treatment_uris=1,
            prop_overlap_treatment_uris_in_engagement_uris=1.0,
            prop_overlap_engagement_uris_in_treatment_uris=1.0,
            total_feeds_per_condition={
                "representative_diversification": 0,
                "engagement": 1,
                "reverse_chronological": 0,
            },
            session_timestamp="2024-01-01T00:00:00",
        )

        # Act & Assert
        with pytest.raises(StorageError, match="down"):
            service.export_feed_generation_session_analytics(
                feed_generation_session_analytics=analytics, timestamp=timestamp
            )

    def test_transform_feed_with_metadata_to_export_models_outputs_expected(self):
        """_transform_feed_with_metadata_to_export_models produces correct StoredFeedModel list."""
        # Arrange
        repo = FeedStorageRepository(adapter=_FakeAdapter())
        service = DataExporterService(feed_storage_repository=repo)
        timestamp = "2024-01-02T03:04:05"
        user_to_ranked_feed_map = {
            "did:1": FeedWithMetadata(
                feed=[CustomFeedPost(item="x", is_in_network=True)],
                bluesky_handle="h1",
                user_did="did:1",
                condition="engagement",
                feed_statistics="{}",
            )
        }

        # Act
        models = service._transform_feed_with_metadata_to_export_models(
            user_to_ranked_feed_map=user_to_ranked_feed_map, timestamp=timestamp
        )

        # Assert
        assert isinstance(models, list)
        assert len(models) == 1
        model = models[0]
        assert model.feed_id == f"did:1::{timestamp}"
        assert model.user == "did:1"
        assert model.bluesky_handle == "h1"
        assert model.bluesky_user_did == "did:1"
        assert model.condition == "engagement"
        assert model.feed[0].item == "x"
        assert model.feed_generation_timestamp == timestamp
