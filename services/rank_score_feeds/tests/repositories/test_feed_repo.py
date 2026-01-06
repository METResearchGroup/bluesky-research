"""Tests for FeedStorageRepository."""

from unittest.mock import Mock

import pytest

from services.rank_score_feeds.models import (
    CustomFeedPost,
    FeedGenerationSessionAnalytics,
    StoredFeedModel,
)
from services.rank_score_feeds.repositories.feed_repo import FeedStorageRepository
from services.rank_score_feeds.storage.base import FeedStorageAdapter
from services.rank_score_feeds.storage.exceptions import StorageError


class _FakeAdapter(FeedStorageAdapter):
    """Concrete adapter for constructing a repository in tests."""

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        return None

    def write_feed_generation_session_analytics(
        self, feed_generation_session_analytics: FeedGenerationSessionAnalytics, timestamp: str
    ) -> None:
        return None


class TestFeedStorageRepository:
    """Tests for FeedStorageRepository."""

    def test_init_accepts_valid_adapter_and_rejects_invalid(self):
        """Constructor accepts FeedStorageAdapter and rejects invalid type."""
        # Arrange & Act
        repo = FeedStorageRepository(adapter=_FakeAdapter())

        # Assert
        assert isinstance(repo, FeedStorageRepository)

        # Invalid adapter type should raise
        with pytest.raises(ValueError, match="Adapter must be an instance of FeedStorageAdapter."):
            FeedStorageRepository(adapter=object())  # type: ignore[arg-type]

    def test_write_feeds_calls_adapter(self):
        """write_feeds forwards call to adapter with same args."""
        # Arrange
        adapter = _FakeAdapter()
        adapter.write_feeds = Mock()  # type: ignore[method-assign]
        repo = FeedStorageRepository(adapter=adapter)
        timestamp = "2024-01-02T03:04:05"
        feeds = [
            StoredFeedModel(
                feed_id=f"did:1::{timestamp}",
                user="did:1",
                bluesky_handle="h1",
                bluesky_user_did="did:1",
                condition="engagement",
                feed_statistics="{}",
                feed=[CustomFeedPost(item="p1", is_in_network=True)],
                feed_generation_timestamp=timestamp,
            )
        ]

        # Act
        repo.write_feeds(feeds=feeds, timestamp=timestamp)

        # Assert
        adapter.write_feeds.assert_called_once_with(feeds=feeds, timestamp=timestamp)

    def test_write_feeds_propagates_storage_error(self):
        """write_feeds propagates StorageError from adapter."""
        # Arrange
        adapter = _FakeAdapter()
        adapter.write_feeds = Mock(side_effect=StorageError("boom"))  # type: ignore[method-assign]
        repo = FeedStorageRepository(adapter=adapter)
        timestamp = "2024-01-02T03:04:05"
        feeds = [
            StoredFeedModel(
                feed_id=f"did:1::{timestamp}",
                user="did:1",
                bluesky_handle="h1",
                bluesky_user_did="did:1",
                condition="engagement",
                feed_statistics="{}",
                feed=[CustomFeedPost(item="p1", is_in_network=True)],
                feed_generation_timestamp=timestamp,
            )
        ]

        # Act & Assert
        with pytest.raises(StorageError, match="boom"):
            repo.write_feeds(feeds=feeds, timestamp=timestamp)

    def test_write_session_analytics_calls_adapter(self):
        """write_feed_generation_session_analytics forwards call to adapter."""
        # Arrange
        adapter = _FakeAdapter()
        adapter.write_feed_generation_session_analytics = Mock()  # type: ignore[method-assign]
        repo = FeedStorageRepository(adapter=adapter)
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

        # Act
        repo.write_feed_generation_session_analytics(
            feed_generation_session_analytics=analytics, timestamp=timestamp
        )

        # Assert
        adapter.write_feed_generation_session_analytics.assert_called_once_with(
            feed_generation_session_analytics=analytics, timestamp=timestamp
        )

    def test_write_session_analytics_propagates_storage_error(self):
        """write_feed_generation_session_analytics propagates StorageError from adapter."""
        # Arrange
        adapter = _FakeAdapter()
        adapter.write_feed_generation_session_analytics = Mock(side_effect=StorageError("down"))  # type: ignore[method-assign]
        repo = FeedStorageRepository(adapter=adapter)
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
            repo.write_feed_generation_session_analytics(
                feed_generation_session_analytics=analytics, timestamp=timestamp
            )
