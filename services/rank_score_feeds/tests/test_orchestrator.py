"""Tests for FeedGenerationOrchestrator.

This test suite verifies the orchestrator structure and basic functionality.
These tests ensure:
- Orchestrator can be instantiated with FeedConfig
- All dependencies are properly constructed
- Basic structure is correct
- Data loading and filtering methods work correctly
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import LatestFeeds
from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator
from services.participant_data.models import UserToBlueskyProfileModel


class TestFeedGenerationOrchestrator:
    """Tests for FeedGenerationOrchestrator class."""

    def test_orchestrator_can_be_instantiated(self):
        """Verify orchestrator can be instantiated with FeedConfig."""
        # Arrange
        config = FeedConfig()

        # Act
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Assert
        assert orchestrator is not None
        assert isinstance(orchestrator, FeedGenerationOrchestrator)
        assert orchestrator.config == config

    def test_orchestrator_has_required_dependencies(self):
        """Verify orchestrator constructs all required dependencies."""
        # Arrange
        config = FeedConfig()

        # Act
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Assert
        assert orchestrator.config is not None
        assert orchestrator.athena is not None
        assert orchestrator.s3 is not None
        assert orchestrator.dynamodb is not None
        assert orchestrator.glue is not None
        assert orchestrator.logger is not None

    def test_orchestrator_config_is_injected(self):
        """Verify config is properly injected into orchestrator."""
        # Arrange
        config = FeedConfig()
        config.max_feed_length = 50  # Modify to verify it's the same instance

        # Act
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Assert
        assert orchestrator.config.max_feed_length == 50
        assert orchestrator.config is config

    def test_orchestrator_has_run_method(self):
        """Verify orchestrator has the run method."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Act & Assert
        assert hasattr(orchestrator, "run")
        assert callable(orchestrator.run)

    def test_orchestrator_has_private_helper_methods(self):
        """Verify orchestrator has expected private helper methods."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Act & Assert
        expected_methods = [
            "_load_data",
            "_load_raw_data",
            "_filter_study_users",
            "_score_posts",
            "_export_scores",
            "_build_post_pools",
            "_generate_feeds",
            "_export_results",
        ]

        for method_name in expected_methods:
            assert hasattr(orchestrator, method_name), (
                f"Missing expected method: {method_name}"
            )
            assert callable(getattr(orchestrator, method_name)), (
                f"Method {method_name} is not callable"
            )

    @patch("services.rank_score_feeds.orchestrator.get_all_users")
    @patch("services.rank_score_feeds.orchestrator.load_feed_input_data")
    @patch("services.rank_score_feeds.orchestrator.load_latest_feeds")
    def test_load_raw_data_returns_correct_structure(
        self, mock_load_feeds, mock_load_feed_input, mock_get_users
    ):
        """Verify _load_raw_data returns RawFeedData with correct structure."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            )
        ]

        mock_feed_input = {
            "consolidate_enrichment_integrations": pd.DataFrame({"uri": ["post1"]}),
            "scraped_user_social_network": {"did:plc:user1": ["did:plc:user2"]},
            "superposters": {"did:plc:superposter1"},
        }

        mock_feeds = LatestFeeds(feeds={"user1.bsky.social": {"post1", "post2"}})

        mock_get_users.return_value = mock_users
        mock_load_feed_input.return_value = mock_feed_input
        mock_load_feeds.return_value = mock_feeds

        # Act
        result = orchestrator._load_raw_data(test_mode=False)

        # Assert
        assert result is not None
        assert "study_users" in result
        assert "feed_input_data" in result
        assert "latest_feeds" in result
        assert result["study_users"] == mock_users
        assert result["feed_input_data"] == mock_feed_input
        assert isinstance(result["latest_feeds"], LatestFeeds)
        assert result["latest_feeds"].feeds == mock_feeds.feeds

    @patch("services.rank_score_feeds.orchestrator.get_all_users")
    def test_load_raw_data_passes_test_mode_to_get_all_users(self, mock_get_users):
        """Verify _load_raw_data passes test_mode parameter correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_get_users.return_value = []
        with patch("services.rank_score_feeds.orchestrator.load_feed_input_data") as mock_feed_input, \
             patch("services.rank_score_feeds.orchestrator.load_latest_feeds") as mock_feeds:
            mock_feed_input.return_value = {
                "consolidate_enrichment_integrations": pd.DataFrame(),
                "scraped_user_social_network": {},
                "superposters": set(),
            }
            mock_feeds.return_value = LatestFeeds(feeds={})

            # Act
            orchestrator._load_raw_data(test_mode=True)

            # Assert
            mock_get_users.assert_called_once_with(test_mode=True)

    def test_filter_study_users_returns_all_when_none_specified(self):
        """Verify _filter_study_users returns all users when None is provided."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            ),
            UserToBlueskyProfileModel(
                study_user_id="2",
                condition="reverse_chronological",
                bluesky_handle="user2.bsky.social",
                bluesky_user_did="did:plc:user2",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            ),
        ]

        # Act
        result = orchestrator._filter_study_users(mock_users, None)

        # Assert
        assert result == mock_users
        assert len(result) == 2

    def test_filter_study_users_filters_by_handle(self):
        """Verify _filter_study_users filters users by handle correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            ),
            UserToBlueskyProfileModel(
                study_user_id="2",
                condition="reverse_chronological",
                bluesky_handle="user2.bsky.social",
                bluesky_user_did="did:plc:user2",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            ),
            UserToBlueskyProfileModel(
                study_user_id="3",
                condition="engagement",
                bluesky_handle="user3.bsky.social",
                bluesky_user_did="did:plc:user3",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            ),
        ]

        # Act
        result = orchestrator._filter_study_users(
            mock_users, ["user1.bsky.social", "user3.bsky.social"]
        )

        # Assert
        assert len(result) == 2
        assert result[0].bluesky_handle == "user1.bsky.social"
        assert result[1].bluesky_handle == "user3.bsky.social"

    def test_filter_study_users_returns_empty_when_no_matches(self):
        """Verify _filter_study_users returns empty list when no handles match."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            ),
        ]

        # Act
        result = orchestrator._filter_study_users(mock_users, ["nonexistent.bsky.social"])

        # Assert
        assert len(result) == 0
