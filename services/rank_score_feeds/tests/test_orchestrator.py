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
from services.rank_score_feeds.models import FeedInputData, LatestFeeds, RawFeedData
from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator
from services.participant_data.models import UserToBlueskyProfileModel


class TestFeedGenerationOrchestrator:
    """Tests for FeedGenerationOrchestrator class."""

    @patch("services.rank_score_feeds.orchestrator.get_all_users")
    @patch("services.rank_score_feeds.services.data_loading.DataLoadingService.load_feed_input_data")
    @patch("services.rank_score_feeds.services.data_loading.DataLoadingService.load_latest_feeds")
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

        mock_feed_input = FeedInputData(
            consolidate_enrichment_integrations=pd.DataFrame({"uri": ["post1"]}),
            scraped_user_social_network={"did:plc:user1": ["did:plc:user2"]},
            superposters={"did:plc:superposter1"},
        )

        mock_feeds = LatestFeeds(feeds={"user1.bsky.social": {"post1", "post2"}})

        mock_get_users.return_value = mock_users
        mock_load_feed_input.return_value = mock_feed_input
        mock_load_feeds.return_value = mock_feeds

        # Act
        result = orchestrator._load_raw_data(test_mode=False)

        # Assert
        assert result is not None
        assert isinstance(result, RawFeedData)
        assert result.study_users == mock_users
        assert isinstance(result.feed_input_data, FeedInputData)
        assert result.feed_input_data.consolidate_enrichment_integrations.equals(
            mock_feed_input.consolidate_enrichment_integrations
        )
        assert result.feed_input_data.scraped_user_social_network == mock_feed_input.scraped_user_social_network
        assert result.feed_input_data.superposters == mock_feed_input.superposters
        assert isinstance(result.latest_feeds, LatestFeeds)
        assert result.latest_feeds.feeds == mock_feeds.feeds

    @patch("services.rank_score_feeds.orchestrator.get_all_users")
    def test_load_raw_data_passes_test_mode_to_get_all_users(self, mock_get_users):
        """Verify _load_raw_data passes test_mode parameter correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_get_users.return_value = []
        with patch("services.rank_score_feeds.services.data_loading.DataLoadingService.load_feed_input_data") as mock_feed_input, \
             patch("services.rank_score_feeds.services.data_loading.DataLoadingService.load_latest_feeds") as mock_feeds:
            mock_feed_input.return_value = FeedInputData(
                consolidate_enrichment_integrations=pd.DataFrame(),
                scraped_user_social_network={},
                superposters=set(),
            )
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

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_deduplicates_by_uri(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts deduplicates posts by URI, keeping most recent."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock exclude list to be empty so we only test deduplication
        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        # Create DataFrame with duplicate URIs but different timestamps
        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post1", "post2", "post3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-02T00:00:00Z",  # More recent, should be kept
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ],
                "author_did": ["did:plc:user1", "did:plc:user1", "did:plc:user2", "did:plc:user3"],
                "author_handle": ["user1.bsky.social", "user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 3  # Should have 3 unique URIs
        assert "post1" in result["uri"].values
        assert "post2" in result["uri"].values
        assert "post3" in result["uri"].values
        # Verify the kept post1 has the more recent timestamp
        kept_post1 = result[result["uri"] == "post1"]
        assert len(kept_post1) == 1
        assert kept_post1.iloc[0]["consolidation_timestamp"] == "2024-01-02T00:00:00Z"

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_filters_by_did(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts filters out posts by author DID."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock exclude list with a DID
        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": {"did:plc:excluded"},
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post2", "post3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ],
                "author_did": ["did:plc:excluded", "did:plc:user1", "did:plc:user2"],
                "author_handle": ["excluded.bsky.social", "user1.bsky.social", "user2.bsky.social"],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 2
        assert "post1" not in result["uri"].values  # Excluded by DID
        assert "post2" in result["uri"].values
        assert "post3" in result["uri"].values

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_filters_by_handle(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts filters out posts by author handle."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock exclude list with a handle
        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": {"excluded.bsky.social"},
            "bsky_dids_to_exclude": set(),
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post2", "post3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ],
                "author_did": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
                "author_handle": ["excluded.bsky.social", "user1.bsky.social", "user2.bsky.social"],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 2
        assert "post1" not in result["uri"].values  # Excluded by handle
        assert "post2" in result["uri"].values
        assert "post3" in result["uri"].values

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_filters_by_both_did_and_handle(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts filters if either DID or handle matches."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock exclude list with both DID and handle
        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": {"excluded_handle.bsky.social"},
            "bsky_dids_to_exclude": {"did:plc:excluded_did"},
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post2", "post3", "post4"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ],
                "author_did": [
                    "did:plc:excluded_did",  # Excluded by DID
                    "did:plc:user1",
                    "did:plc:user2",
                    "did:plc:user3",
                ],
                "author_handle": [
                    "user1.bsky.social",
                    "excluded_handle.bsky.social",  # Excluded by handle
                    "user2.bsky.social",
                    "user3.bsky.social",
                ],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 2
        assert "post1" not in result["uri"].values  # Excluded by DID
        assert "post2" not in result["uri"].values  # Excluded by handle
        assert "post3" in result["uri"].values
        assert "post4" in result["uri"].values

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_handles_empty_dataframe(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts handles empty DataFrame correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        posts_df = pd.DataFrame(
            columns=["uri", "consolidation_timestamp", "author_did", "author_handle"]
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_handles_no_duplicates(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts handles DataFrame with no duplicates."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post2", "post3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-02T00:00:00Z",
                    "2024-01-03T00:00:00Z",
                ],
                "author_did": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
                "author_handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 3
        assert set(result["uri"].values) == {"post1", "post2", "post3"}

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_handles_no_excluded_users(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts handles empty exclude lists."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post2"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-02T00:00:00Z",
                ],
                "author_did": ["did:plc:user1", "did:plc:user2"],
                "author_handle": ["user1.bsky.social", "user2.bsky.social"],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 2
        assert "post1" in result["uri"].values
        assert "post2" in result["uri"].values

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_keeps_most_recent_on_duplicate(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts keeps the most recent post when duplicates exist."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        # Create posts with same URI but different timestamps
        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post1", "post1"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",  # Oldest
                    "2024-01-03T00:00:00Z",  # Most recent
                    "2024-01-02T00:00:00Z",  # Middle
                ],
                "author_did": ["did:plc:user1", "did:plc:user1", "did:plc:user1"],
                "author_handle": ["user1.bsky.social", "user1.bsky.social", "user1.bsky.social"],
                "text": ["old", "newest", "middle"],  # Add text to verify which one is kept
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 1
        assert result.iloc[0]["uri"] == "post1"
        assert result.iloc[0]["consolidation_timestamp"] == "2024-01-03T00:00:00Z"
        assert result.iloc[0]["text"] == "newest"  # Verify the most recent is kept

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_filters_all_posts(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts returns empty DataFrame when all posts are excluded."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock exclude list that matches all posts
        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": {"user1.bsky.social", "user2.bsky.social"},
            "bsky_dids_to_exclude": set(),
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1", "post2"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00Z",
                    "2024-01-02T00:00:00Z",
                ],
                "author_did": ["did:plc:user1", "did:plc:user2"],
                "author_handle": ["user1.bsky.social", "user2.bsky.social"],
            }
        )

        # Act
        result = orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    def test_deduplicate_and_filter_posts_calls_load_users_to_exclude(
        self, mock_load_users_to_exclude
    ):
        """Test that _deduplicate_and_filter_posts calls load_users_to_exclude."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_load_users_to_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        posts_df = pd.DataFrame(
            {
                "uri": ["post1"],
                "consolidation_timestamp": ["2024-01-01T00:00:00Z"],
                "author_did": ["did:plc:user1"],
                "author_handle": ["user1.bsky.social"],
            }
        )

        # Act
        orchestrator._deduplicate_and_filter_posts(posts_df)

        # Assert
        mock_load_users_to_exclude.assert_called_once()
