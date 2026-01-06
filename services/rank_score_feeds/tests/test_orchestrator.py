"""Tests for FeedGenerationOrchestrator.

This test suite verifies the orchestrator structure and functionality.
These tests ensure:
- Orchestrator can be instantiated with FeedConfig
- All dependencies are properly constructed
- Data loading and filtering methods work correctly
- All pipeline steps execute in correct order
- Service delegation works correctly
- Error handling works correctly
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    FeedInputData,
    LatestFeeds,
    RawFeedData,
    LoadedData,
    CandidatePostPools,
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
)
from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator
from services.rank_score_feeds.storage.exceptions import StorageError
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

    @patch("services.rank_score_feeds.orchestrator.get_all_users")
    @patch("services.rank_score_feeds.services.data_loading.DataLoadingService.load_feed_input_data")
    @patch("services.rank_score_feeds.services.data_loading.DataLoadingService.load_latest_feeds")
    def test_load_data_returns_loaded_data(
        self, mock_load_feeds, mock_load_feed_input, mock_get_users
    ):
        """Verify _load_data returns LoadedData with correct structure."""
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
        result = orchestrator._load_data(test_mode=False, users_to_create_feeds_for=None)

        # Assert
        assert isinstance(result, LoadedData)
        assert result.posts_df.equals(mock_feed_input.consolidate_enrichment_integrations)
        assert result.user_to_social_network_map == mock_feed_input.scraped_user_social_network
        assert result.superposter_dids == mock_feed_input.superposters
        assert result.previous_feeds == mock_feeds
        assert result.study_users == mock_users

    def test_load_data_filters_users_when_specified(self):
        """Verify _load_data filters users correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock the raw data loading
        with patch.object(orchestrator, '_load_raw_data') as mock_load_raw:
            mock_raw_data = RawFeedData(
                study_users=[
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
                ],
                feed_input_data=FeedInputData(
                    consolidate_enrichment_integrations=pd.DataFrame(),
                    scraped_user_social_network={},
                    superposters=set(),
                ),
                latest_feeds=LatestFeeds(feeds={}),
            )
            mock_load_raw.return_value = mock_raw_data

            # Act
            result = orchestrator._load_data(
                test_mode=False,
                users_to_create_feeds_for=["user1.bsky.social"]
            )

            # Assert
            assert len(result.study_users) == 1
            assert result.study_users[0].bluesky_handle == "user1.bsky.social"

    def test_score_posts_delegates_to_scoring_service(self):
        """Verify _score_posts delegates to scoring_service correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_posts_df = pd.DataFrame({"uri": ["post1"]})
        mock_superposter_dids = {"did:plc:superposter1"}
        mock_loaded_data = LoadedData(
            posts_df=mock_posts_df,
            user_to_social_network_map={},
            superposter_dids=mock_superposter_dids,
            previous_feeds=LatestFeeds(feeds={}),
            study_users=[],
        )

        expected_result = pd.DataFrame({"uri": ["post1"], "score": [0.5]})

        with patch.object(orchestrator.scoring_service, 'score_posts') as mock_score:
            mock_score.return_value = expected_result

            # Act
            result = orchestrator._score_posts(mock_loaded_data, export_new_scores=True)

            # Assert
            mock_score.assert_called_once_with(
                posts_df=mock_posts_df,
                superposter_dids=mock_superposter_dids,
                export_new_scores=True,
            )
            assert result.equals(expected_result)

    def test_score_posts_passes_export_new_scores_false(self):
        """Verify _score_posts passes export_new_scores=False correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_loaded_data = LoadedData(
            posts_df=pd.DataFrame({"uri": ["post1"]}),
            user_to_social_network_map={},
            superposter_dids=set(),
            previous_feeds=LatestFeeds(feeds={}),
            study_users=[],
        )

        with patch.object(orchestrator.scoring_service, 'score_posts') as mock_score:
            mock_score.return_value = pd.DataFrame()

            # Act
            orchestrator._score_posts(mock_loaded_data, export_new_scores=False)

            # Assert
            mock_score.assert_called_once_with(
                posts_df=mock_loaded_data.posts_df,
                superposter_dids=mock_loaded_data.superposter_dids,
                export_new_scores=False,
            )

    def test_generate_candidate_pools_delegates_to_candidate_service(self):
        """Verify _generate_candidate_pools delegates correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_posts_df = pd.DataFrame({"uri": ["post1"]})
        mock_pools = CandidatePostPools(
            reverse_chronological=pd.DataFrame(),
            engagement=pd.DataFrame(),
            treatment=pd.DataFrame(),
        )

        with patch.object(orchestrator.candidate_service, 'generate_candidate_pools') as mock_gen:
            mock_gen.return_value = mock_pools

            # Act
            result = orchestrator._generate_candidate_pools(mock_posts_df)

            # Assert
            mock_gen.assert_called_once_with(posts_df=mock_posts_df)
            assert result == mock_pools

    def test_generate_feeds_calls_services_correctly(self):
        """Verify _generate_feeds calls context and feed services correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_loaded_data = LoadedData(
            posts_df=pd.DataFrame({"uri": ["post1"]}),
            user_to_social_network_map={"did:plc:user1": ["did:plc:user2"]},
            superposter_dids=set(),
            previous_feeds=LatestFeeds(feeds={}),
            study_users=[
                UserToBlueskyProfileModel(
                    study_user_id="1",
                    condition="engagement",
                    bluesky_handle="user1.bsky.social",
                    bluesky_user_did="did:plc:user1",
                    is_study_user=True,
                    created_timestamp="2024-01-01T00:00:00Z",
                )
            ],
        )

        mock_candidate_pools = CandidatePostPools(
            reverse_chronological=pd.DataFrame(),
            engagement=pd.DataFrame(),
            treatment=pd.DataFrame(),
        )

        mock_in_network_map = {"did:plc:user1": ["post1"]}
        mock_feeds = {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}

        with patch.object(orchestrator.context_service, 'build_in_network_context') as mock_context, \
             patch.object(orchestrator.feed_service, 'generate_feeds_for_users') as mock_feed_gen:
            mock_context.return_value = mock_in_network_map
            mock_feed_gen.return_value = mock_feeds

            # Act
            result = orchestrator._generate_feeds(mock_loaded_data, mock_candidate_pools)

            # Assert
            mock_context.assert_called_once_with(
                scored_posts=mock_loaded_data.posts_df,
                study_users=mock_loaded_data.study_users,
                user_to_social_network_map=mock_loaded_data.user_to_social_network_map,
            )
            mock_feed_gen.assert_called_once_with(
                user_to_in_network_post_uris_map=mock_in_network_map,
                candidate_post_pools=mock_candidate_pools,
                study_users=mock_loaded_data.study_users,
                previous_feeds=mock_loaded_data.previous_feeds,
            )
            assert result == mock_feeds

    def test_calculate_feed_generation_session_analytics_delegates_correctly(self):
        """Verify analytics calculation delegates correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)
        orchestrator.current_datetime_str = "2024-01-01T00:00:00Z"

        mock_feeds = {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}
        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(
            orchestrator.feed_generation_session_analytics_service,
            'calculate_feed_generation_session_analytics'
        ) as mock_calc:
            mock_calc.return_value = mock_analytics

            # Act
            result = orchestrator._calculate_feed_generation_session_analytics(mock_feeds)

            # Assert
            mock_calc.assert_called_once_with(
                user_to_ranked_feed_map=mock_feeds,
                session_timestamp=orchestrator.current_datetime_str,
            )
            assert result == mock_analytics

    def test_export_artifacts_calls_both_export_methods(self):
        """Verify _export_artifacts calls both export methods."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)
        orchestrator.current_datetime_str = "2024-01-01T00:00:00Z"

        mock_feeds = {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}
        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(orchestrator.data_exporter_service, 'export_feeds') as mock_export_feeds, \
             patch.object(
                 orchestrator.data_exporter_service,
                 'export_feed_generation_session_analytics'
             ) as mock_export_analytics:
            # Act
            orchestrator._export_artifacts(mock_feeds, mock_analytics)

            # Assert
            mock_export_feeds.assert_called_once_with(
                user_to_ranked_feed_map=mock_feeds,
                timestamp=orchestrator.current_datetime_str,
            )
            mock_export_analytics.assert_called_once_with(
                feed_generation_session_analytics=mock_analytics,
                timestamp=orchestrator.current_datetime_str,
            )

    def test_ttl_old_feeds_success(self):
        """Verify _ttl_old_feeds calls adapter correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        with patch.object(orchestrator.feed_ttl_adapter, 'move_to_cache') as mock_move:
            # Act
            orchestrator._ttl_old_feeds()

            # Assert
            mock_move.assert_called_once_with(
                prefix="custom_feeds",
                keep_count=config.keep_count,
                sort_field="Key",
            )

    def test_ttl_old_feeds_raises_storage_error_on_failure(self):
        """Verify _ttl_old_feeds raises StorageError on failure."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        with patch.object(orchestrator.feed_ttl_adapter, 'move_to_cache') as mock_move:
            mock_move.side_effect = Exception("S3 error")

            # Act & Assert
            with pytest.raises(StorageError, match="Failed to TTL old feeds"):
                orchestrator._ttl_old_feeds()

    def test_insert_feed_generation_session_metadata_success(self):
        """Verify _insert_feed_generation_session_metadata calls adapter correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(
            orchestrator.session_metadata_adapter,
            'insert_session_metadata'
        ) as mock_insert:
            # Act
            orchestrator._insert_feed_generation_session_metadata(mock_analytics)

            # Assert
            mock_insert.assert_called_once_with(metadata=mock_analytics)

    def test_insert_feed_generation_session_metadata_raises_storage_error_on_failure(self):
        """Verify metadata insertion raises StorageError on failure."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(
            orchestrator.session_metadata_adapter,
            'insert_session_metadata'
        ) as mock_insert:
            mock_insert.side_effect = Exception("DynamoDB error")

            # Act & Assert
            with pytest.raises(
                StorageError,
                match="Failed to insert feed generation session metadata"
            ):
                orchestrator._insert_feed_generation_session_metadata(mock_analytics)

    @patch("services.rank_score_feeds.orchestrator.load_users_to_exclude")
    @patch("services.rank_score_feeds.orchestrator.get_all_users")
    def test_run_completes_full_pipeline_in_test_mode(
        self, mock_get_users, mock_load_exclude
    ):
        """Verify run() executes full pipeline correctly in test_mode=True."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Setup all mocks
        mock_get_users.return_value = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            )
        ]
        mock_load_exclude.return_value = {
            "bsky_handles_to_exclude": set(),
            "bsky_dids_to_exclude": set(),
        }

        # Mock all service calls
        with patch.object(orchestrator.data_loading_service, 'load_feed_input_data') as mock_load_input, \
             patch.object(orchestrator.data_loading_service, 'load_latest_feeds') as mock_load_feeds, \
             patch.object(orchestrator.scoring_service, 'score_posts') as mock_score, \
             patch.object(orchestrator.candidate_service, 'generate_candidate_pools') as mock_candidate, \
             patch.object(orchestrator.context_service, 'build_in_network_context') as mock_context, \
             patch.object(orchestrator.feed_service, 'generate_feeds_for_users') as mock_feed_gen, \
             patch.object(
                 orchestrator.feed_generation_session_analytics_service,
                 'calculate_feed_generation_session_analytics'
             ) as mock_analytics, \
             patch.object(orchestrator.data_exporter_service, 'export_feeds') as mock_export_feeds, \
             patch.object(
                 orchestrator.data_exporter_service,
                 'export_feed_generation_session_analytics'
             ) as mock_export_analytics, \
             patch.object(orchestrator, '_ttl_old_feeds') as mock_ttl, \
             patch.object(
                 orchestrator,
                 '_insert_feed_generation_session_metadata'
             ) as mock_insert:

            # Setup return values
            mock_load_input.return_value = FeedInputData(
                consolidate_enrichment_integrations=pd.DataFrame({
                    "uri": ["post1"],
                    "consolidation_timestamp": ["2024-01-01T00:00:00Z"],
                    "author_did": ["did:plc:author1"],
                    "author_handle": ["author1.bsky.social"],
                }),
                scraped_user_social_network={},
                superposters=set(),
            )
            mock_load_feeds.return_value = LatestFeeds(feeds={})
            mock_score.return_value = pd.DataFrame({"uri": ["post1"], "score": [0.5]})
            mock_candidate.return_value = CandidatePostPools(
                reverse_chronological=pd.DataFrame(),
                engagement=pd.DataFrame(),
                treatment=pd.DataFrame(),
            )
            mock_context.return_value = {"did:plc:user1": ["post1"]}
            mock_feeds_dict = {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}
            mock_feed_gen.return_value = mock_feeds_dict
            mock_analytics.return_value = FeedGenerationSessionAnalytics(
                total_feeds=1,
                total_posts=1,
                total_in_network_posts=1,
                total_in_network_posts_prop=1.0,
                total_unique_engagement_uris=1,
                total_unique_treatment_uris=0,
                prop_overlap_treatment_uris_in_engagement_uris=0.0,
                prop_overlap_engagement_uris_in_treatment_uris=0.0,
                total_feeds_per_condition={"engagement": 1},
                session_timestamp="2024-01-01T00:00:00Z",
            )

            # Act
            orchestrator.run(test_mode=True, export_new_scores=True)

            # Assert - verify all steps were called
            mock_load_input.assert_called_once()
            mock_load_feeds.assert_called_once()
            mock_score.assert_called_once()
            mock_candidate.assert_called_once()
            mock_context.assert_called_once()
            mock_feed_gen.assert_called_once()
            mock_analytics.assert_called_once()
            mock_export_feeds.assert_called_once()
            mock_export_analytics.assert_called_once()
            # TTL and metadata should NOT be called in test_mode
            mock_ttl.assert_not_called()
            mock_insert.assert_not_called()

    def test_run_skips_ttl_and_metadata_in_test_mode(self):
        """Verify run() skips TTL and metadata insertion in test_mode=True."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock everything needed for the pipeline
        with patch.object(orchestrator, '_load_data') as mock_load, \
             patch.object(orchestrator, '_deduplicate_and_filter_posts') as mock_dedup, \
             patch.object(orchestrator, '_score_posts') as mock_score, \
             patch.object(orchestrator, '_generate_candidate_pools') as mock_candidate, \
             patch.object(orchestrator, '_generate_feeds') as mock_gen_feeds, \
             patch.object(
                 orchestrator,
                 '_calculate_feed_generation_session_analytics'
             ) as mock_analytics, \
             patch.object(orchestrator, '_export_artifacts') as mock_export, \
             patch.object(orchestrator, '_ttl_old_feeds') as mock_ttl, \
             patch.object(
                 orchestrator,
                 '_insert_feed_generation_session_metadata'
             ) as mock_insert:

            mock_load.return_value = LoadedData(
                posts_df=pd.DataFrame(),
                user_to_social_network_map={},
                superposter_dids=set(),
                previous_feeds=LatestFeeds(feeds={}),
                study_users=[],
            )
            mock_dedup.return_value = pd.DataFrame()
            mock_score.return_value = pd.DataFrame()
            mock_candidate.return_value = CandidatePostPools(
                reverse_chronological=pd.DataFrame(),
                engagement=pd.DataFrame(),
                treatment=pd.DataFrame(),
            )
            mock_gen_feeds.return_value = {}
            mock_analytics.return_value = FeedGenerationSessionAnalytics(
                total_feeds=0,
                total_posts=0,
                total_in_network_posts=0,
                total_in_network_posts_prop=0.0,
                total_unique_engagement_uris=0,
                total_unique_treatment_uris=0,
                prop_overlap_treatment_uris_in_engagement_uris=0.0,
                prop_overlap_engagement_uris_in_treatment_uris=0.0,
                total_feeds_per_condition={},
                session_timestamp="2024-01-01T00:00:00Z",
            )

            # Act
            orchestrator.run(test_mode=True)

            # Assert
            mock_ttl.assert_not_called()
            mock_insert.assert_not_called()

    def test_run_calls_ttl_and_metadata_in_production_mode(self):
        """Verify run() calls TTL and metadata insertion when test_mode=False."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock everything needed for the pipeline
        with patch.object(orchestrator, '_load_data') as mock_load, \
             patch.object(orchestrator, '_deduplicate_and_filter_posts') as mock_dedup, \
             patch.object(orchestrator, '_score_posts') as mock_score, \
             patch.object(orchestrator, '_generate_candidate_pools') as mock_candidate, \
             patch.object(orchestrator, '_generate_feeds') as mock_gen_feeds, \
             patch.object(
                 orchestrator,
                 '_calculate_feed_generation_session_analytics'
             ) as mock_analytics, \
             patch.object(orchestrator, '_export_artifacts') as mock_export, \
             patch.object(orchestrator, '_ttl_old_feeds') as mock_ttl, \
             patch.object(
                 orchestrator,
                 '_insert_feed_generation_session_metadata'
             ) as mock_insert:

            mock_load.return_value = LoadedData(
                posts_df=pd.DataFrame(),
                user_to_social_network_map={},
                superposter_dids=set(),
                previous_feeds=LatestFeeds(feeds={}),
                study_users=[],
            )
            mock_dedup.return_value = pd.DataFrame()
            mock_score.return_value = pd.DataFrame()
            mock_candidate.return_value = CandidatePostPools(
                reverse_chronological=pd.DataFrame(),
                engagement=pd.DataFrame(),
                treatment=pd.DataFrame(),
            )
            mock_gen_feeds.return_value = {}
            mock_analytics.return_value = FeedGenerationSessionAnalytics(
                total_feeds=0,
                total_posts=0,
                total_in_network_posts=0,
                total_in_network_posts_prop=0.0,
                total_unique_engagement_uris=0,
                total_unique_treatment_uris=0,
                prop_overlap_treatment_uris_in_engagement_uris=0.0,
                prop_overlap_engagement_uris_in_treatment_uris=0.0,
                total_feeds_per_condition={},
                session_timestamp="2024-01-01T00:00:00Z",
            )

            # Act
            orchestrator.run(test_mode=False)

            # Assert
            mock_ttl.assert_called_once()
            mock_insert.assert_called_once()
