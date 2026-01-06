"""Tests for UserContextService."""

from unittest.mock import patch

import pandas as pd
import pytest

from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.services.context import UserContextService


class TestUserContextService:
    """Tests for UserContextService."""

    @pytest.fixture
    def context_service(self):
        """Create a UserContextService instance."""
        return UserContextService()

    @pytest.fixture
    def sample_study_users(self):
        """Create sample study users for testing."""
        return [
            UserToBlueskyProfileModel(
                study_user_id="user1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:test:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
            UserToBlueskyProfileModel(
                study_user_id="user2",
                condition="reverse_chronological",
                bluesky_handle="user2.bsky.social",
                bluesky_user_did="did:test:user2",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
        ]

    def test_init_creates_service(self, context_service):
        """Test that __init__ creates a service instance."""
        # Arrange & Act
        service = UserContextService()

        # Assert
        assert isinstance(service, UserContextService)

    def test_build_in_network_context_returns_dict(self, context_service, sample_study_users):
        """Test that build_in_network_context returns a dictionary."""
        # Arrange
        scored_posts = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "author_did": ["did:test:author1", "did:test:author2"],
            "source": ["firehose", "firehose"],
        })
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1"],
            "did:test:user2": ["did:test:author2"],
        }

        # Act
        result = context_service.build_in_network_context(
            scored_posts=scored_posts,
            study_users=sample_study_users,
            user_to_social_network_map=user_to_social_network_map,
        )

        # Assert
        assert isinstance(result, dict)
        assert "did:test:user1" in result
        assert "did:test:user2" in result
        assert isinstance(result["did:test:user1"], list)
        assert isinstance(result["did:test:user2"], list)

    def test_build_in_network_context_filters_firehose_only(self, context_service, sample_study_users):
        """Test that build_in_network_context only uses firehose source posts."""
        # Arrange
        scored_posts = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:author1", "did:test:author2", "did:test:author3"],
            "source": ["firehose", "most_liked", "firehose"],
        })
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1", "did:test:author2", "did:test:author3"],
        }

        # Act
        result = context_service.build_in_network_context(
            scored_posts=scored_posts,
            study_users=[sample_study_users[0]],
            user_to_social_network_map=user_to_social_network_map,
        )

        # Assert
        # Should only include posts from firehose source
        all_uris = result["did:test:user1"]
        assert "uri1" in all_uris
        assert "uri3" in all_uris
        assert "uri2" not in all_uris  # most_liked source should be excluded

    def test_build_in_network_context_matches_author_dids(self, context_service, sample_study_users):
        """Test that build_in_network_context matches posts by author_did in social network."""
        # Arrange
        scored_posts = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:author1", "did:test:author2", "did:test:author3"],
            "source": ["firehose", "firehose", "firehose"],
        })
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1", "did:test:author2"],
            # user2 has no social network
        }

        # Act
        result = context_service.build_in_network_context(
            scored_posts=scored_posts,
            study_users=sample_study_users,
            user_to_social_network_map=user_to_social_network_map,
        )

        # Assert
        # user1 should have posts from author1 and author2
        assert len(result["did:test:user1"]) == 2
        assert "uri1" in result["did:test:user1"]
        assert "uri2" in result["did:test:user1"]
        assert "uri3" not in result["did:test:user1"]
        # user2 should have no posts (empty social network)
        assert len(result["did:test:user2"]) == 0

    def test_build_in_network_context_handles_empty_dataframe(self, context_service, sample_study_users):
        """Test that build_in_network_context handles empty DataFrame gracefully."""
        # Arrange
        scored_posts = pd.DataFrame({
            "uri": [],
            "author_did": [],
            "source": [],
        })
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1"],
        }

        # Act
        result = context_service.build_in_network_context(
            scored_posts=scored_posts,
            study_users=[sample_study_users[0]],
            user_to_social_network_map=user_to_social_network_map,
        )

        # Assert
        assert isinstance(result, dict)
        assert "did:test:user1" in result
        assert len(result["did:test:user1"]) == 0

    def test_build_in_network_context_handles_empty_study_users(self, context_service):
        """Test that build_in_network_context handles empty study_users list."""
        # Arrange
        scored_posts = pd.DataFrame({
            "uri": ["uri1"],
            "author_did": ["did:test:author1"],
            "source": ["firehose"],
        })
        user_to_social_network_map = {}

        # Act
        result = context_service.build_in_network_context(
            scored_posts=scored_posts,
            study_users=[],
            user_to_social_network_map=user_to_social_network_map,
        )

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_build_in_network_context_handles_missing_user_in_social_network_map(self, context_service, sample_study_users):
        """Test that build_in_network_context handles users not in social network map."""
        # Arrange
        scored_posts = pd.DataFrame({
            "uri": ["uri1"],
            "author_did": ["did:test:author1"],
            "source": ["firehose"],
        })
        user_to_social_network_map = {
            # user1 is missing from map
            "did:test:user2": ["did:test:author1"],
        }

        # Act
        result = context_service.build_in_network_context(
            scored_posts=scored_posts,
            study_users=sample_study_users,
            user_to_social_network_map=user_to_social_network_map,
        )

        # Assert
        # user1 should have empty list (not in map)
        assert "did:test:user1" in result
        assert len(result["did:test:user1"]) == 0
        # user2 should have the post
        assert "did:test:user2" in result
        assert len(result["did:test:user2"]) == 1
        assert "uri1" in result["did:test:user2"]

    def test_curate_baseline_in_network_posts_filters_firehose(self, context_service):
        """Test that _curate_baseline_in_network_posts filters to firehose source only."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:author1", "did:test:author2", "did:test:author3"],
            "source": ["firehose", "most_liked", "firehose"],
        })

        # Act
        result = context_service._curate_baseline_in_network_posts(posts_df=posts_df)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert all(result["source"] == "firehose")
        assert "uri1" in result["uri"].values
        assert "uri3" in result["uri"].values
        assert "uri2" not in result["uri"].values

    def test_curate_baseline_in_network_posts_handles_empty_dataframe(self, context_service):
        """Test that _curate_baseline_in_network_posts handles empty DataFrame and logs warning."""
        # Arrange
        with patch.object(context_service, "logger") as mock_logger:
            posts_df = pd.DataFrame({
                "uri": [],
                "author_did": [],
                "source": [],
            })

            # Act
            result = context_service._curate_baseline_in_network_posts(posts_df=posts_df)

            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            # Should log warning when empty DataFrame results in empty curated DataFrame
            mock_logger.warning.assert_called_once_with("Curated baseline in-network posts DataFrame has length 0. This should not happen.")

    def test_curate_baseline_in_network_posts_logs_warning_when_empty(self, context_service):
        """Test that _curate_baseline_in_network_posts logs warning when curated DataFrame is empty."""
        # Arrange
        with patch.object(context_service, "logger") as mock_logger:
            posts_df = pd.DataFrame({
                "uri": ["uri1", "uri2"],
                "author_did": ["did:test:author1", "did:test:author2"],
                "source": ["most_liked", "most_liked"],  # No firehose posts
            })

            # Act
            result = context_service._curate_baseline_in_network_posts(posts_df=posts_df)

            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            mock_logger.warning.assert_called_once_with("Curated baseline in-network posts DataFrame has length 0. This should not happen.")

    def test_curate_baseline_in_network_posts_handles_all_firehose(self, context_service):
        """Test that _curate_baseline_in_network_posts handles all firehose posts and does not log warning."""
        # Arrange
        with patch.object(context_service, "logger") as mock_logger:
            posts_df = pd.DataFrame({
                "uri": ["uri1", "uri2"],
                "author_did": ["did:test:author1", "did:test:author2"],
                "source": ["firehose", "firehose"],
            })

            # Act
            result = context_service._curate_baseline_in_network_posts(posts_df=posts_df)

            # Assert
            assert len(result) == 2
            assert all(result["source"] == "firehose")
            # Should not log warning when curated DataFrame has posts
            mock_logger.warning.assert_not_called()

    def test_curate_baseline_in_network_posts_handles_no_firehose(self, context_service):
        """Test that _curate_baseline_in_network_posts handles no firehose posts."""
        # Arrange
        with patch.object(context_service, "logger") as mock_logger:
            posts_df = pd.DataFrame({
                "uri": ["uri1", "uri2"],
                "author_did": ["did:test:author1", "did:test:author2"],
                "source": ["most_liked", "most_liked"],
            })

            # Act
            result = context_service._curate_baseline_in_network_posts(posts_df=posts_df)

            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            # Should log warning when no firehose posts result in empty curated DataFrame
            mock_logger.warning.assert_called_once_with("Curated baseline in-network posts DataFrame has length 0. This should not happen.")

    def test_calculate_in_network_posts_for_users_returns_map_for_all_users(self, context_service, sample_study_users):
        """Test that _calculate_in_network_posts_for_users returns map for all study users."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "author_did": ["did:test:author1", "did:test:author2"],
        })
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1"],
            "did:test:user2": ["did:test:author2"],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_users(
            curated_baseline_in_network_posts_df=curated_posts_df,
            user_to_social_network_map=user_to_social_network_map,
            study_users=sample_study_users,
        )

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2
        assert "did:test:user1" in result
        assert "did:test:user2" in result
        assert isinstance(result["did:test:user1"], list)
        assert isinstance(result["did:test:user2"], list)

    def test_calculate_in_network_posts_for_users_handles_empty_study_users(self, context_service):
        """Test that _calculate_in_network_posts_for_users handles empty study_users list."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1"],
            "author_did": ["did:test:author1"],
        })
        user_to_social_network_map = {}

        # Act
        result = context_service._calculate_in_network_posts_for_users(
            curated_baseline_in_network_posts_df=curated_posts_df,
            user_to_social_network_map=user_to_social_network_map,
            study_users=[],
        )

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_calculate_in_network_posts_for_user_returns_matching_posts(self, context_service):
        """Test that _calculate_in_network_posts_for_user returns posts from authors in social network."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:author1", "did:test:author2", "did:test:author3"],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1", "did:test:author2"],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 2
        assert "uri1" in result
        assert "uri2" in result
        assert "uri3" not in result

    def test_calculate_in_network_posts_for_user_handles_empty_social_network(self, context_service):
        """Test that _calculate_in_network_posts_for_user handles empty social network."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "author_did": ["did:test:author1", "did:test:author2"],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            "did:test:user1": [],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0

    def test_calculate_in_network_posts_for_user_handles_missing_user_in_map(self, context_service):
        """Test that _calculate_in_network_posts_for_user handles user not in social network map."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "author_did": ["did:test:author1", "did:test:author2"],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            # user1 is missing
            "did:test:user2": ["did:test:author1"],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0

    def test_calculate_in_network_posts_for_user_handles_empty_dataframe(self, context_service):
        """Test that _calculate_in_network_posts_for_user handles empty DataFrame."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": [],
            "author_did": [],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1"],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0

    def test_calculate_in_network_posts_for_user_handles_no_matching_authors(self, context_service):
        """Test that _calculate_in_network_posts_for_user handles no matching authors."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "author_did": ["did:test:author1", "did:test:author2"],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author3"],  # Different author
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0

    def test_calculate_in_network_posts_for_user_handles_multiple_posts_from_same_author(self, context_service):
        """Test that _calculate_in_network_posts_for_user handles multiple posts from same author."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:author1", "did:test:author1", "did:test:author2"],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1"],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 2
        assert "uri1" in result
        assert "uri2" in result
        assert "uri3" not in result

    def test_calculate_in_network_posts_for_user_preserves_uri_order(self, context_service):
        """Test that _calculate_in_network_posts_for_user preserves URI order from DataFrame."""
        # Arrange
        curated_posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:author1", "did:test:author2", "did:test:author1"],
        })
        user_did = "did:test:user1"
        user_to_social_network_map = {
            "did:test:user1": ["did:test:author1", "did:test:author2"],
        }

        # Act
        result = context_service._calculate_in_network_posts_for_user(
            user_did=user_did,
            user_to_social_network_map=user_to_social_network_map,
            curated_baseline_in_network_posts_df=curated_posts_df,
        )

        # Assert
        # Should preserve order from DataFrame
        assert result == ["uri1", "uri2", "uri3"]

