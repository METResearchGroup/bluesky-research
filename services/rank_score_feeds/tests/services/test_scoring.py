"""Tests for ScoringService."""

import pandas as pd
import pytest
from unittest.mock import Mock, MagicMock

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import ScoredPosts
from services.rank_score_feeds.services.scoring import ScoringService


class TestScoringService:
    """Tests for ScoringService."""

    @pytest.fixture
    def mock_scores_repo(self):
        """Create a mock scores repository."""
        repo = Mock()
        repo.load_cached_scores.return_value = {}
        repo.save_scores.return_value = None
        return repo

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    @pytest.fixture
    def scoring_service(self, mock_scores_repo, feed_config):
        """Create a ScoringService instance."""
        return ScoringService(
            scores_repo=mock_scores_repo,
            feed_config=feed_config,
        )

    def test_score_posts_uses_cached_scores_when_available(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts uses cached scores when available."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "text": ["post1", "post2"],
            "source": ["firehose", "firehose"],
            "synctimestamp": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
            "like_count": [10, 20],
            "author_did": ["did:test:1", "did:test:2"],
            "sociopolitical_was_successfully_labeled": [False, False],
            "is_sociopolitical": [False, False],
            "perspective_was_successfully_labeled": [False, False],
        })
        cached_scores = {
            "uri1": {"engagement_score": 1.0, "treatment_score": 0.5},
        }
        mock_scores_repo.load_cached_scores.return_value = cached_scores

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert len(result.new_post_uris) == 1  # Only uri2 is new
        assert "uri1" not in result.new_post_uris
        assert "uri2" in result.new_post_uris
        assert result.posts_df["engagement_score"].iloc[0] == 1.0  # Cached value
        assert "engagement_score" in result.posts_df.columns
        assert "treatment_score" in result.posts_df.columns
        mock_scores_repo.save_scores.assert_called_once()

    def test_score_posts_calculates_new_scores_for_uncached_posts(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts calculates new scores for uncached posts."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1"],
            "text": ["test"],
            "source": ["firehose"],
            "synctimestamp": ["2024-01-01T00:00:00Z"],
            "like_count": [10],
            "author_did": ["did:test:1"],
            "sociopolitical_was_successfully_labeled": [False],
            "is_sociopolitical": [False],
            "perspective_was_successfully_labeled": [False],
        })
        mock_scores_repo.load_cached_scores.return_value = {}

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert len(result.new_post_uris) == 1
        assert "uri1" in result.new_post_uris
        assert "engagement_score" in result.posts_df.columns
        assert "treatment_score" in result.posts_df.columns
        assert result.posts_df["engagement_score"].iloc[0] > 0
        assert result.posts_df["treatment_score"].iloc[0] > 0

    def test_score_posts_skips_export_when_flag_set(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts skips export when save_new_scores=False."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1"],
            "text": ["test"],
            "source": ["firehose"],
            "synctimestamp": ["2024-01-01T00:00:00Z"],
            "like_count": [10],
            "author_did": ["did:test:1"],
            "sociopolitical_was_successfully_labeled": [False],
            "is_sociopolitical": [False],
            "perspective_was_successfully_labeled": [False],
        })

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
            save_new_scores=False,  # Skip export
        )

        # Assert
        mock_scores_repo.save_scores.assert_not_called()
        assert len(result.new_post_uris) == 1

    def test_score_posts_returns_empty_scored_posts_for_empty_dataframe(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts handles empty DataFrame gracefully."""
        # Arrange
        posts_df = pd.DataFrame()

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert isinstance(result, ScoredPosts)
        assert len(result.posts_df) == 0
        assert len(result.new_post_uris) == 0
        mock_scores_repo.save_scores.assert_not_called()

    def test_score_posts_raises_error_when_uri_column_missing(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts raises ValueError when uri column is missing."""
        # Arrange
        posts_df = pd.DataFrame({
            "text": ["test"],
            "source": ["firehose"],
        })

        # Act & Assert
        with pytest.raises(ValueError, match="must contain 'uri' column"):
            scoring_service.score_posts(
                posts_df=posts_df,
                superposter_dids=set(),
            )

    def test_score_posts_mixes_cached_and_new_scores(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts correctly mixes cached and newly calculated scores."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "text": ["post1", "post2", "post3"],
            "source": ["firehose", "firehose", "firehose"],
            "synctimestamp": ["2024-01-01T00:00:00Z"] * 3,
            "like_count": [10, 20, 30],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "sociopolitical_was_successfully_labeled": [False] * 3,
            "is_sociopolitical": [False] * 3,
            "perspective_was_successfully_labeled": [False] * 3,
        })
        cached_scores = {
            "uri1": {"engagement_score": 1.0, "treatment_score": 0.5},
            "uri2": {"engagement_score": 2.0, "treatment_score": 1.0},
        }
        mock_scores_repo.load_cached_scores.return_value = cached_scores

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert len(result.new_post_uris) == 1
        assert "uri3" in result.new_post_uris
        # Check cached scores are used
        assert result.posts_df[result.posts_df["uri"] == "uri1"]["engagement_score"].iloc[0] == 1.0
        assert result.posts_df[result.posts_df["uri"] == "uri2"]["engagement_score"].iloc[0] == 2.0
        # Check new score was calculated
        assert "uri3" in result.new_post_uris

