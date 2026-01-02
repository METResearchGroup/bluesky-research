"""Tests for ScoringService."""

import pandas as pd
import pytest
from unittest.mock import Mock, MagicMock

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.services.scoring import ScoringService


class TestScoringService:
    """Tests for ScoringService."""

    @pytest.fixture
    def mock_scores_repo(self):
        """Create a mock scores repository."""
        repo = Mock()
        repo.load_cached_scores.return_value = []
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
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "text": ["post1", "post2"],
            "source": ["firehose", "firehose"],
            "synctimestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"],
            "like_count": [10, 20],
            "author_did": ["did:test:1", "did:test:2"],
            "sociopolitical_was_successfully_labeled": [False, False],
            "is_sociopolitical": [False, False],
            "perspective_was_successfully_labeled": [False, False],
        })
        cached_scores = [
            PostScoreByAlgorithm(uri="uri1", engagement_score=1.0, treatment_score=0.5),
        ]
        mock_scores_repo.load_cached_scores.return_value = cached_scores

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # Check that uri1 has cached score
        uri1_row = result[result["uri"] == "uri1"]
        assert len(uri1_row) == 1
        assert uri1_row.iloc[0]["engagement_score"] == 1.0
        assert uri1_row.iloc[0]["treatment_score"] == 0.5
        # Check that uri2 was newly scored
        uri2_row = result[result["uri"] == "uri2"]
        assert len(uri2_row) == 1
        assert "engagement_score" in result.columns
        assert "treatment_score" in result.columns
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
            "synctimestamp": ["2024-01-01-00:00:00"],
            "like_count": [10],
            "author_did": ["did:test:1"],
            "sociopolitical_was_successfully_labeled": [False],
            "is_sociopolitical": [False],
            "perspective_was_successfully_labeled": [False],
        })
        mock_scores_repo.load_cached_scores.return_value = []

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "uri1" in result["uri"].values
        assert "engagement_score" in result.columns
        assert "treatment_score" in result.columns
        assert result["engagement_score"].iloc[0] > 0
        assert result["treatment_score"].iloc[0] > 0

    def test_score_posts_skips_export_when_flag_set(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts skips export when export_new_scores=False."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1"],
            "text": ["test"],
            "source": ["firehose"],
            "synctimestamp": ["2024-01-01-00:00:00"],
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
            export_new_scores=False,  # Skip export
        )

        # Assert
        mock_scores_repo.save_scores.assert_not_called()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_score_posts_raises_error_for_empty_dataframe(
        self, scoring_service, mock_scores_repo, feed_config
    ):
        """Test that score_posts raises ValueError for empty DataFrame."""
        # Arrange
        posts_df = pd.DataFrame()

        # Act & Assert
        with pytest.raises(ValueError, match="Empty posts DataFrame"):
            scoring_service.score_posts(
                posts_df=posts_df,
                superposter_dids=set(),
            )

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
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "text": ["post1", "post2", "post3"],
            "source": ["firehose", "firehose", "firehose"],
            "synctimestamp": ["2024-01-01-00:00:00"] * 3,
            "like_count": [10, 20, 30],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "sociopolitical_was_successfully_labeled": [False] * 3,
            "is_sociopolitical": [False] * 3,
            "perspective_was_successfully_labeled": [False] * 3,
        })
        cached_scores = [
            PostScoreByAlgorithm(uri="uri1", engagement_score=1.0, treatment_score=0.5),
            PostScoreByAlgorithm(uri="uri2", engagement_score=2.0, treatment_score=1.0),
        ]
        mock_scores_repo.load_cached_scores.return_value = cached_scores

        # Act
        result = scoring_service.score_posts(
            posts_df=posts_df,
            superposter_dids=set(),
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        # Check cached scores are used
        uri1_row = result[result["uri"] == "uri1"]
        assert uri1_row.iloc[0]["engagement_score"] == 1.0
        uri2_row = result[result["uri"] == "uri2"]
        assert uri2_row.iloc[0]["engagement_score"] == 2.0
        # Check new score was calculated for uri3
        uri3_row = result[result["uri"] == "uri3"]
        assert len(uri3_row) == 1
        assert uri3_row.iloc[0]["engagement_score"] > 0

    def test_add_scores_to_posts_df_handles_empty_scores(
        self, scoring_service, feed_config
    ):
        """Test that _add_scores_to_posts_df handles empty scores list gracefully."""
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1"],
            "text": ["test"],
            "source": ["firehose"],
        })
        empty_scores: list[PostScoreByAlgorithm] = []

        # Act
        result = scoring_service._add_scores_to_posts_df(
            posts_df=posts_df,
            scores=empty_scores,
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "engagement_score" in result.columns
        assert "treatment_score" in result.columns
        # Scores should be NaN/empty since no scores were provided
        assert pd.isna(result["engagement_score"].iloc[0])
        assert pd.isna(result["treatment_score"].iloc[0])

    def test_add_scores_to_posts_df_handles_both_empty(
        self, scoring_service, feed_config
    ):
        """Test that _add_scores_to_posts_df handles both empty posts_df and empty scores."""
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        posts_df = pd.DataFrame({
            "uri": [],
            "text": [],
            "source": [],
        })
        empty_scores: list[PostScoreByAlgorithm] = []

        # Act
        result = scoring_service._add_scores_to_posts_df(
            posts_df=posts_df,
            scores=empty_scores,
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert "engagement_score" in result.columns
        assert "treatment_score" in result.columns

    def test_add_scores_to_posts_df_raises_error_when_posts_empty_but_scores_not(
        self, scoring_service, feed_config
    ):
        """Test that _add_scores_to_posts_df raises ValueError when posts_df is empty but scores is not."""
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        posts_df = pd.DataFrame({
            "uri": [],
            "text": [],
            "source": [],
        })
        scores = [
            PostScoreByAlgorithm(uri="uri1", engagement_score=1.0, treatment_score=0.5),
        ]

        # Act & Assert
        with pytest.raises(ValueError, match="posts_df is empty but scores is not empty"):
            scoring_service._add_scores_to_posts_df(
                posts_df=posts_df,
                scores=scores,
            )

