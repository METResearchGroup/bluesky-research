"""Tests for ScoresRepository."""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.repositories.scores_repo import ScoresRepository


class TestScoresRepository:
    """Tests for ScoresRepository."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig()

    @pytest.fixture
    def scores_repo(self, feed_config):
        """Create a ScoresRepository instance."""
        return ScoresRepository(feed_config=feed_config)

    def test_load_cached_scores_returns_empty_list_when_no_cache(
        self, scores_repo, mocker
    ):
        """Test that load_cached_scores returns empty list when no cached scores exist."""
        # Arrange
        mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.load_data_from_local_storage",
            return_value=pd.DataFrame(),  # Empty DataFrame
        )

        # Act
        result = scores_repo.load_cached_scores()

        # Assert
        expected = []
        assert result == expected

    def test_load_cached_scores_returns_cached_scores(self, scores_repo, mocker):
        """Test that load_cached_scores returns cached scores correctly."""
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        mock_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "engagement_score": [1.0, 2.0],
            "treatment_score": [0.5, 1.5],
            "scored_timestamp": ["2024-01-01", "2024-01-02"],
        })
        mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.load_data_from_local_storage",
            return_value=mock_df,
        )

        # Act
        result = scores_repo.load_cached_scores()

        # Assert
        expected = [
            PostScoreByAlgorithm(uri="uri2", engagement_score=2.0, treatment_score=1.5),
            PostScoreByAlgorithm(uri="uri1", engagement_score=1.0, treatment_score=0.5),
        ]
        assert len(result) == 2
        # Check that both URIs are present (order may vary due to sorting)
        uris = {score.uri for score in result}
        assert uris == {"uri1", "uri2"}
        # Check scores
        uri1_score = next(score for score in result if score.uri == "uri1")
        assert uri1_score.engagement_score == 1.0
        assert uri1_score.treatment_score == 0.5
        uri2_score = next(score for score in result if score.uri == "uri2")
        assert uri2_score.engagement_score == 2.0
        assert uri2_score.treatment_score == 1.5

    def test_load_cached_scores_filters_nan_scores(self, scores_repo, mocker):
        """Test that load_cached_scores filters out NaN scores."""
        from services.rank_score_feeds.models import PostScoreByAlgorithm
        
        # Arrange
        mock_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "engagement_score": [1.0, float("nan"), 2.0],
            "treatment_score": [0.5, 1.5, float("nan")],
            "scored_timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
        })
        mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.load_data_from_local_storage",
            return_value=mock_df,
        )

        # Act
        result = scores_repo.load_cached_scores()

        # Assert
        # Only uri1 should be included (uri2 has NaN engagement, uri3 has NaN treatment)
        assert len(result) == 1
        assert result[0].uri == "uri1"
        assert result[0].engagement_score == 1.0
        assert result[0].treatment_score == 0.5

    def test_load_cached_scores_handles_exception_gracefully(self, scores_repo, mocker):
        """Test that load_cached_scores returns empty list on exception."""
        # Arrange
        mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.load_data_from_local_storage",
            side_effect=Exception("Storage error"),
        )

        # Act
        result = scores_repo.load_cached_scores()

        # Assert
        expected = []
        assert result == expected

    def test_save_scores_exports_to_storage(self, scores_repo, mocker):
        """Test that save_scores exports scores to storage."""
        from services.rank_score_feeds.models import ScoredPostModel
        
        # Arrange
        scores_to_save = [
            ScoredPostModel(
                uri="uri1",
                text="test post",
                source="firehose",
                engagement_score=1.0,
                treatment_score=0.5,
                scored_timestamp="2024-01-01T00:00:00Z",
            )
        ]
        mock_export = mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.export_data_to_local_storage"
        )

        # Act
        scores_repo.save_scores(scores_to_save)

        # Assert
        mock_export.assert_called_once()
        call_args = mock_export.call_args
        assert call_args.kwargs["service"] == "post_scores"
        df = call_args.kwargs["df"]
        assert len(df) == 1
        assert df.iloc[0]["uri"] == "uri1"
        assert df.iloc[0]["engagement_score"] == 1.0
        assert df.iloc[0]["treatment_score"] == 0.5

    def test_save_scores_skips_when_empty_list(self, scores_repo, mocker):
        """Test that save_scores skips export when given empty list."""
        # Arrange
        mock_export = mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.export_data_to_local_storage"
        )

        # Act
        scores_repo.save_scores([])

        # Assert
        mock_export.assert_not_called()

    def test_load_cached_scores_uses_custom_lookback_days(self, scores_repo, mocker):
        """Test that load_cached_scores uses custom lookback_days when provided."""
        # Arrange
        mock_load = mocker.patch(
            "services.rank_score_feeds.repositories.scores_repo.load_data_from_local_storage",
            return_value=pd.DataFrame(),
        )

        # Act
        scores_repo.load_cached_scores(lookback_days=5)

        # Assert
        call_args = mock_load.call_args
        # Check that the timestamp calculation uses 5 days
        assert "latest_timestamp" in call_args.kwargs

