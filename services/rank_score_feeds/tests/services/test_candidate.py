"""Tests for CandidateGenerationService."""

from unittest.mock import patch

import pandas as pd
import pytest

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CandidatePostPools
from services.rank_score_feeds.services.candidate import CandidateGenerationService


class TestCandidateGenerationService:
    """Tests for CandidateGenerationService."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig(max_num_times_user_can_appear_in_feed=3)

    @pytest.fixture
    def candidate_service(self, feed_config):
        """Create a CandidateGenerationService instance."""
        return CandidateGenerationService(feed_config=feed_config)

    def test_init_stores_config(self, feed_config):
        """Test that __init__ stores the feed_config correctly."""
        # Arrange & Act
        service = CandidateGenerationService(feed_config=feed_config)

        # Assert
        assert service.config == feed_config

    def test_generate_candidate_pools_creates_three_pools(self, candidate_service):
        """Test that generate_candidate_pools creates all three candidate pools."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:1"],
            "source": ["firehose", "firehose", "firehose"],
            "synctimestamp": ["2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00"],
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [0.5, 1.5, 2.5],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        assert isinstance(result, CandidatePostPools)
        assert hasattr(result, "reverse_chronological")
        assert hasattr(result, "engagement")
        assert hasattr(result, "treatment")
        assert isinstance(result.reverse_chronological, pd.DataFrame)
        assert isinstance(result.engagement, pd.DataFrame)
        assert isinstance(result.treatment, pd.DataFrame)

    def test_generate_candidate_pools_filters_by_author_count(self, candidate_service):
        """Test that generate_candidate_pools filters posts by author count limit."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "author_did": ["did:test:1", "did:test:1", "did:test:1", "did:test:1", "did:test:2"],
            "source": ["firehose"] * 5,
            "synctimestamp": ["2024-01-01T00:00:00"] * 5,
            "engagement_score": [1.0, 2.0, 3.0, 4.0, 5.0],
            "treatment_score": [0.5, 1.5, 2.5, 3.5, 4.5],
        })
        # max_num_times_user_can_appear_in_feed is 3, so did:test:1 should only have 3 posts

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        # Check that did:test:1 appears at most 3 times in each pool
        for pool_name in ["reverse_chronological", "engagement", "treatment"]:
            pool = getattr(result, pool_name)
            author_1_posts = pool[pool["author_did"] == "did:test:1"]
            assert len(author_1_posts) <= 3, f"{pool_name} should have at most 3 posts from did:test:1"
        # did:test:2 should still have 1 post
        author_2_posts = result.reverse_chronological[result.reverse_chronological["author_did"] == "did:test:2"]
        assert len(author_2_posts) == 1

    def test_generate_candidate_pools_reverse_chronological_sorted(self, candidate_service):
        """Test that reverse_chronological pool is sorted by synctimestamp descending."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "source": ["firehose", "firehose", "firehose"],
            "synctimestamp": [
                "2024-01-01T00:00:00",
                "2024-01-01T02:00:00",
                "2024-01-01T01:00:00",
            ],
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [0.5, 1.5, 2.5],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        reverse_chron = result.reverse_chronological
        assert len(reverse_chron) == 3
        # Should be sorted descending by synctimestamp
        timestamps = pd.to_datetime(reverse_chron["synctimestamp"]).tolist()
        assert timestamps == sorted(timestamps, reverse=True)

    def test_generate_candidate_pools_reverse_chronological_only_firehose(self, candidate_service):
        """Test that reverse_chronological pool only includes firehose source posts."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "source": ["firehose", "most_liked", "firehose"],
            "synctimestamp": ["2024-01-01T00:00:00"] * 3,
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [0.5, 1.5, 2.5],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        reverse_chron = result.reverse_chronological
        assert len(reverse_chron) == 2
        assert all(reverse_chron["source"] == "firehose")
        assert "uri1" in reverse_chron["uri"].values
        assert "uri3" in reverse_chron["uri"].values
        assert "uri2" not in reverse_chron["uri"].values

    def test_generate_candidate_pools_engagement_sorted(self, candidate_service):
        """Test that engagement pool is sorted by engagement_score descending."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "source": ["firehose"] * 3,
            "synctimestamp": ["2024-01-01T00:00:00"] * 3,
            "engagement_score": [1.0, 3.0, 2.0],
            "treatment_score": [0.5, 1.5, 2.5],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        engagement = result.engagement
        assert len(engagement) == 3
        # Should be sorted descending by engagement_score
        scores = engagement["engagement_score"].tolist()
        assert scores == sorted(scores, reverse=True)
        # Verify order: uri2 (3.0), uri3 (2.0), uri1 (1.0)
        assert engagement.iloc[0]["uri"] == "uri2"
        assert engagement.iloc[1]["uri"] == "uri3"
        assert engagement.iloc[2]["uri"] == "uri1"

    def test_generate_candidate_pools_treatment_sorted(self, candidate_service):
        """Test that treatment pool is sorted by treatment_score descending."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "source": ["firehose"] * 3,
            "synctimestamp": ["2024-01-01T00:00:00"] * 3,
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [1.0, 3.0, 2.0],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        treatment = result.treatment
        assert len(treatment) == 3
        # Should be sorted descending by treatment_score
        scores = treatment["treatment_score"].tolist()
        assert scores == sorted(scores, reverse=True)
        # Verify order: uri2 (3.0), uri3 (2.0), uri1 (1.0)
        assert treatment.iloc[0]["uri"] == "uri2"
        assert treatment.iloc[1]["uri"] == "uri3"
        assert treatment.iloc[2]["uri"] == "uri1"

    def test_generate_candidate_pools_handles_empty_dataframe(self, candidate_service):
        """Test that generate_candidate_pools handles empty DataFrame gracefully."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": [],
            "author_did": [],
            "source": [],
            "synctimestamp": [],
            "engagement_score": [],
            "treatment_score": [],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        assert isinstance(result, CandidatePostPools)
        assert len(result.reverse_chronological) == 0
        assert len(result.engagement) == 0
        assert len(result.treatment) == 0

    def test_generate_candidate_pools_handles_single_post(self, candidate_service):
        """Test that generate_candidate_pools handles single post correctly."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1"],
            "author_did": ["did:test:1"],
            "source": ["firehose"],
            "synctimestamp": ["2024-01-01T00:00:00"],
            "engagement_score": [1.0],
            "treatment_score": [0.5],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        assert len(result.reverse_chronological) == 1
        assert len(result.engagement) == 1
        assert len(result.treatment) == 1
        assert result.reverse_chronological.iloc[0]["uri"] == "uri1"
        assert result.engagement.iloc[0]["uri"] == "uri1"
        assert result.treatment.iloc[0]["uri"] == "uri1"

    def test_generate_candidate_pools_all_pools_include_all_sources(self, candidate_service):
        """Test that engagement and treatment pools include all sources, not just firehose."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "source": ["firehose", "most_liked", "firehose"],
            "synctimestamp": ["2024-01-01T00:00:00"] * 3,
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [0.5, 1.5, 2.5],
        })

        # Act
        result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        # Reverse chronological should only have firehose
        assert len(result.reverse_chronological) == 2
        assert all(result.reverse_chronological["source"] == "firehose")
        # Engagement and treatment should have all posts
        assert len(result.engagement) == 3
        assert len(result.treatment) == 3
        assert "most_liked" in result.engagement["source"].values
        assert "most_liked" in result.treatment["source"].values

    def test_generate_candidate_pools_handles_empty_reverse_chronological_pool(self, candidate_service):
        """Test that generate_candidate_pools handles empty reverse_chronological pool when no firehose posts exist."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:2", "did:test:3"],
            "source": ["most_liked", "most_liked", "most_liked"],  # No firehose posts
            "synctimestamp": ["2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00"],
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [0.5, 1.5, 2.5],
        })

        # Act
        with patch.object(candidate_service, "logger") as mock_logger:
            result = candidate_service.generate_candidate_pools(posts_df=posts_df)

        # Assert
        assert isinstance(result, CandidatePostPools)
        # Reverse chronological should be empty but with correct schema
        assert len(result.reverse_chronological) == 0
        assert list(result.reverse_chronological.columns) == list(posts_df.columns)
        # Engagement and treatment should still have all posts
        assert len(result.engagement) == 3
        assert len(result.treatment) == 3
        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        warning_message = str(mock_logger.warning.call_args[0][0])
        assert "reverse_chronological_candidate_pool is empty" in warning_message
        assert "no firehose source posts found" in warning_message

    def test_filter_posts_by_author_count_keeps_first_n_posts_per_author(self, candidate_service):
        """Test that _filter_posts_by_author_count keeps first N posts per author."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "author_did": ["did:test:1", "did:test:1", "did:test:1", "did:test:2", "did:test:2"],
            "source": ["firehose"] * 5,
            "synctimestamp": ["2024-01-01T00:00:00"] * 5,
            "engagement_score": [1.0, 2.0, 3.0, 4.0, 5.0],
            "treatment_score": [0.5, 1.5, 2.5, 3.5, 4.5],
        })
        max_count = 2

        # Act
        result = candidate_service._filter_posts_by_author_count(
            posts_df=posts_df,
            max_count=max_count,
        )

        # Assert
        # Each author should have at most 2 posts
        author_1_posts = result[result["author_did"] == "did:test:1"]
        author_2_posts = result[result["author_did"] == "did:test:2"]
        assert len(author_1_posts) == max_count
        assert len(author_2_posts) == max_count
        # Should keep first posts (uri1, uri2 for author 1, uri4, uri5 for author 2)
        assert "uri1" in author_1_posts["uri"].values
        assert "uri2" in author_1_posts["uri"].values
        assert "uri3" not in result["uri"].values

    def test_filter_posts_by_author_count_keeps_all_when_fewer_than_max(self, candidate_service):
        """Test that _filter_posts_by_author_count keeps all posts when author has fewer than max."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "author_did": ["did:test:1", "did:test:1"],
            "source": ["firehose"] * 2,
            "synctimestamp": ["2024-01-01T00:00:00"] * 2,
            "engagement_score": [1.0, 2.0],
            "treatment_score": [0.5, 1.5],
        })
        max_count = 5  # More than number of posts

        # Act
        result = candidate_service._filter_posts_by_author_count(
            posts_df=posts_df,
            max_count=max_count,
        )

        # Assert
        assert len(result) == 2
        assert "uri1" in result["uri"].values
        assert "uri2" in result["uri"].values

    def test_filter_posts_by_author_count_handles_empty_dataframe(self, candidate_service):
        """Test that _filter_posts_by_author_count handles empty DataFrame."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": [],
            "author_did": [],
            "source": [],
            "synctimestamp": [],
            "engagement_score": [],
            "treatment_score": [],
        })
        max_count = 3

        # Act
        result = candidate_service._filter_posts_by_author_count(
            posts_df=posts_df,
            max_count=max_count,
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_filter_posts_by_author_count_handles_single_author(self, candidate_service):
        """Test that _filter_posts_by_author_count handles single author correctly."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "author_did": ["did:test:1"] * 5,
            "source": ["firehose"] * 5,
            "synctimestamp": ["2024-01-01T00:00:00"] * 5,
            "engagement_score": [1.0, 2.0, 3.0, 4.0, 5.0],
            "treatment_score": [0.5, 1.5, 2.5, 3.5, 4.5],
        })
        max_count = 2

        # Act
        result = candidate_service._filter_posts_by_author_count(
            posts_df=posts_df,
            max_count=max_count,
        )

        # Assert
        assert len(result) == max_count
        assert len(result[result["author_did"] == "did:test:1"]) == max_count

    def test_filter_posts_by_author_count_preserves_dataframe_structure(self, candidate_service):
        """Test that _filter_posts_by_author_count preserves DataFrame structure and columns."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "author_did": ["did:test:1", "did:test:1", "did:test:2"],
            "source": ["firehose"] * 3,
            "synctimestamp": ["2024-01-01T00:00:00"] * 3,
            "engagement_score": [1.0, 2.0, 3.0],
            "treatment_score": [0.5, 1.5, 2.5],
            "extra_column": ["a", "b", "c"],
        })
        max_count = 1

        # Act
        result = candidate_service._filter_posts_by_author_count(
            posts_df=posts_df,
            max_count=max_count,
        )

        # Assert
        assert set(result.columns) == set(posts_df.columns)
        assert "extra_column" in result.columns
        assert len(result) == 2  # One post per author

