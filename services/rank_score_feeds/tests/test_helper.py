"""Tests for helper functions in rank_score_feeds service.

This test suite verifies the functionality of helper functions:
- load_feed_input_data: Loading feed input data from multiple services
- Error handling and edge cases

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from services.rank_score_feeds.helper import load_feed_input_data
from services.rank_score_feeds.models import FeedInputData


class TestLoadFeedInputData:
    """Tests for load_feed_input_data function."""

    @patch("services.rank_score_feeds.helper.load_enriched_posts")
    @patch("services.rank_score_feeds.helper.load_user_social_network_map")
    @patch("services.rank_score_feeds.helper.load_latest_superposters")
    @patch("services.rank_score_feeds.helper.calculate_lookback_datetime_str")
    def test_load_feed_input_data_returns_correct_structure(
        self,
        mock_calculate_lookback,
        mock_load_superposters,
        mock_load_social_network,
        mock_load_enriched_posts,
    ):
        """Test that load_feed_input_data returns FeedInputData with correct structure."""
        # Arrange
        mock_calculate_lookback.return_value = "2024-01-01T00:00:00Z"
        mock_posts_df = pd.DataFrame({"uri": ["post1"], "author_did": ["did:plc:user1"]})
        mock_social_network = {"did:plc:user1": ["did:plc:user2"]}
        mock_superposters = {"did:plc:superposter1"}

        mock_load_enriched_posts.return_value = mock_posts_df
        mock_load_social_network.return_value = mock_social_network
        mock_load_superposters.return_value = mock_superposters

        # Act
        result = load_feed_input_data(lookback_days=7)

        # Assert
        assert isinstance(result, dict)
        assert "consolidate_enrichment_integrations" in result
        assert "scraped_user_social_network" in result
        assert "superposters" in result
        assert isinstance(result["consolidate_enrichment_integrations"], pd.DataFrame)
        assert isinstance(result["scraped_user_social_network"], dict)
        assert isinstance(result["superposters"], set)
        assert result["consolidate_enrichment_integrations"].equals(mock_posts_df)
        assert result["scraped_user_social_network"] == mock_social_network
        assert result["superposters"] == mock_superposters

    @patch("services.rank_score_feeds.helper.load_enriched_posts")
    @patch("services.rank_score_feeds.helper.load_user_social_network_map")
    @patch("services.rank_score_feeds.helper.load_latest_superposters")
    @patch("services.rank_score_feeds.helper.calculate_lookback_datetime_str")
    def test_load_feed_input_data_passes_lookback_timestamp(
        self,
        mock_calculate_lookback,
        mock_load_superposters,
        mock_load_social_network,
        mock_load_enriched_posts,
    ):
        """Test that load_feed_input_data passes lookback timestamp correctly."""
        # Arrange
        mock_calculate_lookback.return_value = "2024-01-01T00:00:00Z"
        mock_load_enriched_posts.return_value = pd.DataFrame()
        mock_load_social_network.return_value = {}
        mock_load_superposters.return_value = set()

        # Act
        load_feed_input_data(lookback_days=14)

        # Assert
        mock_calculate_lookback.assert_called_once()
        mock_load_enriched_posts.assert_called_once_with(
            latest_timestamp="2024-01-01T00:00:00Z"
        )
        mock_load_superposters.assert_called_once()

    @patch("services.rank_score_feeds.helper.load_enriched_posts")
    @patch("services.rank_score_feeds.helper.load_user_social_network_map")
    @patch("services.rank_score_feeds.helper.load_latest_superposters")
    @patch("services.rank_score_feeds.helper.calculate_lookback_datetime_str")
    def test_load_feed_input_data_handles_empty_data(
        self,
        mock_calculate_lookback,
        mock_load_superposters,
        mock_load_social_network,
        mock_load_enriched_posts,
    ):
        """Test that load_feed_input_data handles empty data correctly."""
        # Arrange
        mock_calculate_lookback.return_value = "2024-01-01T00:00:00Z"
        mock_load_enriched_posts.return_value = pd.DataFrame()
        mock_load_social_network.return_value = {}
        mock_load_superposters.return_value = set()

        # Act
        result = load_feed_input_data()

        # Assert
        assert isinstance(result, dict)
        assert len(result["consolidate_enrichment_integrations"]) == 0
        assert len(result["scraped_user_social_network"]) == 0
        assert len(result["superposters"]) == 0

