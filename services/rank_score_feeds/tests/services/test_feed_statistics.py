"""Tests for FeedStatisticsService."""

import json
import pytest

from services.rank_score_feeds.models import CustomFeedPost
from services.rank_score_feeds.services.feed_statistics import FeedStatisticsService


class TestFeedStatisticsService:
    """Tests for FeedStatisticsService."""

    @pytest.fixture
    def service(self):
        """Create a FeedStatisticsService instance."""
        return FeedStatisticsService()

    def test_init_creates_service_instance(self, service):
        """Test that the service initializes correctly."""
        # Arrange & Act are handled by fixture
        # Assert
        assert isinstance(service, FeedStatisticsService)

    def test_generate_feed_statistics_mixed_feed(self, service):
        """Test statistics generation for a mixed in/out-of-network feed."""
        # Arrange
        feed = [
            CustomFeedPost(item="uri1", is_in_network=True),
            CustomFeedPost(item="uri2", is_in_network=False),
            CustomFeedPost(item="uri3", is_in_network=True),
            CustomFeedPost(item="uri4", is_in_network=False),
            CustomFeedPost(item="uri5", is_in_network=False),
        ]
        expected = {
            "feed_length": 5,
            "total_in_network": 2,
            "prop_in_network": 0.4,
        }

        # Act
        result_json = service.generate_feed_statistics(feed=feed)
        result = json.loads(result_json)

        # Assert
        assert isinstance(result_json, str)
        assert result["feed_length"] == expected["feed_length"]
        assert result["total_in_network"] == expected["total_in_network"]
        assert result["prop_in_network"] == expected["prop_in_network"]

    def test_generate_feed_statistics_empty_feed(self, service):
        """Test statistics generation for an empty feed."""
        # Arrange
        feed: list[CustomFeedPost] = []
        expected = {
            "feed_length": 0,
            "total_in_network": 0,
            "prop_in_network": 0.0,
        }

        # Act
        result = json.loads(service.generate_feed_statistics(feed=feed))

        # Assert
        assert result["feed_length"] == expected["feed_length"]
        assert result["total_in_network"] == expected["total_in_network"]
        assert result["prop_in_network"] == expected["prop_in_network"]

    def test_generate_feed_statistics_all_in_network(self, service):
        """Test statistics when all posts are in-network."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=True) for i in range(1, 5)
        ]
        expected = {
            "feed_length": 4,
            "total_in_network": 4,
            "prop_in_network": 1.0,
        }

        # Act
        result = json.loads(service.generate_feed_statistics(feed=feed))

        # Assert
        assert result["feed_length"] == expected["feed_length"]
        assert result["total_in_network"] == expected["total_in_network"]
        assert result["prop_in_network"] == expected["prop_in_network"]

    def test_generate_feed_statistics_all_out_of_network(self, service):
        """Test statistics when all posts are out-of-network."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=False) for i in range(1, 5)
        ]
        expected = {
            "feed_length": 4,
            "total_in_network": 0,
            "prop_in_network": 0.0,
        }

        # Act
        result = json.loads(service.generate_feed_statistics(feed=feed))

        # Assert
        assert result["feed_length"] == expected["feed_length"]
        assert result["total_in_network"] == expected["total_in_network"]
        assert result["prop_in_network"] == expected["prop_in_network"]

    def test_generate_feed_statistics_rounds_to_three_decimals(self, service):
        """Test that prop_in_network is rounded to three decimals."""
        # Arrange
        # 2 in-network out of 3 total -> 0.666... -> 0.667 after rounding(., 3)
        feed = [
            CustomFeedPost(item="uri1", is_in_network=True),
            CustomFeedPost(item="uri2", is_in_network=True),
            CustomFeedPost(item="uri3", is_in_network=False),
        ]
        expected_prop = 0.667

        # Act
        result = json.loads(service.generate_feed_statistics(feed=feed))

        # Assert
        assert result["feed_length"] == 3
        assert result["total_in_network"] == 2
        assert result["prop_in_network"] == expected_prop
