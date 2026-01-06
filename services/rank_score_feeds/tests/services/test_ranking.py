"""Tests for RankingService."""

import pandas as pd
import pytest

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CustomFeedPost
from services.rank_score_feeds.services.ranking import RankingService


class TestRankingService:
    """Tests for RankingService."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig(max_num_times_user_can_appear_in_feed=3)

    @pytest.fixture
    def ranking_service(self, feed_config):
        """Create a RankingService instance."""
        return RankingService(feed_config=feed_config)

    def test_init_stores_config(self, feed_config):
        """Test that __init__ stores the feed_config correctly."""
        # Arrange & Act
        service = RankingService(feed_config=feed_config)

        # Assert
        assert service.config == feed_config

    def test_create_ranked_candidate_feed_combines_in_network_and_out_of_network(
        self, ranking_service
    ):
        """Test that create_ranked_candidate_feed combines in-network and out-of-network posts."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri2"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4"],
            "source": ["firehose", "firehose", "firehose", "firehose"],
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert isinstance(result, list)
        assert all(isinstance(post, CustomFeedPost) for post in result)
        assert len(result) == 4
        # Check in-network posts are marked correctly
        in_network_posts = [p for p in result if p.is_in_network]
        assert len(in_network_posts) == 2
        assert {p.item for p in in_network_posts} == {"uri1", "uri2"}
        # Check out-of-network posts are marked correctly
        out_of_network_posts = [p for p in result if not p.is_in_network]
        assert len(out_of_network_posts) == 2
        assert {p.item for p in out_of_network_posts} == {"uri3", "uri4"}

    def test_create_ranked_candidate_feed_in_network_posts_first(
        self, ranking_service
    ):
        """Test that in-network posts appear before out-of-network posts in the feed."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri2", "uri4"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "source": ["firehose"] * 5,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert len(result) == 5
        # First two posts should be in-network
        assert result[0].is_in_network is True
        assert result[1].is_in_network is True
        # Remaining posts should be out-of-network
        assert all(not post.is_in_network for post in result[2:])

    def test_create_ranked_candidate_feed_limits_in_network_posts_by_ratio(
        self, ranking_service
    ):
        """Test that max_in_network_posts_ratio limits the number of in-network posts."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri2", "uri3", "uri4", "uri5"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5", "uri6", "uri7"],
            "source": ["firehose"] * 7,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.3  # Should allow max 3 in-network posts

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        in_network_count = sum(1 for p in result if p.is_in_network)
        assert in_network_count == 3  # max_feed_length * 0.3 = 3

    def test_create_ranked_candidate_feed_limits_in_network_posts_by_available(
        self, ranking_service
    ):
        """Test that in-network posts are limited by available posts when fewer than ratio allows."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri2"]  # Only 2 in-network posts available
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "source": ["firehose"] * 5,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5  # Would allow 5, but only 2 available

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        in_network_count = sum(1 for p in result if p.is_in_network)
        assert in_network_count == 2  # Limited by available posts

    def test_create_ranked_candidate_feed_uses_most_liked_for_engagement_condition(
        self, ranking_service
    ):
        """Test that engagement condition uses most_liked source for out-of-network posts."""
        # Arrange
        condition = "engagement"
        in_network_uris = ["uri1"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4"],
            "source": ["firehose", "most_liked", "firehose", "most_liked"],
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        out_of_network_posts = [p for p in result if not p.is_in_network]
        # Should only include most_liked posts, not firehose
        assert len(out_of_network_posts) == 2
        assert all(p.item in ["uri2", "uri4"] for p in out_of_network_posts)

    def test_create_ranked_candidate_feed_uses_most_liked_for_representative_diversification(
        self, ranking_service
    ):
        """Test that representative_diversification condition uses most_liked source."""
        # Arrange
        condition = "representative_diversification"
        in_network_uris = ["uri1"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "source": ["firehose", "most_liked", "firehose"],
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        out_of_network_posts = [p for p in result if not p.is_in_network]
        # Should only include most_liked posts
        assert len(out_of_network_posts) == 1
        assert out_of_network_posts[0].item == "uri2"

    def test_create_ranked_candidate_feed_uses_firehose_for_other_conditions(
        self, ranking_service
    ):
        """Test that other conditions use firehose source for out-of-network posts."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "source": ["firehose", "most_liked", "firehose"],
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        out_of_network_posts = [p for p in result if not p.is_in_network]
        # Should only include firehose posts, not most_liked
        assert len(out_of_network_posts) == 1
        assert out_of_network_posts[0].item == "uri3"

    def test_create_ranked_candidate_feed_returns_only_out_of_network_when_no_in_network(
        self, ranking_service
    ):
        """Test that feed returns only out-of-network posts when no in-network posts exist."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = []  # No in-network posts
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "source": ["firehose"] * 3,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert len(result) == 3
        assert all(not post.is_in_network for post in result)
        assert {p.item for p in result} == {"uri1", "uri2", "uri3"}

    def test_create_ranked_candidate_feed_raises_error_for_none_post_pool(
        self, ranking_service
    ):
        """Test that create_ranked_candidate_feed raises ValueError for None post_pool."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1"]
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act & Assert
        with pytest.raises(ValueError, match="post_pool cannot be None"):
            ranking_service.create_ranked_candidate_feed(
                condition=condition,
                in_network_candidate_post_uris=in_network_uris,
                post_pool=None,
                max_feed_length=max_feed_length,
                max_in_network_posts_ratio=max_in_network_posts_ratio,
            )

    def test_create_ranked_candidate_feed_raises_error_for_empty_post_pool(
        self, ranking_service
    ):
        """Test that create_ranked_candidate_feed raises ValueError for empty post_pool."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1"]
        post_pool = pd.DataFrame({
            "uri": [],
            "source": [],
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act & Assert
        with pytest.raises(ValueError, match="post_pool cannot be None"):
            ranking_service.create_ranked_candidate_feed(
                condition=condition,
                in_network_candidate_post_uris=in_network_uris,
                post_pool=post_pool,
                max_feed_length=max_feed_length,
                max_in_network_posts_ratio=max_in_network_posts_ratio,
            )

    def test_create_ranked_candidate_feed_handles_all_in_network_posts(
        self, ranking_service
    ):
        """Test that feed handles case where all posts in pool are in-network."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri2", "uri3"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "source": ["firehose"] * 3,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert len(result) == 3
        assert all(post.is_in_network for post in result)
        # Should be limited by ratio (max 5, but only 3 available)
        in_network_count = sum(1 for p in result if p.is_in_network)
        assert in_network_count == 3

    def test_create_ranked_candidate_feed_handles_no_out_of_network_posts(
        self, ranking_service
    ):
        """Test that feed handles case where no out-of-network posts match source filter."""
        # Arrange
        condition = "reverse_chronological"  # Uses firehose
        in_network_uris = ["uri1"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "source": ["firehose", "most_liked"],  # uri2 is most_liked, not firehose
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert len(result) == 1
        assert result[0].is_in_network is True
        assert result[0].item == "uri1"

    def test_create_ranked_candidate_feed_preserves_post_order(
        self, ranking_service
    ):
        """Test that feed preserves the order of posts from the DataFrames."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri2", "uri4"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "source": ["firehose"] * 5,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert len(result) == 5
        # In-network posts should be first (uri2, uri4 in their original order)
        assert result[0].item == "uri2"
        assert result[0].is_in_network is True
        assert result[1].item == "uri4"
        assert result[1].is_in_network is True
        # Out-of-network posts follow (uri1, uri3, uri5)
        assert result[2].item == "uri1"
        assert result[2].is_in_network is False
        assert result[3].item == "uri3"
        assert result[3].is_in_network is False
        assert result[4].item == "uri5"
        assert result[4].is_in_network is False

    def test_create_ranked_candidate_feed_handles_zero_max_in_network_ratio(
        self, ranking_service
    ):
        """Test that feed handles zero max_in_network_posts_ratio correctly.
        
        When max_in_network_posts_ratio is 0.0, in-network posts are filtered out
        before combining with out-of-network posts. Only out-of-network posts
        that match the source filter are returned.
        """
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri2", "uri3"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4", "uri5"],
            "source": ["firehose"] * 5,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.0  # No in-network posts allowed

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        in_network_count = sum(1 for p in result if p.is_in_network)
        assert in_network_count == 0
        # Only out-of-network posts are returned (uri4, uri5)
        # uri1, uri2, uri3 are filtered out as in-network posts
        assert len(result) == 2
        assert all(not post.is_in_network for post in result)
        assert {p.item for p in result} == {"uri4", "uri5"}

    def test_create_ranked_candidate_feed_handles_one_max_in_network_ratio(
        self, ranking_service
    ):
        """Test that feed handles max_in_network_posts_ratio of 1.0 correctly."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri2"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3", "uri4"],
            "source": ["firehose"] * 4,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 1.0  # All posts can be in-network

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        in_network_count = sum(1 for p in result if p.is_in_network)
        assert in_network_count == 2  # All available in-network posts
        assert len(result) == 4

    def test_create_ranked_candidate_feed_handles_in_network_uris_not_in_pool(
        self, ranking_service
    ):
        """Test that feed handles in-network URIs that don't exist in post_pool."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1", "uri_nonexistent"]  # uri_nonexistent not in pool
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2", "uri3"],
            "source": ["firehose"] * 3,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        in_network_posts = [p for p in result if p.is_in_network]
        assert len(in_network_posts) == 1
        assert in_network_posts[0].item == "uri1"
        # uri_nonexistent should not appear in results
        assert "uri_nonexistent" not in [p.item for p in result]

    def test_create_ranked_candidate_feed_creates_custom_feed_post_objects(
        self, ranking_service
    ):
        """Test that feed creates CustomFeedPost objects with correct structure."""
        # Arrange
        condition = "reverse_chronological"
        in_network_uris = ["uri1"]
        post_pool = pd.DataFrame({
            "uri": ["uri1", "uri2"],
            "source": ["firehose"] * 2,
        })
        max_feed_length = 10
        max_in_network_posts_ratio = 0.5

        # Act
        result = ranking_service.create_ranked_candidate_feed(
            condition=condition,
            in_network_candidate_post_uris=in_network_uris,
            post_pool=post_pool,
            max_feed_length=max_feed_length,
            max_in_network_posts_ratio=max_in_network_posts_ratio,
        )

        # Assert
        assert len(result) == 2
        for post in result:
            assert isinstance(post, CustomFeedPost)
            assert hasattr(post, "item")
            assert hasattr(post, "is_in_network")
            assert isinstance(post.item, str)
            assert isinstance(post.is_in_network, bool)
            assert post.item in ["uri1", "uri2"]

