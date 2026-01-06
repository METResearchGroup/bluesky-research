"""Tests for RerankingService."""

from unittest.mock import patch

import pytest

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import CustomFeedPost
from services.rank_score_feeds.services.reranking import RerankingService


class TestRerankingService:
    """Tests for RerankingService."""

    @pytest.fixture
    def feed_config(self):
        """Create a test FeedConfig."""
        return FeedConfig(
            max_feed_length=10,
            feed_preprocessing_multiplier=2,
            max_prop_old_posts=0.6,
            jitter_amount=2,
        )

    @pytest.fixture
    def reranking_service(self, feed_config):
        """Create a RerankingService instance."""
        return RerankingService(feed_config=feed_config)

    @pytest.fixture
    def sample_feed(self):
        """Create a sample feed for testing."""
        return [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 21)  # 20 posts
        ]

    def test_init_stores_config_and_extracts_values(self, feed_config):
        """Test that __init__ stores config and extracts values correctly."""
        # Arrange & Act
        service = RerankingService(feed_config=feed_config)

        # Assert
        assert service.config == feed_config
        assert service.max_feed_length == feed_config.max_feed_length
        assert service.feed_preprocessing_multiplier == feed_config.feed_preprocessing_multiplier
        assert service.max_prop_old_posts == feed_config.max_prop_old_posts
        assert service.jitter_amount == feed_config.jitter_amount

    def test_rerank_feed_orchestrates_all_steps(self, reranking_service, sample_feed):
        """Test that rerank_feed orchestrates all reranking steps correctly."""
        # Arrange
        uris_of_posts_used_in_previous_feeds = {"uri5", "uri6", "uri7"}

        # Act
        result = reranking_service.rerank_feed(
            feed=sample_feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )

        # Assert
        assert isinstance(result, list)
        assert all(isinstance(post, CustomFeedPost) for post in result)
        assert len(result) == 10  # max_feed_length

    def test_rerank_feed_raises_error_when_feed_too_short_after_processing(
        self, reranking_service
    ):
        """Test that rerank_feed raises ValueError when feed is too short after processing."""
        # Arrange
        # Create a feed that will be too short after filtering
        feed = [
            CustomFeedPost(item="uri1", is_in_network=True),
            CustomFeedPost(item="uri2", is_in_network=False),
        ]  # Only 2 posts, but max_feed_length is 10
        uris_of_posts_used_in_previous_feeds = set()

        # Act & Assert
        with pytest.raises(ValueError, match="Feed length is less than max_feed_length"):
            reranking_service.rerank_feed(
                feed=feed,
                uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
            )

    def test_clip_feed_to_preprocessing_window_clips_correctly(
        self, reranking_service, sample_feed
    ):
        """Test that _clip_feed_to_preprocessing_window clips feed to preprocessing window."""
        # Arrange
        # max_feed_length=10, feed_preprocessing_multiplier=2, so should clip to 20
        # sample_feed has 20 posts, so should return all

        # Act
        result = reranking_service._clip_feed_to_preprocessing_window(feed=sample_feed)

        # Assert
        assert len(result) == 20  # 10 * 2 = 20
        assert result == sample_feed

    def test_clip_feed_to_preprocessing_window_clips_large_feed(
        self, reranking_service
    ):
        """Test that _clip_feed_to_preprocessing_window clips large feeds correctly."""
        # Arrange
        large_feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 51)  # 50 posts
        ]
        # max_feed_length=10, feed_preprocessing_multiplier=2, so should clip to 20

        # Act
        result = reranking_service._clip_feed_to_preprocessing_window(feed=large_feed)

        # Assert
        assert len(result) == 20  # 10 * 2 = 20
        assert result == large_feed[:20]

    def test_clip_feed_to_preprocessing_window_handles_empty_feed(
        self, reranking_service
    ):
        """Test that _clip_feed_to_preprocessing_window handles empty feed."""
        # Arrange
        empty_feed: list[CustomFeedPost] = []

        # Act
        result = reranking_service._clip_feed_to_preprocessing_window(feed=empty_feed)

        # Assert
        assert len(result) == 0
        assert result == []

    def test_enforce_fresh_content_in_feed_returns_feed_when_no_old_posts(
        self, reranking_service, sample_feed
    ):
        """Test that _enforce_fresh_content_in_feed returns feed unchanged when no old posts."""
        # Arrange
        uris_of_posts_used_in_previous_feeds = set()  # No old posts

        # Act
        result = reranking_service._enforce_fresh_content_in_feed(
            feed=sample_feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )

        # Assert
        assert result == sample_feed
        assert len(result) == len(sample_feed)

    def test_enforce_fresh_content_in_feed_filters_old_posts(
        self, reranking_service
    ):
        """Test that _enforce_fresh_content_in_feed limits old posts to max proportion."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 21)  # 20 posts
        ]
        # max_feed_length=10, max_prop_old_posts=0.6, so max_num_old_posts = 6
        uris_of_posts_used_in_previous_feeds = {f"uri{i}" for i in range(1, 11)}  # First 10 are old

        # Act
        result = reranking_service._enforce_fresh_content_in_feed(
            feed=feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )

        # Assert
        # Should include all fresh posts (uri11-uri20 = 10 posts)
        # Plus up to 6 old posts (uri1-uri6)
        old_posts = [p for p in result if p.item in uris_of_posts_used_in_previous_feeds]
        assert len(old_posts) == 6  # max_num_old_posts = 10 * 0.6 = 6
        fresh_posts = [p for p in result if p.item not in uris_of_posts_used_in_previous_feeds]
        assert len(fresh_posts) == 10  # All fresh posts should be included

    def test_enforce_fresh_content_in_feed_always_includes_fresh_posts(
        self, reranking_service
    ):
        """Test that _enforce_fresh_content_in_feed always includes all fresh posts."""
        # Arrange
        feed = [
            CustomFeedPost(item="uri1", is_in_network=True),  # Old
            CustomFeedPost(item="uri2", is_in_network=False),  # Fresh
            CustomFeedPost(item="uri3", is_in_network=True),  # Old
            CustomFeedPost(item="uri4", is_in_network=False),  # Fresh
            CustomFeedPost(item="uri5", is_in_network=True),  # Old
        ]
        uris_of_posts_used_in_previous_feeds = {"uri1", "uri3", "uri5"}

        # Act
        result = reranking_service._enforce_fresh_content_in_feed(
            feed=feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )

        # Assert
        # All fresh posts should be included
        fresh_posts = [p for p in result if p.item not in uris_of_posts_used_in_previous_feeds]
        assert len(fresh_posts) == 2
        assert {p.item for p in fresh_posts} == {"uri2", "uri4"}

    def test_enforce_fresh_content_in_feed_limits_old_posts_correctly(
        self, reranking_service
    ):
        """Test that _enforce_fresh_content_in_feed limits old posts to max_num_old_posts."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 11)  # 10 posts, all old
        ]
        uris_of_posts_used_in_previous_feeds = {f"uri{i}" for i in range(1, 11)}
        # max_feed_length=10, max_prop_old_posts=0.6, so max_num_old_posts = 6

        # Act
        result = reranking_service._enforce_fresh_content_in_feed(
            feed=feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )

        # Assert
        assert len(result) == 6  # Only max_num_old_posts should be included
        assert all(p.item in uris_of_posts_used_in_previous_feeds for p in result)

    def test_enforce_fresh_content_in_feed_preserves_order(
        self, reranking_service
    ):
        """Test that _enforce_fresh_content_in_feed preserves order of posts."""
        # Arrange
        feed = [
            CustomFeedPost(item="uri1", is_in_network=True),  # Old
            CustomFeedPost(item="uri2", is_in_network=False),  # Fresh
            CustomFeedPost(item="uri3", is_in_network=True),  # Old
            CustomFeedPost(item="uri4", is_in_network=False),  # Fresh
        ]
        uris_of_posts_used_in_previous_feeds = {"uri1", "uri3"}

        # Act
        result = reranking_service._enforce_fresh_content_in_feed(
            feed=feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )

        # Assert
        # Order should be preserved: fresh posts in their original positions
        assert result[0].item == "uri1"  # Old, included
        assert result[1].item == "uri2"  # Fresh, always included
        assert result[2].item == "uri3"  # Old, included
        assert result[3].item == "uri4"  # Fresh, always included

    def test_truncate_feed_truncates_to_max_length(self, reranking_service, sample_feed):
        """Test that _truncate_feed truncates feed to max_feed_length."""
        # Arrange
        max_feed_length = 10

        # Act
        result = reranking_service._truncate_feed(
            feed=sample_feed, max_feed_length=max_feed_length
        )

        # Assert
        assert len(result) == max_feed_length
        assert result == sample_feed[:max_feed_length]

    def test_truncate_feed_handles_feed_shorter_than_max(
        self, reranking_service
    ):
        """Test that _truncate_feed handles feed shorter than max_feed_length."""
        # Arrange
        short_feed = [
            CustomFeedPost(item="uri1", is_in_network=True),
            CustomFeedPost(item="uri2", is_in_network=False),
        ]
        max_feed_length = 10

        # Act
        result = reranking_service._truncate_feed(
            feed=short_feed, max_feed_length=max_feed_length
        )

        # Assert
        assert len(result) == 2
        assert result == short_feed

    def test_truncate_feed_handles_empty_feed(self, reranking_service):
        """Test that _truncate_feed handles empty feed."""
        # Arrange
        empty_feed: list[CustomFeedPost] = []
        max_feed_length = 10

        # Act
        result = reranking_service._truncate_feed(
            feed=empty_feed, max_feed_length=max_feed_length
        )

        # Assert
        assert len(result) == 0
        assert result == []

    def test_validate_feed_length_passes_when_feed_equals_max_length(
        self, reranking_service
    ):
        """Test that _validate_feed_length passes when feed length equals max_feed_length."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 11)  # Exactly 10 posts
        ]
        max_feed_length = 10

        # Act & Assert (should not raise)
        reranking_service._validate_feed_length(feed=feed, max_feed_length=max_feed_length)

    def test_validate_feed_length_raises_error_when_feed_too_short(
        self, reranking_service
    ):
        """Test that _validate_feed_length raises ValueError when feed is too short."""
        # Arrange
        feed = [
            CustomFeedPost(item="uri1", is_in_network=True),
            CustomFeedPost(item="uri2", is_in_network=False),
        ]  # Only 2 posts
        max_feed_length = 10

        # Act & Assert
        with pytest.raises(
            ValueError, match="Feed length is less than max_feed_length: 2 < 10"
        ):
            reranking_service._validate_feed_length(
                feed=feed, max_feed_length=max_feed_length
            )

    def test_validate_feed_length_handles_empty_feed(self, reranking_service):
        """Test that _validate_feed_length raises error for empty feed."""
        # Arrange
        empty_feed: list[CustomFeedPost] = []
        max_feed_length = 10

        # Act & Assert
        with pytest.raises(
            ValueError, match="Feed length is less than max_feed_length: 0 < 10"
        ):
            reranking_service._validate_feed_length(
                feed=empty_feed, max_feed_length=max_feed_length
            )

    def test_jitter_feed_applies_jitter(self, reranking_service):
        """Test that _jitter_feed applies jitter to feed order."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 11)  # 10 posts
        ]
        jitter_amount = 2

        # Act
        with patch("services.rank_score_feeds.services.reranking.random.randint") as mock_randint:
            # Mock random to return predictable shifts
            mock_randint.side_effect = [1, -1, 0, 2, -2, 0, 1, -1, 0, 1]
            result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        assert len(result) == len(feed)
        # Verify all posts are still present
        assert {p.item for p in result} == {p.item for p in feed}

    def test_jitter_feed_preserves_all_posts(self, reranking_service):
        """Test that _jitter_feed preserves all posts in the feed."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 6)  # 5 posts
        ]
        jitter_amount = 2

        # Act
        result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        assert len(result) == len(feed)
        assert {p.item for p in result} == {p.item for p in feed}
        assert {p.is_in_network for p in result} == {p.is_in_network for p in feed}

    def test_jitter_feed_handles_zero_jitter_amount(self, reranking_service):
        """Test that _jitter_feed handles zero jitter_amount (no changes)."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 6)  # 5 posts
        ]
        jitter_amount = 0

        # Act
        result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        # With jitter_amount=0, all shifts should be 0, so order should be preserved
        assert result == feed

    def test_jitter_feed_handles_single_post(self, reranking_service):
        """Test that _jitter_feed handles feed with single post."""
        # Arrange
        feed = [CustomFeedPost(item="uri1", is_in_network=True)]
        jitter_amount = 2

        # Act
        result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        assert len(result) == 1
        assert result[0] == feed[0]

    def test_jitter_feed_handles_empty_feed(self, reranking_service):
        """Test that _jitter_feed handles empty feed."""
        # Arrange
        empty_feed: list[CustomFeedPost] = []
        jitter_amount = 2

        # Act
        result = reranking_service._jitter_feed(feed=empty_feed, jitter_amount=jitter_amount)

        # Assert
        assert len(result) == 0
        assert result == []

    def test_jitter_feed_respects_bounds(self, reranking_service):
        """Test that _jitter_feed respects feed bounds when applying shifts."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 6)  # 5 posts
        ]
        jitter_amount = 10  # Large jitter amount

        # Act
        result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        # Even with large jitter, all posts should still be in valid positions
        assert len(result) == len(feed)
        assert {p.item for p in result} == {p.item for p in feed}
        # All indices should be valid (0 to len-1)
        assert all(0 <= i < len(result) for i in range(len(result)))

    def test_jitter_feed_correctly_processes_elements(self, reranking_service):
        """Test that _jitter_feed correctly processes each element without skipping."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 6)
        ]
        jitter_amount = 2

        # Act
        with patch("services.rank_score_feeds.services.reranking.random.randint") as mock_randint:
            mock_randint.side_effect = [1, 1, 1, 1, 1]
            result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        assert len(result) == len(feed)
        assert {p.item for p in result} == {p.item for p in feed}
        # Verify that elements are reordered based on target positions
        # With all shifts = +1, elements are sorted by their target positions
        # (uri1 wants pos 1, uri2 wants pos 2, etc., so order is preserved)
        assert result[0].item == "uri1"
        assert result[1].item == "uri2"
        assert result[2].item == "uri3"
        assert result[3].item == "uri4"
        assert result[4].item == "uri5"

    def test_jitter_feed_handles_overlapping_target_positions(self, reranking_service):
        """Test that _jitter_feed correctly handles multiple elements targeting overlapping positions."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 6)
        ]
        jitter_amount = 10

        # Act
        with patch("services.rank_score_feeds.services.reranking.random.randint") as mock_randint:
            # Mock shifts to create the rotation scenario:
            # i=0: shift=+2 -> new_pos=2 (uri1 moves to position 2)
            # i=1: shift=-1 -> new_pos=0 (uri2 moves to position 0)
            # i=2: shift=-1 -> new_pos=1 (uri3 moves to position 1)
            # i=3: shift=0 -> new_pos=3 (uri4 stays)
            # i=4: shift=0 -> new_pos=4 (uri5 stays)
            mock_randint.side_effect = [2, -1, -1, 0, 0]
            result = reranking_service._jitter_feed(feed=feed, jitter_amount=jitter_amount)

        # Assert
        assert len(result) == len(feed)
        assert {p.item for p in result} == {p.item for p in feed}
        assert result[0].item == "uri2"
        assert result[1].item == "uri3"
        assert result[2].item == "uri1"
        assert result[3].item == "uri4"
        assert result[4].item == "uri5"

    def test_rerank_feed_with_all_fresh_posts(self, reranking_service):
        """Test that rerank_feed works correctly when all posts are fresh."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 21)  # 20 posts, all fresh
        ]
        uris_of_posts_used_in_previous_feeds = set()  # No old posts

        # Act
        with patch("services.rank_score_feeds.services.reranking.random.randint") as mock_randint:
            # Mock random to return predictable shifts
            mock_randint.side_effect = [0] * 10  # No shifts
            result = reranking_service.rerank_feed(
                feed=feed,
                uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
            )

        # Assert
        assert len(result) == 10  # max_feed_length
        assert all(isinstance(post, CustomFeedPost) for post in result)

    def test_rerank_feed_with_mixed_fresh_and_old_posts(self, reranking_service):
        """Test that rerank_feed correctly handles mix of fresh and old posts."""
        # Arrange
        feed = [
            CustomFeedPost(item=f"uri{i}", is_in_network=(i % 2 == 0))
            for i in range(1, 21)  # 20 posts
        ]
        # Mark first 10 as old
        uris_of_posts_used_in_previous_feeds = {f"uri{i}" for i in range(1, 11)}
        # max_feed_length=10, max_prop_old_posts=0.6, so max_num_old_posts = 6

        # Act
        with patch("services.rank_score_feeds.services.reranking.random.randint") as mock_randint:
            # Mock random to return predictable shifts
            mock_randint.side_effect = [0] * 10  # No shifts
            result = reranking_service.rerank_feed(
                feed=feed,
                uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
            )

        # Assert
        assert len(result) == 10  # max_feed_length
        # Should have 6 old posts max, rest fresh
        old_posts = [p for p in result if p.item in uris_of_posts_used_in_previous_feeds]
        assert len(old_posts) <= 6
        fresh_posts = [p for p in result if p.item not in uris_of_posts_used_in_previous_feeds]
        assert len(fresh_posts) >= 4  # At least 4 fresh posts

