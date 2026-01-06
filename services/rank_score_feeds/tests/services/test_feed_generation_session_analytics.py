"""Tests for FeedGenerationSessionAnalyticsService.calculate_feed_generation_session_analytics."""

from services.rank_score_feeds.models import (
    CustomFeedPost,
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
)
from services.rank_score_feeds.services.feed_generation_session_analytics import (
    FeedGenerationSessionAnalyticsService,
)


class TestCalculateFeedGenerationSessionAnalytics:
    """Tests for calculate_feed_generation_session_analytics function."""

    def test_overlap_metrics_use_correct_denominators_and_rounding(self):
        """Verify overlap metrics map to correct denominators and are rounded to 3 decimals."""
        # Arrange
        # Engagement URIs: 3 unique -> {a, b, c}
        # Treatment URIs: 4 unique -> {b, c, d, e}
        # Overlap: 2 -> {b, c}
        engagement_feed = FeedWithMetadata(
            feed=[
                CustomFeedPost(item="a", is_in_network=True),
                CustomFeedPost(item="b", is_in_network=False),
                CustomFeedPost(item="c", is_in_network=True),
            ],
            bluesky_handle="user_eng",
            user_did="did:eng",
            condition="engagement",
            feed_statistics="{}",
        )
        treatment_feed = FeedWithMetadata(
            feed=[
                CustomFeedPost(item="b", is_in_network=True),
                CustomFeedPost(item="c", is_in_network=False),
                CustomFeedPost(item="d", is_in_network=False),
                CustomFeedPost(item="e", is_in_network=True),
            ],
            bluesky_handle="user_treat",
            user_did="did:treat",
            condition="representative_diversification",
            feed_statistics="{}",
        )
        user_to_ranked_feed_map = {
            "user_eng": engagement_feed,
            "user_treat": treatment_feed,
        }
        service = FeedGenerationSessionAnalyticsService()

        # Act
        result = service.calculate_feed_generation_session_analytics(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            session_timestamp="2024-01-01T00:00:00",
        )

        # Assert
        assert isinstance(result, FeedGenerationSessionAnalytics)
        # Totals
        assert result.total_feeds == 2
        assert result.total_unique_engagement_uris == 3
        assert result.total_unique_treatment_uris == 4
        # Overlap denominators:
        # prop_overlap_treatment_uris_in_engagement_uris = overlap / total_unique_treatment_uris = 2/4 = 0.5
        # prop_overlap_engagement_uris_in_treatment_uris = overlap / total_unique_engagement_uris = 2/3 = 0.667
        assert result.prop_overlap_treatment_uris_in_engagement_uris == 0.5
        assert result.prop_overlap_engagement_uris_in_treatment_uris == 0.667

    def test_overlap_metrics_handle_zero_denominators(self):
        """Ensure overlap metrics are 0.0 when either side has zero unique URIs."""
        # Arrange
        # Case 1: No engagement URIs, some treatment URIs -> both metrics should be 0.0
        empty_engagement_feed = FeedWithMetadata(
            feed=[],
            bluesky_handle="user_eng_empty",
            user_did="did:eng:empty",
            condition="engagement",
            feed_statistics="{}",
        )
        treatment_feed = FeedWithMetadata(
            feed=[
                CustomFeedPost(item="x", is_in_network=True),
                CustomFeedPost(item="y", is_in_network=False),
            ],
            bluesky_handle="user_treat",
            user_did="did:treat",
            condition="representative_diversification",
            feed_statistics="{}",
        )

        user_to_ranked_feed_map = {
            "user_eng_empty": empty_engagement_feed,
            "user_treat": treatment_feed,
        }
        service = FeedGenerationSessionAnalyticsService()

        # Act
        result = service.calculate_feed_generation_session_analytics(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            session_timestamp="2024-01-01T00:00:00",
        )

        # Assert
        assert result.total_unique_engagement_uris == 0
        assert result.total_unique_treatment_uris == 2
        assert result.prop_overlap_treatment_uris_in_engagement_uris == 0.0
        assert result.prop_overlap_engagement_uris_in_treatment_uris == 0.0

        # Case 2: Engagement URIs exist, no treatment URIs -> both metrics should be 0.0
        engagement_feed = FeedWithMetadata(
            feed=[
                CustomFeedPost(item="a", is_in_network=True),
                CustomFeedPost(item="b", is_in_network=False),
            ],
            bluesky_handle="user_eng",
            user_did="did:eng",
            condition="engagement",
            feed_statistics="{}",
        )
        empty_treatment_feed = FeedWithMetadata(
            feed=[],
            bluesky_handle="user_treat_empty",
            user_did="did:treat:empty",
            condition="representative_diversification",
            feed_statistics="{}",
        )
        user_to_ranked_feed_map = {
            "user_eng": engagement_feed,
            "user_treat_empty": empty_treatment_feed,
        }

        # Act
        result = service.calculate_feed_generation_session_analytics(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            session_timestamp="2024-01-01T00:00:01",
        )

        # Assert
        assert result.total_unique_engagement_uris == 2
        assert result.total_unique_treatment_uris == 0
        assert result.prop_overlap_treatment_uris_in_engagement_uris == 0.0
        assert result.prop_overlap_engagement_uris_in_treatment_uris == 0.0
