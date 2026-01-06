"""Tests for FeedGenerationService."""

from unittest.mock import Mock

import pandas as pd
import pytest

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    CandidatePostPools,
    CustomFeedPost,
    FeedWithMetadata,
    LatestFeeds,
)
from services.rank_score_feeds.services.feed import FeedGenerationService

from services.participant_data.models import UserToBlueskyProfileModel


class TestFeedGenerationService:
    """Tests for FeedGenerationService."""

    @pytest.fixture
    def feed_config(self) -> FeedConfig:
        """Create a minimal FeedConfig for tests."""
        return FeedConfig(max_feed_length=3, max_in_network_posts_ratio=0.6)

    @pytest.fixture
    def ranking_service(self) -> Mock:
        """Mock RankingService."""
        mock = Mock()
        mock.create_ranked_candidate_feed.return_value = [
            CustomFeedPost(item="ranked1", is_in_network=True)
        ]
        return mock

    @pytest.fixture
    def reranking_service(self) -> Mock:
        """Mock RerankingService."""
        mock = Mock()
        mock.rerank_feed.return_value = [
            CustomFeedPost(item="reranked1", is_in_network=False)
        ]
        return mock

    @pytest.fixture
    def feed_statistics_service(self) -> Mock:
        """Mock FeedStatisticsService."""
        mock = Mock()
        mock.generate_feed_statistics.return_value = "{}"
        return mock

    @pytest.fixture
    def service(
        self,
        ranking_service: Mock,
        reranking_service: Mock,
        feed_statistics_service: Mock,
        feed_config: FeedConfig,
    ) -> FeedGenerationService:
        """Create FeedGenerationService with mocks."""
        return FeedGenerationService(
            ranking_service=ranking_service,
            reranking_service=reranking_service,
            feed_statistics_service=feed_statistics_service,
            feed_config=feed_config,
        )

    @pytest.fixture
    def candidate_pools(self) -> CandidatePostPools:
        """Create candidate pools with distinct DataFrames."""
        return CandidatePostPools(
            reverse_chronological=pd.DataFrame({"uri": ["rc1"]}),
            engagement=pd.DataFrame({"uri": ["e1"]}),
            treatment=pd.DataFrame({"uri": ["t1"]}),
        )

    @pytest.fixture
    def study_user_engagement(self) -> UserToBlueskyProfileModel:
        """Create a study user for engagement condition."""
        return UserToBlueskyProfileModel(
            study_user_id="u1",
            is_study_user=True,
            created_timestamp="2024-01-01T00:00:00",
            bluesky_handle="handle_u1",
            bluesky_user_did="did:u1",
            condition="engagement",
        )

    def test_generate_feeds_for_users_orchestrates_services_and_builds_map(
        self,
        service: FeedGenerationService,
        ranking_service: Mock,
        reranking_service: Mock,
        feed_statistics_service: Mock,
        candidate_pools: CandidatePostPools,
        study_user_engagement: UserToBlueskyProfileModel,
    ):
        """Ensure end-to-end orchestration and outputs are correct, including default feed."""
        # Arrange
        user_to_in_network_post_uris_map = {"did:u1": ["p1", "p2"]}
        study_users = [study_user_engagement]
        previous_feeds = LatestFeeds(feeds={"handle_u1": {"old1"}, "default": {"d1"}})

        # Act
        result = service.generate_feeds_for_users(
            user_to_in_network_post_uris_map=user_to_in_network_post_uris_map,
            candidate_post_pools=candidate_pools,
            study_users=study_users,
            previous_feeds=previous_feeds,
        )

        # Assert
        assert isinstance(result, dict)
        assert "did:u1" in result  # keyed by user_did
        assert "default" in result
        assert isinstance(result["did:u1"], FeedWithMetadata)
        assert isinstance(result["default"], FeedWithMetadata)

        # Ranking called for user feed and default feed
        assert ranking_service.create_ranked_candidate_feed.call_count == 2
        # Stats generated twice (user + default)
        assert feed_statistics_service.generate_feed_statistics.call_count == 2
        # Reranking called twice (user + default)
        assert reranking_service.rerank_feed.call_count == 2

        # Verify ranking call for user includes correct condition and candidate pool
        ranking_service.create_ranked_candidate_feed.assert_any_call(
            condition="engagement",
            in_network_candidate_post_uris=["p1", "p2"],
            post_pool=candidate_pools.engagement,
            max_feed_length=service.config.max_feed_length,
            max_in_network_posts_ratio=service.config.max_in_network_posts_ratio,
        )
        # Verify ranking call for default feed uses reverse_chronological
        ranking_service.create_ranked_candidate_feed.assert_any_call(
            condition="reverse_chronological",
            in_network_candidate_post_uris=[],
            post_pool=candidate_pools.reverse_chronological,
            max_feed_length=service.config.max_feed_length,
            max_in_network_posts_ratio=service.config.max_in_network_posts_ratio,
        )

    def test_select_candidate_pool_for_each_condition(
        self,
        service: FeedGenerationService,
        candidate_pools: CandidatePostPools,
        study_user_engagement: UserToBlueskyProfileModel,
    ):
        """Verify correct candidate pool is returned for each condition."""
        # Arrange
        user_eng = study_user_engagement
        user_rev = UserToBlueskyProfileModel(
            study_user_id="u2",
            is_study_user=True,
            created_timestamp="2024-01-01T00:00:00",
            bluesky_handle="handle_u2",
            bluesky_user_did="did:u2",
            condition="reverse_chronological",
        )
        user_treat = UserToBlueskyProfileModel(
            study_user_id="u3",
            is_study_user=True,
            created_timestamp="2024-01-01T00:00:00",
            bluesky_handle="handle_u3",
            bluesky_user_did="did:u3",
            condition="representative_diversification",
        )

        # Act
        pool_eng = service._select_candidate_pool_for_feed_generation(
            candidate_post_pools=candidate_pools, user=user_eng
        )
        pool_rev = service._select_candidate_pool_for_feed_generation(
            candidate_post_pools=candidate_pools, user=user_rev
        )
        pool_treat = service._select_candidate_pool_for_feed_generation(
            candidate_post_pools=candidate_pools, user=user_treat
        )

        # Assert
        assert pool_eng.equals(candidate_pools.engagement)
        assert pool_rev.equals(candidate_pools.reverse_chronological)
        assert pool_treat.equals(candidate_pools.treatment)

    def test_select_candidate_pool_raises_for_empty_pool(
        self, service: FeedGenerationService
    ):
        """Raise ValueError when selected candidate pool is empty."""
        # Arrange
        empty_pools = CandidatePostPools(
            reverse_chronological=pd.DataFrame({"uri": []}),
            engagement=pd.DataFrame({"uri": []}),
            treatment=pd.DataFrame({"uri": []}),
        )
        user_rev = UserToBlueskyProfileModel(
            study_user_id="u4",
            is_study_user=True,
            created_timestamp="2024-01-01T00:00:00",
            bluesky_handle="handle_u4",
            bluesky_user_did="did:u4",
            condition="reverse_chronological",
        )

        # Act & Assert
        with pytest.raises(ValueError, match="post_pool cannot be None"):
            service._select_candidate_pool_for_feed_generation(
                candidate_post_pools=empty_pools, user=user_rev
            )

    def test_generate_feeds_for_users_raises_for_invalid_condition(
        self,
        service: FeedGenerationService,
        candidate_pools: CandidatePostPools,
    ):
        """Raise ValueError when user condition is invalid (post_pool None)."""
        # Arrange
        # Use model_construct to bypass Pydantic validation and test service's
        # ValueError handling for invalid conditions that bypass model validation
        bad_user = UserToBlueskyProfileModel.model_construct(
            study_user_id="u5",
            is_study_user=True,
            created_timestamp="2024-01-01T00:00:00",
            bluesky_handle="handle_u5",
            bluesky_user_did="did:u5",
            condition="unknown_condition",
        )
        user_to_in_network_post_uris_map = {"did:u5": []}
        previous_feeds = LatestFeeds(feeds={})

        # Act & Assert
        with pytest.raises(ValueError, match="post_pool cannot be None"):
            service.generate_feeds_for_users(
                user_to_in_network_post_uris_map=user_to_in_network_post_uris_map,
                candidate_post_pools=candidate_pools,
                study_users=[bad_user],
                previous_feeds=previous_feeds,
            )

    def test_generate_default_feed_uses_previous_default_uris_in_rerank(
        self,
        service: FeedGenerationService,
        ranking_service: Mock,
        reranking_service: Mock,
        candidate_pools: CandidatePostPools,
    ):
        """Default feed path uses reverse_chronological and passes previous default URIs to rerank."""
        # Arrange
        previous_default_uris = {"default_old_uri"}
        previous_feeds = LatestFeeds(feeds={"default": previous_default_uris})

        # Act
        result = service.generate_feeds_for_users(
            user_to_in_network_post_uris_map={},
            candidate_post_pools=candidate_pools,
            study_users=[],
            previous_feeds=previous_feeds,
        )

        # Assert
        assert "default" in result
        reranking_service.rerank_feed.assert_called_once()
        # Ensure reranking received the expected default previous URIs
        _, kwargs = reranking_service.rerank_feed.call_args
        assert kwargs["uris_of_posts_used_in_previous_feeds"] == previous_default_uris

    def test_generate_feed_for_user_logs_error_on_empty_reranked_feed(
        self,
        service: FeedGenerationService,
        ranking_service: Mock,
        reranking_service: Mock,
        study_user_engagement: UserToBlueskyProfileModel,
    ):
        """When reranked feed is empty, an error is logged."""
        # Arrange
        reranking_service.rerank_feed.return_value = []
        service.logger = Mock()
        candidate_pool = pd.DataFrame({"uri": ["e1"]})

        # Act
        result = service._generate_feed_for_user(
            user=study_user_engagement,
            candidate_pool=candidate_pool,
            in_network_post_uris=["p1"],
            uris_of_posts_used_in_previous_feeds=set(),
        )

        # Assert
        assert result == []
        service.logger.error.assert_called_once()
