"""Service for orchestrating feed generation."""

import pandas as pd

from lib.log.logger import get_logger
from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import UserInNetworkPostsMap, CandidatePostPools, LatestFeeds, CustomFeedPost
from services.rank_score_feeds.services.ranking import RankingService
from services.rank_score_feeds.services.reranking import RerankingService


class FeedGenerationService:
    """Orchestrates feed generation for individual users."""

    def __init__(
        self,
        ranking_service: RankingService,
        reranking_service: RerankingService,
        feed_config: FeedConfig,
    ):
        """Initialize with ranking/reranking services and config.
        
        Args:
            ranking_service: Service for initial ranking.
            reranking_service: Service for re-ranking with business rules.
            feed_config: Configuration for feed generation algorithm.
        """
        self.ranking_service = ranking_service
        self.reranking_service = reranking_service
        self.config = feed_config
        self.logger = get_logger(__name__)

    def generate_feeds_for_users(
        self,
        user_to_in_network_post_uris_map: UserInNetworkPostsMap,
        candidate_post_pools: CandidatePostPools,
        study_users: list[UserToBlueskyProfileModel],
        previous_feeds: LatestFeeds
    ) -> dict[str, dict]:
        """Generate feeds for all users.
        
        Args:
            user_to_in_network_post_uris_map: Mapping of user DIDs to in-network post URIs.
            candidate_post_pools: Post pools for ranking.
        
        Returns:
            Dictionary mapping user DIDs to feeds.
        """
        # TODO: remove this later. Janky. At the very least, add better typing.
        user_to_ranked_feed_map: dict[str, dict] = {}
        total_study_users = len(study_users)

        for i, user in enumerate(study_users):
            if i % 10 == 0:
                self.logger.info(f"Creating feed for user {i}/{total_study_users}")
            candidate_pool: pd.DataFrame = self._select_candidate_pool_for_feed_generation(
                candidate_post_pools=candidate_post_pools,
                user=user,
            )
            in_network_post_uris: list[str] = user_to_in_network_post_uris_map[user.bluesky_user_did]
            uris_of_posts_used_in_previous_feeds: set[str] = previous_feeds.get(user.bluesky_handle, set())
            feed = self._generate_feed_for_user(
                user=user,
                candidate_pool=candidate_pool,
                in_network_post_uris=in_network_post_uris,
                uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
            )

            # TODO: should calculate feed metadata after feed generation.

            user_to_ranked_feed_map[user.bluesky_user_did] = feed

        # TODO: generate default feed.

        return user_to_ranked_feed_map

    def _generate_default_feed(self) -> dict:
        return {}

    def _generate_feed_for_user(
        self,
        user: UserToBlueskyProfileModel,
        candidate_pool: pd.DataFrame,
        in_network_post_uris: list[str],
        uris_of_posts_used_in_previous_feeds: set[str],
    ) -> dict:
        """Generate complete feed: ranking → reranking → statistics.
        
        Args:
            user: User to generate feed for.
            candidate_pool: Candidate pool to rank from.
            in_network_post_uris: In-network post URIs for this user.
            uris_of_posts_used_in_previous_feeds: Previous feed URIs for freshness filtering.
        
        Returns:
            Dictionary containing feed and metadata.
        """
        candidate_feed: list[CustomFeedPost] = self.ranking_service.create_ranked_candidate_feed(
            condition=user.condition,
            in_network_candidate_post_uris=in_network_post_uris,
            post_pool=candidate_pool,
            max_feed_length=self.config.max_feed_length,
            max_in_network_posts_ratio=self.config.max_in_network_posts_ratio,
        )
        reranked_feed: list[CustomFeedPost] = self.reranking_service.rerank_feed(
            feed=candidate_feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )
        # TODO: update what's returned here.
        return reranked_feed
    
    def _select_candidate_pool_for_feed_generation(
        self,
        candidate_post_pools: CandidatePostPools,
        user: UserToBlueskyProfileModel,
    ) -> pd.DataFrame:
        """Select the appropriate candidate pool for feed generation.
        
        Args:
            candidate_post_pools: Post pools for ranking.
            user: User to generate feed for.
        
        Returns:
            Candidate pool for feed generation.
        """
        post_pool: pd.DataFrame | None = (
            candidate_post_pools.reverse_chronological
            if user.condition == "reverse_chronological"
            else candidate_post_pools.engagement
            if user.condition == "engagement"
            else candidate_post_pools.treatment
            if user.condition == "representative_diversification"
            else None
        )
        if post_pool is None or len(post_pool) == 0:
            raise ValueError(
                "post_pool cannot be None. This means that a user condition is unexpected/invalid"
            )
        return post_pool
