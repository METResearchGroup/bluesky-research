"""Service for orchestrating feed generation."""

import pandas as pd

from lib.log.logger import get_logger
from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    FeedWithMetadata,
    UserInNetworkPostsMap,
    CandidatePostPools,
    LatestFeeds,
    CustomFeedPost,
)
from services.rank_score_feeds.services.feed_statistics import FeedStatisticsService
from services.rank_score_feeds.services.ranking import RankingService
from services.rank_score_feeds.services.reranking import RerankingService


class FeedGenerationService:
    """Orchestrates feed generation for individual users."""

    def __init__(
        self,
        ranking_service: RankingService,
        reranking_service: RerankingService,
        feed_statistics_service: FeedStatisticsService,
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
        self.feed_statistics_service = feed_statistics_service
        self.config = feed_config
        self.logger = get_logger(__name__)

    def generate_feeds_for_users(
        self,
        user_to_in_network_post_uris_map: UserInNetworkPostsMap,
        candidate_post_pools: CandidatePostPools,
        study_users: list[UserToBlueskyProfileModel],
        previous_feeds: LatestFeeds,
    ) -> dict[str, FeedWithMetadata]:
        """Generate feeds for all users.

        Args:
            user_to_in_network_post_uris_map: Mapping of user DIDs to in-network post URIs.
            candidate_post_pools: Post pools for ranking.

        Returns:
            Dictionary mapping user DIDs to feeds.
        """
        user_to_ranked_feed_map: dict[str, FeedWithMetadata] = {}
        total_study_users = len(study_users)

        for i, user in enumerate(study_users):
            if i % 10 == 0:
                self.logger.info(f"Creating feed for user {i}/{total_study_users}")
            candidate_pool: pd.DataFrame = (
                self._select_candidate_pool_for_feed_generation(
                    candidate_post_pools=candidate_post_pools,
                    user=user,
                )
            )
            in_network_post_uris: list[str] = user_to_in_network_post_uris_map[
                user.bluesky_user_did
            ]
            uris_of_posts_used_in_previous_feeds: set[str] = previous_feeds.get(
                user.bluesky_handle, set()
            )

            feed: list[CustomFeedPost] = self._generate_feed_for_user(
                user=user,
                candidate_pool=candidate_pool,
                in_network_post_uris=in_network_post_uris,
                uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
            )

            feed_statistics: str = (
                self.feed_statistics_service.generate_feed_statistics(feed=feed)
            )

            feed_with_metadata: FeedWithMetadata = self._generate_feed_with_metadata(
                feed=feed,
                user=user,
                feed_statistics=feed_statistics,
            )

            user_to_ranked_feed_map[user.bluesky_user_did] = feed_with_metadata

        # generate default feed with metadata, for users who log into the feed
        # but who aren't in the study.
        default_feed_with_metadata: FeedWithMetadata = (
            self._generate_default_feed_with_metadata(
                candidate_post_pools=candidate_post_pools,
                uris_of_posts_used_in_previous_feeds=previous_feeds.get(
                    "default", set()
                ),
            )
        )
        user_to_ranked_feed_map["default"] = default_feed_with_metadata

        return user_to_ranked_feed_map

    def _generate_feed_with_metadata(
        self,
        feed: list[CustomFeedPost],
        user: UserToBlueskyProfileModel,
        feed_statistics: str,
    ) -> FeedWithMetadata:
        """Generate a feed with metadata."""
        return FeedWithMetadata(
            feed=feed,
            bluesky_handle=user.bluesky_handle,
            user_did=user.bluesky_user_did,
            condition=user.condition,
            feed_statistics=feed_statistics,
        )

    def _generate_feed_for_user(
        self,
        user: UserToBlueskyProfileModel,
        candidate_pool: pd.DataFrame,
        in_network_post_uris: list[str],
        uris_of_posts_used_in_previous_feeds: set[str],
    ) -> list[CustomFeedPost]:
        """Generate complete feed: ranking → reranking → statistics.

        Args:
            user: User to generate feed for.
            candidate_pool: Candidate pool to rank from.
            in_network_post_uris: In-network post URIs for this user.
            uris_of_posts_used_in_previous_feeds: Previous feed URIs for freshness filtering.

        Returns:
            Dictionary containing feed and metadata.
        """
        candidate_feed: list[CustomFeedPost] = (
            self.ranking_service.create_ranked_candidate_feed(
                condition=user.condition,
                in_network_candidate_post_uris=in_network_post_uris,
                post_pool=candidate_pool,
                max_feed_length=self.config.max_feed_length,
                max_in_network_posts_ratio=self.config.max_in_network_posts_ratio,
            )
        )
        reranked_feed: list[CustomFeedPost] = self.reranking_service.rerank_feed(
            feed=candidate_feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )
        if len(reranked_feed) == 0:
            self.logger.error(
                f"No feed created for user {user.bluesky_user_did}. This shouldn't happen..."
            )
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

    def _generate_default_feed_with_metadata(
        self,
        candidate_post_pools: CandidatePostPools,
        uris_of_posts_used_in_previous_feeds: set[str],
    ) -> FeedWithMetadata:
        candidate_pool: pd.DataFrame = candidate_post_pools.reverse_chronological
        default_feed: list[CustomFeedPost] = self._generate_default_feed(
            candidate_pool=candidate_pool,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )
        default_feed_statistics: str = (
            self.feed_statistics_service.generate_feed_statistics(feed=default_feed)
        )
        default_study_user = UserToBlueskyProfileModel(
            study_user_id="default",
            is_study_user=False,
            created_timestamp="default",
            bluesky_handle="default",
            bluesky_user_did="default",
            condition="reverse_chronological",
        )
        default_feed_with_metadata = self._generate_feed_with_metadata(
            feed=default_feed,
            user=default_study_user,
            feed_statistics=default_feed_statistics,
        )
        return default_feed_with_metadata

    def _generate_default_feed(
        self,
        candidate_pool: pd.DataFrame,
        uris_of_posts_used_in_previous_feeds: set[str],
    ) -> list[CustomFeedPost]:
        """Generate a default feed for users that aren't logged in or for if a user
        isn't in the study but opens the link.

        The default feed consists of posts from the reverse-chronological condition
        (i.e., from the firehose) without personalization.
        """
        candidate_feed: list[CustomFeedPost] = (
            self.ranking_service.create_ranked_candidate_feed(
                condition="reverse_chronological",
                in_network_candidate_post_uris=[],
                post_pool=candidate_pool,
                max_feed_length=self.config.max_feed_length,
                max_in_network_posts_ratio=self.config.max_in_network_posts_ratio,
            )
        )
        reranked_feed: list[CustomFeedPost] = self.reranking_service.rerank_feed(
            feed=candidate_feed,
            uris_of_posts_used_in_previous_feeds=uris_of_posts_used_in_previous_feeds,
        )
        if len(reranked_feed) == 0:
            self.logger.error(
                "No feed created for default user. This shouldn't happen..."
            )
        return reranked_feed
