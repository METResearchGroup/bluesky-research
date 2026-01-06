"""Orchestrator for feed generation pipeline.

This module provides the FeedGenerationOrchestrator class that wires together
all the components of the feed generation process using dependency injection.
"""

import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    LoadedData,
    CandidatePostPools,
    UserInNetworkPostsMap,
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
)
from services.rank_score_feeds.storage.exceptions import StorageError

logger = get_logger(__name__)


class FeedGenerationOrchestrator:
    """Orchestrates the feed generation pipeline.

    This class wires together all dependencies and coordinates the execution
    of the feed generation process. It serves as the composition root for
    dependency injection.
    """

    def __init__(self, feed_config: FeedConfig):
        """Initialize the orchestrator with configuration and dependencies.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        from services.rank_score_feeds.repositories.feed_repo import (
            FeedStorageRepository,
        )
        from services.rank_score_feeds.repositories.scores_repo import ScoresRepository
        from services.rank_score_feeds.services.candidate import (
            CandidateGenerationService,
        )
        from services.rank_score_feeds.services.context import UserContextService
        from services.rank_score_feeds.services.data_loading import FeedDataLoader
        from services.rank_score_feeds.services.export import DataExporterService
        from services.rank_score_feeds.services.feed import FeedGenerationService
        from services.rank_score_feeds.services.feed_statistics import (
            FeedStatisticsService,
        )
        from services.rank_score_feeds.services.feed_generation_session_analytics import (
            FeedGenerationSessionAnalyticsService,
        )
        from services.rank_score_feeds.services.ranking import RankingService
        from services.rank_score_feeds.services.reranking import RerankingService
        from services.rank_score_feeds.services.scoring import ScoringService
        from services.rank_score_feeds.storage.adapters import (
            S3FeedStorageAdapter,
            S3FeedTTLAdapter,
            DynamoDBSessionMetadataAdapter,
        )

        self.config = feed_config
        self.logger = logger

        # Data loading facade
        self.feed_data_loader = FeedDataLoader(feed_config=feed_config)

        # Scoring service
        scores_repo = ScoresRepository(feed_config=feed_config)
        self.scoring_service = ScoringService(
            scores_repo=scores_repo,
            feed_config=feed_config,
        )

        # Feed generation services
        self.candidate_service = CandidateGenerationService(feed_config=feed_config)
        self.context_service = UserContextService()
        self.ranking_service = RankingService(feed_config=feed_config)
        self.reranking_service = RerankingService(feed_config=feed_config)
        self.feed_statistics_service = FeedStatisticsService()
        self.feed_service = FeedGenerationService(
            ranking_service=self.ranking_service,
            reranking_service=self.reranking_service,
            feed_statistics_service=self.feed_statistics_service,
            feed_config=feed_config,
        )
        self.feed_generation_session_analytics_service = (
            FeedGenerationSessionAnalyticsService()
        )

        # Export services
        export_s3_adapter = S3FeedStorageAdapter()
        export_storage_repository = FeedStorageRepository(adapter=export_s3_adapter)
        self.data_exporter_service = DataExporterService(
            feed_storage_repository=export_storage_repository
        )

        # TTL services
        self.feed_ttl_adapter = S3FeedTTLAdapter()
        self.session_metadata_adapter = DynamoDBSessionMetadataAdapter()

    def run(
        self,
        users_to_create_feeds_for: list[str] | None = None,
        export_new_scores: bool = True,
        test_mode: bool = False,
    ) -> None:
        """Execute the feed generation pipeline.

        Args:
            users_to_create_feeds_for: Optional list of user handles to create
                feeds for. If None, creates feeds for all users.
            export_new_scores: If True, export new scores.
            test_mode: If True, run in test mode (limited users, no TTL).

        """
        # Generate fresh timestamp for this run
        self.current_datetime_str: str = generate_current_datetime_str()

        self.logger.info("Starting rank score feeds.")

        # Step 1: Load all data
        loaded_data = self._load_data(
            test_mode=test_mode, users_to_create_feeds_for=users_to_create_feeds_for
        )

        # Step 2: Score posts (export is handled by ScoringService unless skipped)
        posts_df_with_scores: pd.DataFrame = self._score_posts(
            loaded_data, export_new_scores=export_new_scores
        )

        # Step 4: Build candidate post pools.
        candidate_post_pools: CandidatePostPools = self._generate_candidate_pools(
            posts_df_with_scores
        )

        # Step 5: Generate feeds
        user_to_ranked_feed_map: dict[str, FeedWithMetadata] = self._generate_feeds(
            loaded_data=loaded_data,
            candidate_post_pools=candidate_post_pools,
        )

        # Step 6: calculate analytics for the current session of feed generation.
        feed_generation_session_analytics: FeedGenerationSessionAnalytics = (
            self._calculate_feed_generation_session_analytics(
                user_to_ranked_feed_map=user_to_ranked_feed_map
            )
        )

        # Step 7: export artifacts.
        self._export_artifacts(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            feed_generation_session_analytics=feed_generation_session_analytics,
        )

        if not test_mode:
            # Step 8: TTL old feeds.
            self._ttl_old_feeds()

            # Step 9: Insert feed generation session metadata.
            self._insert_feed_generation_session_metadata(
                feed_generation_session_analytics=feed_generation_session_analytics
            )

    def _score_posts(
        self, loaded_data: LoadedData, export_new_scores: bool = True
    ) -> pd.DataFrame:
        """Calculate scores for all posts.

        Args:
            loaded_data: Loaded input data.
            export_new_scores: If True, export new scores to storage.

        Returns:
            DataFrame with scored posts.
        """
        return self.scoring_service.score_posts(
            posts_df=loaded_data.posts_df,
            superposter_dids=loaded_data.superposter_dids,
            export_new_scores=export_new_scores,
        )

    def _generate_candidate_pools(self, posts_df: pd.DataFrame) -> CandidatePostPools:
        return self.candidate_service.generate_candidate_pools(posts_df=posts_df)

    def _generate_feeds(
        self,
        loaded_data: LoadedData,
        candidate_post_pools: CandidatePostPools,
    ) -> dict[str, FeedWithMetadata]:
        """Manages generating feeds for all users.

        Args:
            loaded_data: Loaded input data.
            candidate_post_pools: Post pools for ranking.

        Returns:
            Dictionary mapping user DIDs to feeds.
        """
        # step 1: calculate in-network posts, per user.
        user_to_in_network_post_uris_map: UserInNetworkPostsMap = (
            self.context_service.build_in_network_context(
                scored_posts=loaded_data.posts_df,
                study_users=loaded_data.study_users,
                user_to_social_network_map=loaded_data.user_to_social_network_map,
            )
        )

        # step 2: generate feeds.
        user_to_ranked_feed_map: dict[str, FeedWithMetadata] = (
            self.feed_service.generate_feeds_for_users(
                user_to_in_network_post_uris_map=user_to_in_network_post_uris_map,
                candidate_post_pools=candidate_post_pools,
                study_users=loaded_data.study_users,
                previous_feeds=loaded_data.previous_feeds,
            )
        )

        return user_to_ranked_feed_map

    def _load_data(
        self,
        test_mode: bool = False,
        users_to_create_feeds_for: list[str] | None = None,
    ) -> LoadedData:
        """Load and prepare data for feed generation.

        Args:
            test_mode: If True, filter to test users only when loading study users.
            users_to_create_feeds_for: Optional list of user handles to create
                feeds for. If None, creates feeds for all users.

        Returns:
            LoadedData with filtered and deduplicated posts.
        """
        # Load all data
        loaded_data = self.feed_data_loader.load_complete_data(
            test_mode=test_mode, users_to_create_feeds_for=users_to_create_feeds_for
        )

        # Deduplicate and filter posts
        loaded_data.posts_df = self.feed_data_loader.deduplicate_and_filter_posts(
            loaded_data.posts_df
        )

        return loaded_data

    def _calculate_feed_generation_session_analytics(
        self, user_to_ranked_feed_map: dict[str, FeedWithMetadata]
    ) -> FeedGenerationSessionAnalytics:
        """Calculates feed generation session analytics."""
        feed_generation_session_analytics: FeedGenerationSessionAnalytics = self.feed_generation_session_analytics_service.calculate_feed_generation_session_analytics(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            session_timestamp=self.current_datetime_str,
        )
        return feed_generation_session_analytics

    def _export_artifacts(
        self,
        user_to_ranked_feed_map: dict[str, FeedWithMetadata],
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
    ) -> None:
        self.data_exporter_service.export_feeds(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            timestamp=self.current_datetime_str,
        )
        self.data_exporter_service.export_feed_generation_session_analytics(
            feed_generation_session_analytics=feed_generation_session_analytics,
            timestamp=self.current_datetime_str,
        )

    def _ttl_old_feeds(self) -> None:
        """Move old feeds to cache. Keeps the custom_feeds/active/ directory clean."""
        try:
            self.feed_ttl_adapter.move_to_cache(
                prefix="custom_feeds",
                keep_count=self.config.keep_count,
                sort_field="Key",
            )
        except Exception as e:
            self.logger.error(f"Failed to TTL old feeds: {e}")
            raise StorageError(f"Failed to TTL old feeds: {e}") from e

    def _insert_feed_generation_session_metadata(
        self, feed_generation_session_analytics: FeedGenerationSessionAnalytics
    ) -> None:
        """Insert feed generation session metadata into DynamoDB."""
        try:
            self.session_metadata_adapter.insert_session_metadata(
                metadata=feed_generation_session_analytics
            )
        except Exception as e:
            self.logger.error(f"Failed to insert feed generation session metadata: {e}")
            raise StorageError(
                f"Failed to insert feed generation session metadata: {e}"
            ) from e
