"""Orchestrator for feed generation pipeline.

This module provides the FeedGenerationOrchestrator class that wires together
all the components of the feed generation process using dependency injection.
"""

import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    load_users_to_exclude,
)
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.helper import (
    export_results,
    insert_feed_generation_session,
    load_feed_input_data,
    load_latest_feeds,
)
from services.rank_score_feeds.models import (
    FeedInputData,
    LoadedData,
    CandidatePostPools,
    RawFeedData,
    LatestFeeds,
    UserInNetworkPostsMap,
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
)

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
        from lib.aws.athena import Athena
        from lib.aws.dynamodb import DynamoDB
        from lib.aws.glue import Glue
        from lib.aws.s3 import S3

        from services.rank_score_feeds.repositories.feed_repo import FeedStorageRepository
        from services.rank_score_feeds.repositories.scores_repo import ScoresRepository
        from services.rank_score_feeds.services.candidate import (
            CandidateGenerationService,
        )
        from services.rank_score_feeds.services.context import UserContextService
        from services.rank_score_feeds.services.export import DataExporterService
        from services.rank_score_feeds.services.feed import FeedGenerationService
        from services.rank_score_feeds.services.feed_statistics import (
            FeedStatisticsService,
        )
        from services.rank_score_feeds.services.feed_generation_session_analytics import FeedGenerationSessionAnalyticsService
        from services.rank_score_feeds.services.ranking import RankingService
        from services.rank_score_feeds.services.reranking import RerankingService
        from services.rank_score_feeds.services.scoring import ScoringService
        from services.rank_score_feeds.storage.adapters import S3FeedStorageAdapter

        self.config = feed_config
        self.athena = Athena()
        self.s3 = S3()
        self.dynamodb = DynamoDB()
        self.glue = Glue()
        self.logger = logger

        self.current_datetime_str: str = generate_current_datetime_str()

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
        self.feed_generation_session_analytics_service = FeedGenerationSessionAnalyticsService()

        # Export services
        export_s3_adapter = S3FeedStorageAdapter()
        export_storage_repository = FeedStorageRepository(adapter=export_s3_adapter)
        self.data_exporter_service = DataExporterService(feed_storage_repository=export_storage_repository)

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
        self.logger.info("Starting rank score feeds.")

        # Step 1: Load all data
        loaded_data = self._load_data(test_mode, users_to_create_feeds_for)

        # Step 2: Deduplicate and filter posts
        loaded_data.posts_df = self._deduplicate_and_filter_posts(loaded_data.posts_df)

        # Step 3: Score posts (export is handled by ScoringService unless skipped)
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

        # Step 8: TTL old feeds.

        # Step 9: Insert feed generation session metadata.


        # Step 6: Export results
        # self._export_results(run_result, test_mode)


    def _load_raw_data(
        self,
        test_mode: bool,
    ) -> RawFeedData:
        """Load raw data from all sources.

        Loads data from all services without any transformation or filtering.
        This is the pure data loading step.

        Args:
            test_mode: If True, filter to test users only when loading study users.

        Returns:
            RawFeedData containing:
                - study_users: All study users (potentially filtered by test_mode)
                - feed_input_data: Feed input data from multiple services
                - latest_feeds: Previous feeds per user handle
        """
        study_users: list[UserToBlueskyProfileModel] = get_all_users(
            test_mode=test_mode
        )
        feed_input_data: FeedInputData = load_feed_input_data()
        latest_feeds: LatestFeeds = load_latest_feeds()

        return RawFeedData(
            study_users=study_users,
            feed_input_data=feed_input_data,
            latest_feeds=latest_feeds,
        )

    def _filter_study_users(
        self,
        study_users: list[UserToBlueskyProfileModel],
        users_to_create_feeds_for: list[str] | None,
    ) -> list[UserToBlueskyProfileModel]:
        """Filter study users to only those specified by handles.

        Args:
            study_users: List of all study users to filter from.
            users_to_create_feeds_for: Optional list of user handles to filter to.
                If None, returns all study users unchanged.

        Returns:
            Filtered list of study users matching the provided handles.
        """
        if not users_to_create_feeds_for:
            return study_users

        self.logger.info(
            f"Filtering to {len(users_to_create_feeds_for)} specified users."
        )
        return [
            user
            for user in study_users
            if user.bluesky_handle in users_to_create_feeds_for
        ]

    def _load_data(
        self,
        test_mode: bool,
        users_to_create_feeds_for: list[str] | None,
    ) -> LoadedData:
        """Load and transform all required input data.

        Loads raw data from all sources, transforms it to the required format,
        and applies user filtering if specified.

        Args:
            test_mode: If True, filter to test users only.
            users_to_create_feeds_for: Optional list of user handles to filter to.

        Returns:
            LoadedData containing all loaded and transformed inputs.
        """
        # Load raw data
        raw_data: RawFeedData = self._load_raw_data(test_mode)

        # Filter users if specified
        filtered_study_users = self._filter_study_users(
            raw_data.study_users,
            users_to_create_feeds_for,
        )

        return LoadedData(
            posts_df=raw_data.feed_input_data.consolidate_enrichment_integrations,
            user_to_social_network_map=raw_data.feed_input_data.scraped_user_social_network,
            superposter_dids=raw_data.feed_input_data.superposters,
            previous_feeds=raw_data.latest_feeds,
            study_users=filtered_study_users,
        )

    def _deduplicate_and_filter_posts(
        self, consolidated_enriched_posts_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Deduplicates and filters posts.

        Performs two operations:
        1. Deduplicates posts by URI, keeping the most recent consolidation_timestamp
        2. Filters out posts from excluded authors (by DID or handle)
        """
        # Deduplication based on unique URIs, keeping the most recent consolidation_timestamp
        len_before = consolidated_enriched_posts_df.shape[0]
        deduplicated_df: pd.DataFrame = consolidated_enriched_posts_df.sort_values(
            by="consolidation_timestamp", ascending=False
        ).drop_duplicates(subset="uri", keep="first")
        len_after = deduplicated_df.shape[0]

        if len_before != len_after:
            logger.info(f"Deduplicated posts from {len_before} to {len_after}.")

        # filter out excluded authors.
        users_to_exclude: dict[str, set[str]] = load_users_to_exclude()
        bsky_handles_to_exclude: set[str] = users_to_exclude["bsky_handles_to_exclude"]
        bsky_dids_to_exclude: set[str] = users_to_exclude["bsky_dids_to_exclude"]

        # keep posts where authors are NOT in the excludelists.
        # Convert sets to lists for pandas isin() compatibility
        mask: pd.Series = ~(
            deduplicated_df["author_did"].isin(
                list(bsky_dids_to_exclude)
            )  # convert set to list for pyright check; performance diff negligible.
            | deduplicated_df["author_handle"].isin(list(bsky_handles_to_exclude))
        )
        filtered_df: pd.DataFrame = deduplicated_df[mask]  # type: ignore[assignment] # pyright: ignore[reportUnknownReturnType]
        return filtered_df

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

    def _calculate_feed_generation_session_analytics(
        self, user_to_ranked_feed_map: dict[str, FeedWithMetadata]
    ) -> FeedGenerationSessionAnalytics:
        """Calculates feed generation session analytics."""
        feed_generation_session_analytics: FeedGenerationSessionAnalytics = (
            self.feed_generation_session_analytics_service.calculate_feed_generation_session_analytics(
                user_to_ranked_feed_map=user_to_ranked_feed_map,
                session_timestamp=self.current_datetime_str,
            )
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

    def _ttl_old_feeds(self, test_mode: bool) -> None:
        pass

    def _insert_feed_generation_session_metadata(self, feed_generation_session_analytics: FeedGenerationSessionAnalytics) -> None:
        pass

    def _export_results(self, run_result: RunResult, test_mode: bool) -> None:
        """Export all results (feeds, analytics, session metadata).

        Args:
            run_result: Result containing feeds and analytics.
            test_mode: If True, skip TTL and session insert.
        """
        # Export analytics
        export_feed_analytics(analytics=run_result.analytics)

        # Convert RunResult back to dict format for export_results function
        # (maintaining backward compatibility with existing export function)
        user_to_ranked_feed_map: dict[str, dict] = {}
        for user_did, feed_result in run_result.user_feeds.items():
            user_to_ranked_feed_map[user_did] = {
                "feed": feed_result.feed,
                "bluesky_handle": feed_result.bluesky_handle,
                "bluesky_user_did": feed_result.user_did,
                "condition": feed_result.condition,
                "feed_statistics": feed_result.feed_statistics,
            }

        # Add default feed
        user_to_ranked_feed_map["default"] = {
            "feed": run_result.default_feed.feed,
            "bluesky_handle": run_result.default_feed.bluesky_handle,
            "bluesky_user_did": run_result.default_feed.user_did,
            "condition": run_result.default_feed.condition,
            "feed_statistics": run_result.default_feed.feed_statistics,
        }

        # Write feeds to S3
        export_results(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            timestamp=run_result.timestamp,
        )

        # TTL old feeds
        if not test_mode:
            self.logger.info("TTLing old feeds from active to cache.")
            self.s3.sort_and_move_files_from_active_to_cache(
                prefix="custom_feeds",
                keep_count=self.config.keep_count,
                sort_field="Key",
            )
            self.logger.info(
                f"Done TTLing old feeds from active to cache (keeping {self.config.keep_count})."
            )

            # Insert feed generation session metadata.
            feed_generation_session = {
                "feed_generation_timestamp": run_result.timestamp,
                "number_of_new_feeds": len(user_to_ranked_feed_map),
            }
            insert_feed_generation_session(feed_generation_session)
