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
    calculate_feed_analytics,
    calculate_in_network_posts_for_user,
    create_ranked_candidate_feed,
    export_feed_analytics,
    export_post_scores,
    export_results,
    filter_posts_by_author_count,
    generate_feed_statistics,
    insert_feed_generation_session,
    load_feed_input_data,
    load_latest_feeds,
    postprocess_feed,
)
from services.rank_score_feeds.models import (
    FeedInputData,
    LoadedData,
    PostPools,
    RawFeedData,
    RunResult,
    ScoredPosts,
    UserFeedResult,
    LatestFeeds,
)
from services.rank_score_feeds.scoring import calculate_post_scores

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

        self.config = feed_config
        self.athena = Athena()
        self.s3 = S3()
        self.dynamodb = DynamoDB()
        self.glue = Glue()
        self.logger = logger

    def run(
        self,
        users_to_create_feeds_for: list[str] | None = None,
        skip_export_post_scores: bool = False,
        test_mode: bool = False,
    ) -> RunResult:
        """Execute the feed generation pipeline.

        Args:
            users_to_create_feeds_for: Optional list of user handles to create
                feeds for. If None, creates feeds for all users.
            skip_export_post_scores: If True, skip exporting post scores.
            test_mode: If True, run in test mode (limited users, no TTL).

        Returns:
            RunResult containing all generated feeds, analytics, and metadata.
        """
        self.logger.info("Starting rank score feeds.")

        # Step 1: Load all data
        loaded_data = self._load_data(test_mode, users_to_create_feeds_for)

        # Step 2: Deduplicate and filter posts
        loaded_data.posts_df = self._deduplicate_and_filter_posts(loaded_data.posts_df)

        # Step 3: Score posts
        scored_posts = self._score_posts(loaded_data)

        # Step 4: Export scores (optional)
        if not skip_export_post_scores:
            self._export_scores(scored_posts)

        # Step 5: Build post pools
        post_pools = self._build_post_pools(scored_posts.posts_df)

        # Step 6: Generate feeds
        run_result = self._generate_feeds(loaded_data, scored_posts, post_pools)

        # Step 7: Export results
        self._export_results(run_result, test_mode)

        return run_result

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

    def _score_posts(self, loaded_data: LoadedData) -> ScoredPosts:
        """Calculate scores for all posts.

        Args:
            loaded_data: Loaded input data.

        Returns:
            ScoredPosts containing posts with scores and list of new post URIs.
        """
        # Calculate scores for all the posts. Load any pre-existing scores and then
        # calculate scores for new posts.
        post_scores, new_post_uris = calculate_post_scores(
            posts=loaded_data.posts_df,
            superposter_dids=loaded_data.superposter_dids,
            feed_config=self.config,
            load_previous_scores=True,
        )

        engagement_scores = [score["engagement_score"] for score in post_scores]
        treatment_scores = [score["treatment_score"] for score in post_scores]
        loaded_data.posts_df["engagement_score"] = engagement_scores
        loaded_data.posts_df["treatment_score"] = treatment_scores

        self.logger.info(f"Calculated {len(loaded_data.posts_df)} post scores.")

        return ScoredPosts(
            posts_df=loaded_data.posts_df,
            new_post_uris=new_post_uris,
        )

    def _export_scores(self, scored_posts: ScoredPosts) -> None:
        """Export post scores to storage.

        Args:
            scored_posts: Posts with scores.
        """
        scores_to_export: list[dict] = scored_posts.posts_df[
            scored_posts.posts_df["uri"].isin(scored_posts.new_post_uris)
        ][["uri", "text", "source", "engagement_score", "treatment_score"]].to_dict(
            "records"
        )  # type: ignore[call-overload]

        self.logger.info(f"Exporting {len(scores_to_export)} post scores.")
        export_post_scores(scores_to_export=scores_to_export)

    def _build_post_pools(self, posts_df: pd.DataFrame) -> PostPools:
        """Build the three post pools used for ranking.

        Args:
            posts_df: DataFrame with scored posts.

        Returns:
            PostPools containing the three sorted and filtered post pools.
        """
        # Sort feeds (reverse-chronological: timestamp descending, others: score descending)
        reverse_chronological_post_pool_df = posts_df[
            posts_df["source"] == "firehose"
        ].sort_values(by="synctimestamp", ascending=False)  # type: ignore[call-overload]

        engagement_post_pool_df = posts_df.sort_values(
            by="engagement_score", ascending=False
        )

        treatment_post_pool_df = posts_df.sort_values(
            by="treatment_score", ascending=False
        )

        # Filter so that only first X posts from each author are included.
        reverse_chronological_post_pool_df = filter_posts_by_author_count(
            reverse_chronological_post_pool_df,
            max_count=self.config.max_num_times_user_can_appear_in_feed,
        )

        engagement_post_pool_df = filter_posts_by_author_count(
            engagement_post_pool_df,
            max_count=self.config.max_num_times_user_can_appear_in_feed,
        )

        treatment_post_pool_df = filter_posts_by_author_count(
            treatment_post_pool_df,
            max_count=self.config.max_num_times_user_can_appear_in_feed,
        )

        return PostPools(
            reverse_chronological=reverse_chronological_post_pool_df,
            engagement=engagement_post_pool_df,
            treatment=treatment_post_pool_df,
        )

    def _select_post_pool_for_user(
        self, user: UserToBlueskyProfileModel, post_pools: PostPools
    ) -> pd.DataFrame:
        """Select the appropriate post pool for a user based on their condition.

        Args:
            user: User to select post pool for.
            post_pools: Available post pools.

        Returns:
            Selected post pool DataFrame.

        Raises:
            ValueError: If user condition is invalid or post pool is empty.
        """
        if user.condition == "reverse_chronological":
            post_pool = post_pools.reverse_chronological
        elif user.condition == "engagement":
            post_pool = post_pools.engagement
        elif user.condition == "representative_diversification":
            post_pool = post_pools.treatment
        else:
            post_pool = None

        if post_pool is None or len(post_pool) == 0:
            raise ValueError(
                "post_pool cannot be None. This means that a user condition is unexpected/invalid"
            )

        return post_pool

    def _generate_feed_for_user(
        self,
        user: UserToBlueskyProfileModel,
        post_pool: pd.DataFrame,
        in_network_post_uris: list[str],
        previous_post_uris: set[str],
    ) -> dict:
        """Generate a feed for a single user.

        Args:
            user: User to generate feed for.
            post_pool: Post pool to use for ranking.
            in_network_post_uris: List of in-network post URIs for this user.
            previous_post_uris: Set of previous post URIs to avoid.

        Returns:
            Dictionary containing feed data and metadata.
        """
        candidate_feed = create_ranked_candidate_feed(
            condition=user.condition,
            in_network_candidate_post_uris=in_network_post_uris,
            post_pool=post_pool,
            max_feed_length=self.config.max_feed_length,
            max_in_network_posts_ratio=self.config.max_in_network_posts_ratio,
        )

        feed = postprocess_feed(
            feed=candidate_feed,
            max_feed_length=self.config.max_feed_length,
            max_prop_old_posts=self.config.max_prop_old_posts,
            feed_preprocessing_multiplier=self.config.feed_preprocessing_multiplier,
            jitter_amount=self.config.jitter_amount,
            previous_post_uris=previous_post_uris,
        )

        if len(feed) == 0:
            self.logger.error(
                f"No feed created for user {user.bluesky_user_did}. This shouldn't happen..."
            )

        return {
            "feed": feed,
            "bluesky_handle": user.bluesky_handle,
            "bluesky_user_did": user.bluesky_user_did,
            "condition": user.condition,
            "feed_statistics": generate_feed_statistics(feed=feed),
        }

    def _create_default_feed(
        self, post_pools: PostPools, previous_post_uris: set[str]
    ) -> dict:
        """Create the default feed for non-study users.

        Args:
            post_pools: Post pools for ranking.
            previous_post_uris: Set of previous post URIs to avoid.

        Returns:
            Dictionary containing default feed data and metadata.
        """
        default_feed = create_ranked_candidate_feed(
            condition="reverse_chronological",
            in_network_candidate_post_uris=[],
            post_pool=post_pools.reverse_chronological,
            max_feed_length=self.config.max_feed_length,
            max_in_network_posts_ratio=self.config.max_in_network_posts_ratio,
        )

        postprocessed_default_feed = postprocess_feed(
            feed=default_feed,
            max_feed_length=self.config.max_feed_length,
            max_prop_old_posts=self.config.max_prop_old_posts,
            feed_preprocessing_multiplier=self.config.feed_preprocessing_multiplier,
            jitter_amount=self.config.jitter_amount,
            previous_post_uris=previous_post_uris,
        )

        return {
            "feed": postprocessed_default_feed,
            "bluesky_handle": "default",
            "bluesky_user_did": "default",
            "condition": "default",
            "feed_statistics": generate_feed_statistics(
                feed=postprocessed_default_feed
            ),
        }

    def _prepare_post_dataframes(
        self, scored_posts: ScoredPosts
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Prepare in-network and out-of-network post DataFrames.

        Args:
            scored_posts: Posts with scores.

        Returns:
            Tuple of (in_network_df, out_of_network_df).
        """
        candidate_in_network_user_activity_posts_df = scored_posts.posts_df[
            scored_posts.posts_df["source"] == "firehose"
        ]
        # yes, popular posts can also be in-network. For now we'll treat them as
        # out-of-network (and perhaps revisit treating them as in-network as well.)
        # TODO: revisit this.
        out_of_network_user_activity_posts_df = scored_posts.posts_df[
            scored_posts.posts_df["source"] == "most_liked"
        ]

        self.logger.info(
            f"Loaded {len(candidate_in_network_user_activity_posts_df)} in-network posts."
        )
        self.logger.info(
            f"Loaded {len(out_of_network_user_activity_posts_df)} out-of-network posts."
        )

        return (
            candidate_in_network_user_activity_posts_df,
            out_of_network_user_activity_posts_df,
        )

    def _generate_feeds(
        self,
        loaded_data: LoadedData,
        scored_posts: ScoredPosts,
        post_pools: PostPools,
    ) -> RunResult:
        """Generate feeds for all users.

        Args:
            loaded_data: Loaded input data.
            scored_posts: Posts with scores.
            post_pools: Post pools for ranking.

        Returns:
            RunResult containing all generated feeds and analytics.
        """
        # Prepare post DataFrames
        (
            candidate_in_network_user_activity_posts_df,
            _,
        ) = self._prepare_post_dataframes(scored_posts)

        # Get lists of in-network posts for each user
        user_to_in_network_post_uris_map: dict[str, list[str]] = {
            user.bluesky_user_did: calculate_in_network_posts_for_user(
                user_did=user.bluesky_user_did,
                user_to_social_network_map=loaded_data.user_to_social_network_map,
                candidate_in_network_user_activity_posts_df=(
                    candidate_in_network_user_activity_posts_df  # type: ignore[arg-type]
                ),
            )
            for user in loaded_data.study_users
        }

        # Generate feeds for each user
        user_to_ranked_feed_map: dict[str, dict] = {}
        total_users = len(loaded_data.study_users)
        self.logger.info(f"Creating feeds for {total_users} users")

        for i, user in enumerate(loaded_data.study_users):
            if i % 10 == 0:
                self.logger.info(f"Creating feed for user {i}/{total_users}")

            post_pool = self._select_post_pool_for_user(user, post_pools)
            in_network_post_uris = user_to_in_network_post_uris_map[
                user.bluesky_user_did
            ]
            previous_post_uris = loaded_data.previous_feeds.get(
                user.bluesky_handle, set()
            )

            feed_data = self._generate_feed_for_user(
                user=user,
                post_pool=post_pool,
                in_network_post_uris=in_network_post_uris,
                previous_post_uris=previous_post_uris,
            )

            user_to_ranked_feed_map[user.bluesky_user_did] = feed_data

        # Create default feed
        default_previous_post_uris = loaded_data.previous_feeds.get("default", set())
        user_to_ranked_feed_map["default"] = self._create_default_feed(
            post_pools, default_previous_post_uris
        )

        timestamp = generate_current_datetime_str()

        # Calculate analytics
        analytics = calculate_feed_analytics(
            user_to_ranked_feed_map=user_to_ranked_feed_map,
            timestamp=timestamp,
        )

        # Convert user_to_ranked_feed_map to UserFeedResult objects
        user_feeds: dict[str, UserFeedResult] = {}
        for user_did, feed_data in user_to_ranked_feed_map.items():
            user_feeds[user_did] = UserFeedResult(
                user_did=feed_data["bluesky_user_did"],
                bluesky_handle=feed_data["bluesky_handle"],
                condition=feed_data["condition"],
                feed=feed_data["feed"],
                feed_statistics=feed_data["feed_statistics"],
            )

        default_feed_result = user_feeds.pop("default")

        return RunResult(
            user_feeds=user_feeds,
            default_feed=default_feed_result,
            analytics=analytics,
            timestamp=timestamp,
        )

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
