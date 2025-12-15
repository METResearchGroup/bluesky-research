"""Orchestrator for feed generation pipeline.

This module provides the FeedGenerationOrchestrator class that wires together
all the components of the feed generation process using dependency injection.
"""

import pandas as pd

from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.constants import TEST_USER_HANDLES
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
    load_latest_feeds,
    load_latest_processed_data,
    postprocess_feed,
    preprocess_data,
)
from services.rank_score_feeds.models import (
    LoadedData,
    PostPools,
    RunResult,
    ScoredPosts,
    UserFeedResult,
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

        # Step 2: Preprocess
        # NOTE: I think this was a one-time patch that should really not
        # be necessary if the preprocess_raw_data service is working.
        loaded_data.posts_df = preprocess_data(loaded_data.posts_df)

        # Step 3: Score posts
        scored_posts = self._score_posts(loaded_data)

        # Step 4: Export scores (conditional)
        if not skip_export_post_scores:
            self._export_scores(scored_posts)

        # Step 5: Build post pools
        post_pools = self._build_post_pools(scored_posts.posts_df)

        # Step 6: Generate feeds
        run_result = self._generate_feeds(loaded_data, scored_posts, post_pools)

        # Step 7: Export results
        self._export_results(run_result, test_mode)

        return run_result

    def _load_data(
        self,
        test_mode: bool,
        users_to_create_feeds_for: list[str] | None,
    ) -> LoadedData:
        """Load all required input data.

        Args:
            test_mode: If True, filter to test users only.
            users_to_create_feeds_for: Optional list of user handles to filter to.

        Returns:
            LoadedData containing all loaded inputs.
        """
        study_users: list[UserToBlueskyProfileModel] = get_all_users()

        if test_mode:
            logger.info(f"Filtering to test users only: {TEST_USER_HANDLES}.")
            study_users = [
                user for user in study_users if user.bluesky_handle in TEST_USER_HANDLES
            ]

        latest_data = load_latest_processed_data()
        latest_feeds = load_latest_feeds()

        consolidated_enriched_posts: list = latest_data[
            "consolidate_enrichment_integrations"
        ]  # type: ignore[assignment]
        consolidated_enriched_posts_df = pd.DataFrame(
            [post.dict() for post in consolidated_enriched_posts]
        )
        user_to_social_network_map: dict = latest_data["scraped_user_social_network"]  # type: ignore[assignment]
        superposter_dids: set[str] = latest_data["superposters"]  # type: ignore[assignment]
        self.logger.info(f"Loaded {len(superposter_dids)} superposters.")

        # Filter users if specified
        if users_to_create_feeds_for:
            self.logger.info(
                f"Creating custom feeds for {len(users_to_create_feeds_for)} users provided in the input."
            )
            study_users = [
                user
                for user in study_users
                if user.bluesky_handle in users_to_create_feeds_for
            ]

        return LoadedData(
            posts_df=consolidated_enriched_posts_df,
            posts_models=consolidated_enriched_posts,  # type: ignore[arg-type]
            user_to_social_network_map=user_to_social_network_map,  # type: ignore[arg-type]
            superposter_dids=superposter_dids,
            previous_feeds=latest_feeds,
            study_users=study_users,
        )

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
        # List of all in-network user posts, across all study users.
        # Needs to be filtered for the in-network posts relevant for a given study user.
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

        # Get lists of in-network and out-of-network posts
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

        # Generate feeds for each user.
        user_to_ranked_feed_map: dict[str, dict] = {}
        total_users = len(loaded_data.study_users)
        self.logger.info(f"Creating feeds for {total_users} users")

        for i, user in enumerate(loaded_data.study_users):
            if i % 10 == 0:
                self.logger.info(f"Creating feed for user {i}/{total_users}")

            post_pool: pd.DataFrame | None = (
                post_pools.reverse_chronological
                if user.condition == "reverse_chronological"
                else post_pools.engagement
                if user.condition == "engagement"
                else post_pools.treatment
                if user.condition == "representative_diversification"
                else None
            )

            if post_pool is None or len(post_pool) == 0:
                raise ValueError(
                    "post_pool cannot be None. This means that a user condition is unexpected/invalid"
                )

            candidate_feed = create_ranked_candidate_feed(
                condition=user.condition,
                in_network_candidate_post_uris=(
                    user_to_in_network_post_uris_map[user.bluesky_user_did]
                ),
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
                previous_post_uris=loaded_data.previous_feeds.get(
                    user.bluesky_handle, set()
                ),
            )

            user_to_ranked_feed_map[user.bluesky_user_did] = {
                "feed": feed,
                "bluesky_handle": user.bluesky_handle,
                "bluesky_user_did": user.bluesky_user_did,
                "condition": user.condition,
                "feed_statistics": generate_feed_statistics(feed=feed),
            }

            if len(feed) == 0:
                self.logger.error(
                    f"No feed created for user {user.bluesky_user_did}. This shouldn't happen..."
                )

        # Insert default feed, for users that aren't logged in or for if a user
        # isn't in the study but opens the link.
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
            previous_post_uris=loaded_data.previous_feeds.get("default", set()),
        )

        user_to_ranked_feed_map["default"] = {
            "feed": postprocessed_default_feed,
            "bluesky_handle": "default",
            "bluesky_user_did": "default",
            "condition": "default",
            "feed_statistics": generate_feed_statistics(
                feed=postprocessed_default_feed
            ),
        }

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
