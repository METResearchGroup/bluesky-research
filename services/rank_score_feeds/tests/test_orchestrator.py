"""Tests for FeedGenerationOrchestrator.

This test suite verifies the orchestrator structure and functionality.
These tests ensure:
- Orchestrator can be instantiated with FeedConfig
- All dependencies are properly constructed
- Data loading and filtering methods work correctly
- All pipeline steps execute in correct order
- Service delegation works correctly
- Error handling works correctly
"""

import pytest
import pandas as pd
from typing import cast
from unittest.mock import patch, MagicMock

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    LatestFeeds,
    LoadedData,
    CandidatePostPools,
    FeedWithMetadata,
    FeedGenerationSessionAnalytics,
)
from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator
from services.rank_score_feeds.storage.exceptions import StorageError
from services.participant_data.models import UserToBlueskyProfileModel


class TestFeedGenerationOrchestrator:
    """Tests for FeedGenerationOrchestrator class."""

    def test_load_data_returns_loaded_data(self):
        """Verify _load_data returns LoadedData with correct structure."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            )
        ]

        mock_posts_df = pd.DataFrame({"uri": ["post1"]})
        mock_loaded_data = LoadedData(
            posts_df=mock_posts_df,
            user_to_social_network_map={"did:plc:user1": ["did:plc:user2"]},
            superposter_dids={"did:plc:superposter1"},
            previous_feeds=LatestFeeds(feeds={"user1.bsky.social": {"post1", "post2"}}),
            study_users=mock_users,
        )

        with patch.object(
            orchestrator.feed_data_loader, "load_complete_data"
        ) as mock_load_complete:
            mock_load_complete.return_value = mock_loaded_data

            # Act
            result = orchestrator._load_data(test_mode=False, users_to_create_feeds_for=None)

        # Assert
            assert isinstance(result, LoadedData)
            assert result.posts_df.equals(mock_posts_df)
            assert result.user_to_social_network_map == mock_loaded_data.user_to_social_network_map
            assert result.superposter_dids == mock_loaded_data.superposter_dids
            assert result.previous_feeds == mock_loaded_data.previous_feeds
            assert result.study_users == mock_loaded_data.study_users
            mock_load_complete.assert_called_once_with(
                test_mode=False, users_to_create_feeds_for=None, deduplicate_posts=True
            )

    def test_load_data_filters_users_when_specified(self):
        """Verify _load_data filters users correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        filtered_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            )
        ]

        mock_loaded_data = LoadedData(
            posts_df=pd.DataFrame(),
            user_to_social_network_map={},
            superposter_dids=set(),
            previous_feeds=LatestFeeds(feeds={}),
            study_users=filtered_users,
        )

        with patch.object(
            orchestrator.feed_data_loader, "load_complete_data"
        ) as mock_load_complete:
            mock_load_complete.return_value = mock_loaded_data

            # Act
            result = orchestrator._load_data(
                test_mode=False, users_to_create_feeds_for=["user1.bsky.social"]
            )

            # Assert
            assert len(result.study_users) == 1
            assert result.study_users[0].bluesky_handle == "user1.bsky.social"
            mock_load_complete.assert_called_once_with(
                test_mode=False,
                users_to_create_feeds_for=["user1.bsky.social"],
                deduplicate_posts=True,
            )

    def test_score_posts_delegates_to_scoring_service(self):
        """Verify _score_posts delegates to scoring_service correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_posts_df = pd.DataFrame({"uri": ["post1"]})
        mock_superposter_dids = {"did:plc:superposter1"}
        mock_loaded_data = LoadedData(
            posts_df=mock_posts_df,
            user_to_social_network_map={},
            superposter_dids=mock_superposter_dids,
            previous_feeds=LatestFeeds(feeds={}),
            study_users=[],
        )

        expected_result = pd.DataFrame({"uri": ["post1"], "score": [0.5]})

        with patch.object(orchestrator.scoring_service, 'score_posts') as mock_score:
            mock_score.return_value = expected_result

            # Act
            result = orchestrator._score_posts(mock_loaded_data, export_new_scores=True)

            # Assert
            mock_score.assert_called_once_with(
                posts_df=mock_posts_df,
                superposter_dids=mock_superposter_dids,
                export_new_scores=True,
            )
            assert result.equals(expected_result)

    def test_score_posts_passes_export_new_scores_false(self):
        """Verify _score_posts passes export_new_scores=False correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_loaded_data = LoadedData(
            posts_df=pd.DataFrame({"uri": ["post1"]}),
            user_to_social_network_map={},
            superposter_dids=set(),
            previous_feeds=LatestFeeds(feeds={}),
            study_users=[],
        )

        with patch.object(orchestrator.scoring_service, 'score_posts') as mock_score:
            mock_score.return_value = pd.DataFrame()

            # Act
            orchestrator._score_posts(mock_loaded_data, export_new_scores=False)

            # Assert
            mock_score.assert_called_once_with(
                posts_df=mock_loaded_data.posts_df,
                superposter_dids=mock_loaded_data.superposter_dids,
                export_new_scores=False,
            )

    def test_generate_candidate_pools_delegates_to_candidate_service(self):
        """Verify _generate_candidate_pools delegates correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_posts_df = pd.DataFrame({"uri": ["post1"]})
        mock_pools = CandidatePostPools(
            reverse_chronological=pd.DataFrame(),
            engagement=pd.DataFrame(),
            treatment=pd.DataFrame(),
        )

        with patch.object(orchestrator.candidate_service, 'generate_candidate_pools') as mock_gen:
            mock_gen.return_value = mock_pools

            # Act
            result = orchestrator._generate_candidate_pools(mock_posts_df)

            # Assert
            mock_gen.assert_called_once_with(posts_df=mock_posts_df)
            assert result == mock_pools

    def test_generate_feeds_calls_services_correctly(self):
        """Verify _generate_feeds calls context and feed services correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_loaded_data = LoadedData(
            posts_df=pd.DataFrame({"uri": ["post1"]}),
            user_to_social_network_map={"did:plc:user1": ["did:plc:user2"]},
            superposter_dids=set(),
            previous_feeds=LatestFeeds(feeds={}),
            study_users=[
                UserToBlueskyProfileModel(
                    study_user_id="1",
                    condition="engagement",
                    bluesky_handle="user1.bsky.social",
                    bluesky_user_did="did:plc:user1",
                    is_study_user=True,
                    created_timestamp="2024-01-01T00:00:00Z",
                )
            ],
        )

        mock_candidate_pools = CandidatePostPools(
            reverse_chronological=pd.DataFrame(),
            engagement=pd.DataFrame(),
            treatment=pd.DataFrame(),
        )

        mock_in_network_map = {"did:plc:user1": ["post1"]}
        mock_feeds = {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}

        with patch.object(orchestrator.context_service, 'build_in_network_context') as mock_context, \
             patch.object(orchestrator.feed_service, 'generate_feeds_for_users') as mock_feed_gen:
            mock_context.return_value = mock_in_network_map
            mock_feed_gen.return_value = mock_feeds

            # Act
            result = orchestrator._generate_feeds(mock_loaded_data, mock_candidate_pools)

            # Assert
            mock_context.assert_called_once_with(
                scored_posts=mock_loaded_data.posts_df,
                study_users=mock_loaded_data.study_users,
                user_to_social_network_map=mock_loaded_data.user_to_social_network_map,
            )
            mock_feed_gen.assert_called_once_with(
                user_to_in_network_post_uris_map=mock_in_network_map,
                candidate_post_pools=mock_candidate_pools,
                study_users=mock_loaded_data.study_users,
                previous_feeds=mock_loaded_data.previous_feeds,
            )
            assert result == mock_feeds

    def test_calculate_feed_generation_session_analytics_delegates_correctly(self):
        """Verify analytics calculation delegates correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)
        orchestrator.current_datetime_str = "2024-01-01T00:00:00Z"

        mock_feeds: dict[str, FeedWithMetadata] = cast(
            dict[str, FeedWithMetadata],
            {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}
        )
        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(
            orchestrator.feed_generation_session_analytics_service,
            'calculate_feed_generation_session_analytics'
        ) as mock_calc:
            mock_calc.return_value = mock_analytics

            # Act
            result = orchestrator._calculate_feed_generation_session_analytics(mock_feeds)

            # Assert
            mock_calc.assert_called_once_with(
                user_to_ranked_feed_map=mock_feeds,
                session_timestamp=orchestrator.current_datetime_str,
            )
            assert result == mock_analytics

    def test_export_artifacts_calls_both_export_methods(self):
        """Verify _export_artifacts calls both export methods."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)
        orchestrator.current_datetime_str = "2024-01-01T00:00:00Z"

        mock_feeds: dict[str, FeedWithMetadata] = cast(
            dict[str, FeedWithMetadata],
            {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}
        )
        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(orchestrator.data_exporter_service, 'export_feeds') as mock_export_feeds, \
             patch.object(
                 orchestrator.data_exporter_service,
                 'export_feed_generation_session_analytics'
             ) as mock_export_analytics:
            # Act
            orchestrator._export_artifacts(mock_feeds, mock_analytics)

            # Assert
            mock_export_feeds.assert_called_once_with(
                user_to_ranked_feed_map=mock_feeds,
                timestamp=orchestrator.current_datetime_str,
            )
            mock_export_analytics.assert_called_once_with(
                feed_generation_session_analytics=mock_analytics,
                timestamp=orchestrator.current_datetime_str,
            )

    def test_ttl_old_feeds_success(self):
        """Verify _ttl_old_feeds calls adapter correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        with patch.object(orchestrator.feed_ttl_adapter, 'move_to_cache') as mock_move:
            # Act
            orchestrator._ttl_old_feeds()

            # Assert
            mock_move.assert_called_once_with(
                prefix="custom_feeds",
                keep_count=config.keep_count,
                sort_field="Key",
            )

    def test_ttl_old_feeds_raises_storage_error_on_failure(self):
        """Verify _ttl_old_feeds raises StorageError on failure."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        with patch.object(orchestrator.feed_ttl_adapter, 'move_to_cache') as mock_move:
            mock_move.side_effect = Exception("S3 error")

            # Act & Assert
            with pytest.raises(StorageError, match="Failed to TTL old feeds"):
                orchestrator._ttl_old_feeds()

    def test_insert_feed_generation_session_metadata_success(self):
        """Verify _insert_feed_generation_session_metadata calls adapter correctly."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(
            orchestrator.session_metadata_adapter,
            'insert_session_metadata'
        ) as mock_insert:
            # Act
            orchestrator._insert_feed_generation_session_metadata(mock_analytics)

            # Assert
            mock_insert.assert_called_once_with(metadata=mock_analytics)

    def test_insert_feed_generation_session_metadata_raises_storage_error_on_failure(self):
        """Verify metadata insertion raises StorageError on failure."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_analytics = FeedGenerationSessionAnalytics(
            total_feeds=1,
            total_posts=10,
            total_in_network_posts=5,
            total_in_network_posts_prop=0.5,
            total_unique_engagement_uris=10,
            total_unique_treatment_uris=8,
            prop_overlap_treatment_uris_in_engagement_uris=0.6,
            prop_overlap_engagement_uris_in_treatment_uris=0.75,
            total_feeds_per_condition={"engagement": 1},
            session_timestamp="2024-01-01T00:00:00Z",
        )

        with patch.object(
            orchestrator.session_metadata_adapter,
            'insert_session_metadata'
        ) as mock_insert:
            mock_insert.side_effect = Exception("DynamoDB error")

            # Act & Assert
            with pytest.raises(
                StorageError,
                match="Failed to insert feed generation session metadata"
            ):
                orchestrator._insert_feed_generation_session_metadata(mock_analytics)

    def test_run_completes_full_pipeline_in_test_mode(self):
        """Verify run() executes full pipeline correctly in test_mode=True."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        mock_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                condition="engagement",
                bluesky_handle="user1.bsky.social",
                bluesky_user_did="did:plc:user1",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00Z",
            )
        ]

        mock_loaded_data = LoadedData(
            posts_df=pd.DataFrame({
                "uri": ["post1"],
                "consolidation_timestamp": ["2024-01-01T00:00:00Z"],
                "author_did": ["did:plc:author1"],
                "author_handle": ["author1.bsky.social"],
            }),
            user_to_social_network_map={},
            superposter_dids=set(),
            previous_feeds=LatestFeeds(feeds={}),
            study_users=mock_users,
        )

        # Mock all service calls
        with patch.object(orchestrator, '_load_data') as mock_load_data, \
             patch.object(orchestrator.scoring_service, 'score_posts') as mock_score, \
             patch.object(orchestrator.candidate_service, 'generate_candidate_pools') as mock_candidate, \
             patch.object(orchestrator.context_service, 'build_in_network_context') as mock_context, \
             patch.object(orchestrator.feed_service, 'generate_feeds_for_users') as mock_feed_gen, \
             patch.object(
                 orchestrator.feed_generation_session_analytics_service,
                 'calculate_feed_generation_session_analytics'
             ) as mock_analytics, \
             patch.object(orchestrator.data_exporter_service, 'export_feeds') as mock_export_feeds, \
             patch.object(
                 orchestrator.data_exporter_service,
                 'export_feed_generation_session_analytics'
             ) as mock_export_analytics, \
             patch.object(orchestrator, '_ttl_old_feeds') as mock_ttl, \
             patch.object(
                 orchestrator,
                 '_insert_feed_generation_session_metadata'
             ) as mock_insert:

            # Setup return values
            mock_load_data.return_value = mock_loaded_data
            mock_score.return_value = pd.DataFrame({"uri": ["post1"], "score": [0.5]})
            mock_candidate.return_value = CandidatePostPools(
                reverse_chronological=pd.DataFrame(),
                engagement=pd.DataFrame(),
                treatment=pd.DataFrame(),
            )
            mock_context.return_value = {"did:plc:user1": ["post1"]}
            mock_feeds_dict = {"did:plc:user1": MagicMock(spec=FeedWithMetadata)}
            mock_feed_gen.return_value = mock_feeds_dict
            mock_analytics.return_value = FeedGenerationSessionAnalytics(
                total_feeds=1,
                total_posts=1,
                total_in_network_posts=1,
                total_in_network_posts_prop=1.0,
                total_unique_engagement_uris=1,
                total_unique_treatment_uris=0,
                prop_overlap_treatment_uris_in_engagement_uris=0.0,
                prop_overlap_engagement_uris_in_treatment_uris=0.0,
                total_feeds_per_condition={"engagement": 1},
                session_timestamp="2024-01-01T00:00:00Z",
            )

            # Act
            orchestrator.run(test_mode=True, export_new_scores=True)

            # Assert - verify all steps were called
            mock_load_data.assert_called_once_with(test_mode=True, users_to_create_feeds_for=None)
            mock_score.assert_called_once()
            mock_candidate.assert_called_once()
            mock_context.assert_called_once()
            mock_feed_gen.assert_called_once()
            mock_analytics.assert_called_once()
            mock_export_feeds.assert_called_once()
            mock_export_analytics.assert_called_once()
            # TTL and metadata should NOT be called in test_mode
            mock_ttl.assert_not_called()
            mock_insert.assert_not_called()

    def test_run_skips_ttl_and_metadata_in_test_mode(self):
        """Verify run() skips TTL and metadata insertion in test_mode=True."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock everything needed for the pipeline
        with patch.object(orchestrator, '_load_data') as mock_load, \
             patch.object(orchestrator, '_score_posts') as mock_score, \
             patch.object(orchestrator, '_generate_candidate_pools') as mock_candidate, \
             patch.object(orchestrator, '_generate_feeds') as mock_gen_feeds, \
             patch.object(
                 orchestrator,
                 '_calculate_feed_generation_session_analytics'
             ) as mock_analytics, \
             patch.object(orchestrator, '_export_artifacts') as mock_export, \
             patch.object(orchestrator, '_ttl_old_feeds') as mock_ttl, \
             patch.object(
                 orchestrator,
                 '_insert_feed_generation_session_metadata'
             ) as mock_insert:

            mock_load.return_value = LoadedData(
                posts_df=pd.DataFrame(),
                user_to_social_network_map={},
                superposter_dids=set(),
                previous_feeds=LatestFeeds(feeds={}),
                study_users=[],
            )
            mock_score.return_value = pd.DataFrame()
            mock_candidate.return_value = CandidatePostPools(
                reverse_chronological=pd.DataFrame(),
                engagement=pd.DataFrame(),
                treatment=pd.DataFrame(),
            )
            mock_gen_feeds.return_value = {}
            mock_analytics.return_value = FeedGenerationSessionAnalytics(
                total_feeds=0,
                total_posts=0,
                total_in_network_posts=0,
                total_in_network_posts_prop=0.0,
                total_unique_engagement_uris=0,
                total_unique_treatment_uris=0,
                prop_overlap_treatment_uris_in_engagement_uris=0.0,
                prop_overlap_engagement_uris_in_treatment_uris=0.0,
                total_feeds_per_condition={},
                session_timestamp="2024-01-01T00:00:00Z",
            )

            # Act
            orchestrator.run(test_mode=True)

            # Assert
            mock_ttl.assert_not_called()
            mock_insert.assert_not_called()

    def test_run_calls_ttl_and_metadata_in_production_mode(self):
        """Verify run() calls TTL and metadata insertion when test_mode=False."""
        # Arrange
        config = FeedConfig()
        orchestrator = FeedGenerationOrchestrator(feed_config=config)

        # Mock everything needed for the pipeline
        with patch.object(orchestrator, '_load_data') as mock_load, \
             patch.object(orchestrator, '_score_posts') as mock_score, \
             patch.object(orchestrator, '_generate_candidate_pools') as mock_candidate, \
             patch.object(orchestrator, '_generate_feeds') as mock_gen_feeds, \
             patch.object(
                 orchestrator,
                 '_calculate_feed_generation_session_analytics'
             ) as mock_analytics, \
             patch.object(orchestrator, '_export_artifacts') as mock_export, \
             patch.object(orchestrator, '_ttl_old_feeds') as mock_ttl, \
             patch.object(
                 orchestrator,
                 '_insert_feed_generation_session_metadata'
             ) as mock_insert:

            mock_load.return_value = LoadedData(
                posts_df=pd.DataFrame(),
                user_to_social_network_map={},
                superposter_dids=set(),
                previous_feeds=LatestFeeds(feeds={}),
                study_users=[],
            )
            mock_score.return_value = pd.DataFrame()
            mock_candidate.return_value = CandidatePostPools(
                reverse_chronological=pd.DataFrame(),
                engagement=pd.DataFrame(),
                treatment=pd.DataFrame(),
            )
            mock_gen_feeds.return_value = {}
            mock_analytics.return_value = FeedGenerationSessionAnalytics(
                total_feeds=0,
                total_posts=0,
                total_in_network_posts=0,
                total_in_network_posts_prop=0.0,
                total_unique_engagement_uris=0,
                total_unique_treatment_uris=0,
                prop_overlap_treatment_uris_in_engagement_uris=0.0,
                prop_overlap_engagement_uris_in_treatment_uris=0.0,
                total_feeds_per_condition={},
                session_timestamp="2024-01-01T00:00:00Z",
            )

            # Act
            orchestrator.run(test_mode=False)

            # Assert
            mock_ttl.assert_called_once()
            mock_insert.assert_called_once()
