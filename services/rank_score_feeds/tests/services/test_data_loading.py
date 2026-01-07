"""Tests for DataLoadingService, DataTransformationService, and FeedDataLoader."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from services.calculate_superposters.models import CalculateSuperposterSource
from services.participant_data.models import UserToBlueskyProfileModel
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    FeedInputData,
    LatestFeeds,
    LoadedData,
    RawFeedData,
)
from services.rank_score_feeds.services.data_loading import (
    DataLoadingService,
    DataTransformationService,
    FeedDataLoader,
)
from lib.datetime_utils import TimestampFormat


class TestInit:
    """Tests for DataLoadingService.__init__."""

    def test_init_sets_dependencies(self):
        """Service initializes logger and Athena, and stores config."""
        # Arrange
        feed_config = FeedConfig(freshness_lookback_days=7)
        mock_athena = Mock()

        # Patch get_logger before any imports that might call it at module level
        # This includes S3 (imported by athena) and athena itself
        with patch(
            "lib.aws.athena.Athena", return_value=mock_athena
        ) as p_athena:
            # Act
            service = DataLoadingService(feed_config=feed_config)

        # Assert
        assert service.config == feed_config
        assert service.logger is not None
        assert service.athena is mock_athena
        p_athena.assert_called_once()


class TestLoadFeedInputData:
    """Tests for DataLoadingService.load_feed_input_data."""

    @pytest.fixture
    def service(self):
        """Create service with a specific lookback default."""
        return DataLoadingService(feed_config=FeedConfig(freshness_lookback_days=7))

    def test_uses_config_lookback_when_none(self, service):
        """When lookback_days is None, uses config.freshness_lookback_days."""
        # Arrange
        with patch(
            "services.rank_score_feeds.services.data_loading.calculate_lookback_datetime_str",
            return_value="ts",
        ) as p_calc, patch(
            "services.rank_score_feeds.services.data_loading.load_enriched_posts",
            return_value=pd.DataFrame({"uri": ["u1"]}),
        ) as p_load_posts, patch(
            "services.rank_score_feeds.services.data_loading.load_user_social_network_map",
            return_value={"did:1": ["did:2"]},
        ) as p_social, patch(
            "services.rank_score_feeds.services.data_loading.load_latest_superposters",
            return_value={"did:sp"},
        ) as p_super:
            service.logger = Mock()

            # Act
            result = service.load_feed_input_data()

        # Assert
        assert isinstance(result, FeedInputData)
        assert len(result.consolidate_enrichment_integrations) == 1
        assert result.scraped_user_social_network == {"did:1": ["did:2"]}
        assert result.superposters == {"did:sp"}

        p_calc.assert_called_once_with(7, format=TimestampFormat.BLUESKY)
        p_load_posts.assert_called_once_with(latest_timestamp="ts")
        p_social.assert_called_once()
        # Verify source and timestamp passed
        args, kwargs = p_super.call_args
        assert kwargs["source"] == CalculateSuperposterSource.LOCAL
        assert kwargs["latest_timestamp"] == "ts"

    def test_uses_explicit_lookback_when_provided(self, service):
        """When lookback_days is provided, it overrides config default."""
        # Arrange
        with patch(
            "services.rank_score_feeds.services.data_loading.calculate_lookback_datetime_str",
            return_value="other_ts",
        ) as p_calc, patch(
            "services.rank_score_feeds.services.data_loading.load_enriched_posts",
            return_value=pd.DataFrame({"uri": ["u1", "u2"]}),
        ), patch(
            "services.rank_score_feeds.services.data_loading.load_user_social_network_map",
            return_value={"did:1": ["did:2", "did:3"]},
        ), patch(
            "services.rank_score_feeds.services.data_loading.load_latest_superposters",
            return_value={"did:sp1", "did:sp2"},
        ):
            # Act
            result = service.load_feed_input_data(lookback_days=3)

        # Assert
        assert isinstance(result, FeedInputData)
        p_calc.assert_called_once_with(3, format=TimestampFormat.BLUESKY)


class TestLoadLatestFeeds:
    """Tests for DataLoadingService.load_latest_feeds."""

    @pytest.fixture
    def service(self):
        """Create service and stub Athena."""
        svc = DataLoadingService(feed_config=FeedConfig(freshness_lookback_days=7))
        svc.athena = Mock()
        return svc

    def test_builds_latest_feeds_mapping(self, service):
        """Builds LatestFeeds mapping from Athena query with JSON feed payload."""
        # Arrange
        df = pd.DataFrame(
            [
                {
                    "bluesky_handle": "u1",
                    "feed": '[{"item":"a"},{"item":"b"}]',
                },
                {
                    "bluesky_handle": "u2",
                    "feed": '[{"item":"c"},{"item":"a"}]',
                },
            ]
        )
        service.athena.query_results_as_df.return_value = df
        with patch(
            "services.rank_score_feeds.services.data_loading.parse_converted_pandas_dicts",
            side_effect=lambda d: d,
        ):
            service.logger = Mock()

            # Act
            result = service.load_latest_feeds()

        # Assert
        assert isinstance(result, LatestFeeds)
        assert "u1" in result.keys()
        assert "u2" in result.keys()
        assert result.get("u1") == {"a", "b"}
        assert result.get("u2") == {"a", "c"}
        # Logger called at least twice (start and finish)
        assert service.logger.info.call_count >= 2

    def test_handles_empty_results(self, service):
        """Returns empty LatestFeeds when no rows are returned."""
        # Arrange
        service.athena.query_results_as_df.return_value = pd.DataFrame([])
        with patch(
            "services.rank_score_feeds.services.data_loading.parse_converted_pandas_dicts",
            return_value=[],
        ):
            # Act
            result = service.load_latest_feeds()

        # Assert
        assert isinstance(result, LatestFeeds)
        assert len(result) == 0


class TestLoadStudyUsers:
    """Tests for DataLoadingService.load_study_users."""

    @pytest.fixture
    def service(self):
        """Create service with mocked dependencies."""
        svc = DataLoadingService(feed_config=FeedConfig(freshness_lookback_days=7))
        svc.logger = Mock()
        return svc

    def test_loads_all_users_when_test_mode_false(self, service):
        """Test that load_study_users calls get_all_users with test_mode=False."""
        # Arrange
        expected_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                bluesky_handle="user1",
                bluesky_user_did="did:user1",
                condition="engagement",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
            UserToBlueskyProfileModel(
                study_user_id="2",
                bluesky_handle="user2",
                bluesky_user_did="did:user2",
                condition="representative_diversification",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
        ]

        with patch(
            "services.rank_score_feeds.services.data_loading.get_all_users",
            return_value=expected_users,
        ) as mock_get_users:
            # Act
            result = service.load_study_users(test_mode=False)

        # Assert
        assert result == expected_users
        mock_get_users.assert_called_once_with(test_mode=False)
        assert service.logger.info.call_count == 2

    def test_loads_test_users_when_test_mode_true(self, service):
        """Test that load_study_users calls get_all_users with test_mode=True."""
        # Arrange
        expected_users = [
            UserToBlueskyProfileModel(
                study_user_id="test1",
                bluesky_handle="test_user",
                bluesky_user_did="did:test",
                condition="engagement",
                is_study_user=False,
                created_timestamp="2024-01-01T00:00:00",
            )
        ]

        with patch(
            "services.rank_score_feeds.services.data_loading.get_all_users",
            return_value=expected_users,
        ) as mock_get_users:
            # Act
            result = service.load_study_users(test_mode=True)

        # Assert
        assert result == expected_users
        mock_get_users.assert_called_once_with(test_mode=True)
        assert service.logger.info.call_count == 2


class TestLoadRawData:
    """Tests for DataLoadingService.load_raw_data."""

    @pytest.fixture
    def service(self):
        """Create service with mocked dependencies."""
        svc = DataLoadingService(feed_config=FeedConfig(freshness_lookback_days=7))
        svc.logger = Mock()
        return svc

    def test_loads_all_raw_data_components(self, service):
        """Test that load_raw_data loads study users, feed input data, and latest feeds."""
        # Arrange
        expected_users = [
            UserToBlueskyProfileModel(
                study_user_id="1",
                bluesky_handle="user1",
                bluesky_user_did="did:user1",
                condition="engagement",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            )
        ]
        expected_feed_input = FeedInputData(
            consolidate_enrichment_integrations=pd.DataFrame({"uri": ["post1"]}),
            scraped_user_social_network={"did:1": ["did:2"]},
            superposters={"did:sp"},
        )
        expected_latest_feeds = LatestFeeds(feeds={"user1": {"post1", "post2"}})

        with patch.object(
            service, "load_study_users", return_value=expected_users
        ) as mock_load_users, patch.object(
            service, "load_feed_input_data", return_value=expected_feed_input
        ) as mock_load_feed, patch.object(
            service, "load_latest_feeds", return_value=expected_latest_feeds
        ) as mock_load_feeds:
            # Act
            result = service.load_raw_data(test_mode=False)

        # Assert
        assert isinstance(result, RawFeedData)
        assert result.study_users == expected_users
        assert result.feed_input_data == expected_feed_input
        assert result.latest_feeds == expected_latest_feeds
        mock_load_users.assert_called_once_with(test_mode=False)
        mock_load_feed.assert_called_once()
        mock_load_feeds.assert_called_once()

    def test_passes_test_mode_to_load_study_users(self, service):
        """Test that test_mode parameter is passed to load_study_users."""
        # Arrange
        expected_users = []
        with patch.object(
            service, "load_study_users", return_value=expected_users
        ) as mock_load_users, patch.object(
            service, "load_feed_input_data", return_value=FeedInputData(
                consolidate_enrichment_integrations=pd.DataFrame(),
                scraped_user_social_network={},
                superposters=set(),
            )
        ), patch.object(
            service, "load_latest_feeds", return_value=LatestFeeds(feeds={})
        ):
            # Act
            service.load_raw_data(test_mode=True)

        # Assert
        mock_load_users.assert_called_once_with(test_mode=True)


class TestDataTransformationServiceInit:
    """Tests for DataTransformationService.__init__."""

    def test_init_sets_config_and_logger(self):
        """Test that __init__ sets config and logger."""
        # Arrange
        feed_config = FeedConfig(freshness_lookback_days=7)
        mock_logger = Mock()

        with patch(
            "services.rank_score_feeds.services.data_loading.get_logger",
            return_value=mock_logger,
        ):
            # Act
            service = DataTransformationService(feed_config=feed_config)

        # Assert
        assert service.config == feed_config
        assert service.logger is mock_logger


class TestFilterStudyUsers:
    """Tests for DataTransformationService.filter_study_users."""

    @pytest.fixture
    def service(self):
        """Create service with mocked logger."""
        svc = DataTransformationService(feed_config=FeedConfig(freshness_lookback_days=7))
        svc.logger = Mock()
        return svc

    @pytest.fixture
    def sample_users(self):
        """Create sample study users."""
        return [
            UserToBlueskyProfileModel(
                study_user_id="1",
                bluesky_handle="user1",
                bluesky_user_did="did:user1",
                condition="engagement",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
            UserToBlueskyProfileModel(
                study_user_id="2",
                bluesky_handle="user2",
                bluesky_user_did="did:user2",
                condition="representative_diversification",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
            UserToBlueskyProfileModel(
                study_user_id="3",
                bluesky_handle="user3",
                bluesky_user_did="did:user3",
                condition="reverse_chronological",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            ),
        ]

    def test_returns_all_users_when_none_specified(self, service, sample_users):
        """Test that filter_study_users returns all users when None is provided."""
        # Arrange
        expected = sample_users

        # Act
        result = service.filter_study_users(sample_users, None)

        # Assert
        assert result == expected
        service.logger.info.assert_not_called()

    def test_returns_all_users_when_empty_list_provided(self, service, sample_users):
        """Test that filter_study_users returns all users when empty list is provided."""
        # Arrange
        expected = sample_users

        # Act
        result = service.filter_study_users(sample_users, [])

        # Assert
        assert result == expected
        service.logger.info.assert_not_called()

    def test_filters_to_specified_users(self, service, sample_users):
        """Test that filter_study_users filters to only specified handles."""
        # Arrange
        users_to_create_feeds_for = ["user1", "user3"]
        expected = [sample_users[0], sample_users[2]]

        # Act
        result = service.filter_study_users(sample_users, users_to_create_feeds_for)

        # Assert
        assert result == expected
        assert len(result) == 2
        assert result[0].bluesky_handle == "user1"
        assert result[1].bluesky_handle == "user3"
        service.logger.info.assert_called_once()

    def test_returns_empty_list_when_no_matches(self, service, sample_users):
        """Test that filter_study_users returns empty list when no handles match."""
        # Arrange
        users_to_create_feeds_for = ["nonexistent_user"]

        # Act
        result = service.filter_study_users(sample_users, users_to_create_feeds_for)

        # Assert
        assert result == []
        service.logger.info.assert_called_once()


class TestDeduplicateAndFilterPosts:
    """Tests for DataTransformationService.deduplicate_and_filter_posts."""

    @pytest.fixture
    def service(self):
        """Create service with mocked logger."""
        svc = DataTransformationService(feed_config=FeedConfig(freshness_lookback_days=7))
        svc.logger = Mock()
        return svc

    def test_deduplicates_posts_by_uri(self, service):
        """Test that deduplicate_and_filter_posts removes duplicate URIs."""
        # Arrange
        posts_df = pd.DataFrame(
            {
                "uri": ["uri1", "uri2", "uri1", "uri3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00",
                    "2024-01-02T00:00:00",
                    "2024-01-03T00:00:00",  # More recent duplicate
                    "2024-01-04T00:00:00",
                ],
                "author_did": ["did:1", "did:2", "did:1", "did:3"],
                "author_handle": ["user1", "user2", "user1", "user3"],
            }
        )

        with patch(
            "services.rank_score_feeds.services.data_loading.load_users_to_exclude",
            return_value={"bsky_handles_to_exclude": set(), "bsky_dids_to_exclude": set()},
        ):
            # Act
            result = service.deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 3
        assert "uri1" in result["uri"].values
        assert "uri2" in result["uri"].values
        assert "uri3" in result["uri"].values
        # Should keep the most recent uri1 (2024-01-03)
        uri1_rows = result[result["uri"] == "uri1"]
        assert len(uri1_rows) == 1
        assert uri1_rows.iloc[0]["consolidation_timestamp"] == "2024-01-03T00:00:00"
        service.logger.info.assert_called_once()

    def test_filters_excluded_authors_by_did(self, service):
        """Test that deduplicate_and_filter_posts filters out excluded authors by DID."""
        # Arrange
        posts_df = pd.DataFrame(
            {
                "uri": ["uri1", "uri2", "uri3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00",
                    "2024-01-02T00:00:00",
                    "2024-01-03T00:00:00",
                ],
                "author_did": ["did:excluded", "did:allowed", "did:excluded"],
                "author_handle": ["excluded", "allowed", "excluded2"],
            }
        )

        with patch(
            "services.rank_score_feeds.services.data_loading.load_users_to_exclude",
            return_value={
                "bsky_handles_to_exclude": set(),
                "bsky_dids_to_exclude": {"did:excluded"},
            },
        ):
            # Act
            result = service.deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 1
        assert result.iloc[0]["uri"] == "uri2"
        assert result.iloc[0]["author_did"] == "did:allowed"

    def test_filters_excluded_authors_by_handle(self, service):
        """Test that deduplicate_and_filter_posts filters out excluded authors by handle."""
        # Arrange
        posts_df = pd.DataFrame(
            {
                "uri": ["uri1", "uri2", "uri3"],
                "consolidation_timestamp": [
                    "2024-01-01T00:00:00",
                    "2024-01-02T00:00:00",
                    "2024-01-03T00:00:00",
                ],
                "author_did": ["did:1", "did:2", "did:3"],
                "author_handle": ["excluded", "allowed", "excluded"],
            }
        )

        with patch(
            "services.rank_score_feeds.services.data_loading.load_users_to_exclude",
            return_value={
                "bsky_handles_to_exclude": {"excluded"},
                "bsky_dids_to_exclude": set(),
            },
        ):
            # Act
            result = service.deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 1
        assert result.iloc[0]["uri"] == "uri2"
        assert result.iloc[0]["author_handle"] == "allowed"

    def test_handles_empty_dataframe(self, service):
        """Test that deduplicate_and_filter_posts handles empty DataFrame."""
        # Arrange
        posts_df = pd.DataFrame({
            "uri": [],
            "consolidation_timestamp": [],
            "author_did": [],
            "author_handle": [],
        })

        with patch(
            "services.rank_score_feeds.services.data_loading.load_users_to_exclude",
            return_value={"bsky_handles_to_exclude": set(), "bsky_dids_to_exclude": set()},
        ):
            # Act
            result = service.deduplicate_and_filter_posts(posts_df)

        # Assert
        assert len(result) == 0
        assert "uri" in result.columns
        assert "consolidation_timestamp" in result.columns
        assert "author_did" in result.columns
        assert "author_handle" in result.columns


class TestTransformToLoadedData:
    """Tests for DataTransformationService.transform_to_loaded_data."""

    @pytest.fixture
    def service(self):
        """Create service."""
        return DataTransformationService(feed_config=FeedConfig(freshness_lookback_days=7))

    @pytest.fixture
    def sample_raw_data(self):
        """Create sample raw data."""
        return RawFeedData(
            study_users=[
                UserToBlueskyProfileModel(
                    study_user_id="1",
                    bluesky_handle="user1",
                    bluesky_user_did="did:user1",
                    condition="engagement",
                    is_study_user=True,
                    created_timestamp="2024-01-01T00:00:00",
                )
            ],
            feed_input_data=FeedInputData(
                consolidate_enrichment_integrations=pd.DataFrame({"uri": ["post1"]}),
                scraped_user_social_network={"did:1": ["did:2"]},
                superposters={"did:sp"},
            ),
            latest_feeds=LatestFeeds(feeds={"user1": {"post1", "post2"}}),
        )

    def test_transforms_raw_data_to_loaded_data(self, service, sample_raw_data):
        """Test that transform_to_loaded_data correctly transforms RawFeedData to LoadedData."""
        # Arrange
        filtered_users = sample_raw_data.study_users

        # Act
        result = service.transform_to_loaded_data(sample_raw_data, filtered_users)

        # Assert
        assert isinstance(result, LoadedData)
        assert len(result.posts_df) == 1
        assert result.posts_df.iloc[0]["uri"] == "post1"
        assert result.user_to_social_network_map == {"did:1": ["did:2"]}
        assert result.superposter_dids == {"did:sp"}
        assert result.previous_feeds == sample_raw_data.latest_feeds
        assert result.study_users == filtered_users

    def test_uses_filtered_users_in_result(self, service, sample_raw_data):
        """Test that transform_to_loaded_data uses the filtered users provided."""
        # Arrange
        filtered_users = [
            UserToBlueskyProfileModel(
                study_user_id="2",
                bluesky_handle="user2",
                bluesky_user_did="did:user2",
                condition="representative_diversification",
                is_study_user=True,
                created_timestamp="2024-01-01T00:00:00",
            )
        ]

        # Act
        result = service.transform_to_loaded_data(sample_raw_data, filtered_users)

        # Assert
        assert result.study_users == filtered_users
        assert len(result.study_users) == 1
        assert result.study_users[0].bluesky_handle == "user2"

    def test_uses_provided_posts_df_when_specified(self, service, sample_raw_data):
        """Test that transform_to_loaded_data uses provided posts_df instead of raw_data."""
        # Arrange
        filtered_users = sample_raw_data.study_users
        custom_posts_df = pd.DataFrame({"uri": ["post2", "post3"]})

        # Act
        result = service.transform_to_loaded_data(
            sample_raw_data, filtered_users, posts_df=custom_posts_df
        )

        # Assert
        assert isinstance(result, LoadedData)
        assert len(result.posts_df) == 2
        assert result.posts_df.iloc[0]["uri"] == "post2"
        assert result.posts_df.iloc[1]["uri"] == "post3"
        assert result.user_to_social_network_map == {"did:1": ["did:2"]}
        assert result.superposter_dids == {"did:sp"}
        assert result.previous_feeds == sample_raw_data.latest_feeds
        assert result.study_users == filtered_users


class TestFeedDataLoaderInit:
    """Tests for FeedDataLoader.__init__."""

    def test_init_creates_services(self):
        """Test that __init__ creates DataLoadingService and DataTransformationService."""
        # Arrange
        feed_config = FeedConfig(freshness_lookback_days=7)
        mock_logger = Mock()

        with patch(
            "services.rank_score_feeds.services.data_loading.get_logger",
            return_value=mock_logger,
        ), patch(
            "services.rank_score_feeds.services.data_loading.DataLoadingService"
        ) as mock_data_loading, patch(
            "services.rank_score_feeds.services.data_loading.DataTransformationService"
        ) as mock_data_transformation:
            mock_data_loading_instance = Mock()
            mock_data_transformation_instance = Mock()
            mock_data_loading.return_value = mock_data_loading_instance
            mock_data_transformation.return_value = mock_data_transformation_instance

            # Act
            loader = FeedDataLoader(feed_config=feed_config)

        # Assert
        assert loader.config == feed_config
        assert loader.logger is mock_logger
        assert loader.data_loading_service is mock_data_loading_instance
        assert loader.data_transformation_service is mock_data_transformation_instance
        mock_data_loading.assert_called_once_with(feed_config=feed_config)
        mock_data_transformation.assert_called_once_with(feed_config=feed_config)


class TestLoadCompleteData:
    """Tests for FeedDataLoader.load_complete_data."""

    @pytest.fixture
    def loader(self):
        """Create loader with mocked services."""
        loader = FeedDataLoader(feed_config=FeedConfig(freshness_lookback_days=7))
        loader.data_loading_service = Mock()
        loader.data_transformation_service = Mock()
        loader.logger = Mock()
        return loader

    @pytest.fixture
    def sample_raw_data(self):
        """Create sample raw data."""
        return RawFeedData(
            study_users=[
                UserToBlueskyProfileModel(
                    study_user_id="1",
                    bluesky_handle="user1",
                    bluesky_user_did="did:user1",
                    condition="engagement",
                    is_study_user=True,
                    created_timestamp="2024-01-01T00:00:00",
                )
            ],
            feed_input_data=FeedInputData(
                consolidate_enrichment_integrations=pd.DataFrame({"uri": ["post1"]}),
                scraped_user_social_network={"did:1": ["did:2"]},
                superposters={"did:sp"},
            ),
            latest_feeds=LatestFeeds(feeds={"user1": {"post1"}}),
        )

    def test_loads_complete_data_without_user_filtering(self, loader, sample_raw_data):
        """Test that load_complete_data loads data when no user filtering is specified."""
        # Arrange
        expected_loaded_data = LoadedData(
            posts_df=sample_raw_data.feed_input_data.consolidate_enrichment_integrations,
            user_to_social_network_map=sample_raw_data.feed_input_data.scraped_user_social_network,
            superposter_dids=sample_raw_data.feed_input_data.superposters,
            previous_feeds=sample_raw_data.latest_feeds,
            study_users=sample_raw_data.study_users,
        )

        loader.data_loading_service.load_raw_data.return_value = sample_raw_data
        loader.data_transformation_service.filter_study_users.return_value = (
            sample_raw_data.study_users
        )
        loader.data_transformation_service.transform_to_loaded_data.return_value = (
            expected_loaded_data
        )

        # Act
        result = loader.load_complete_data(test_mode=False, users_to_create_feeds_for=None)

        # Assert
        assert result == expected_loaded_data
        loader.data_loading_service.load_raw_data.assert_called_once_with(test_mode=False)
        loader.data_transformation_service.filter_study_users.assert_called_once_with(
            sample_raw_data.study_users, None
        )
        loader.data_transformation_service.transform_to_loaded_data.assert_called_once_with(
            sample_raw_data, sample_raw_data.study_users, posts_df=None
        )

    def test_loads_complete_data_with_user_filtering(self, loader, sample_raw_data):
        """Test that load_complete_data filters users when specified."""
        # Arrange
        filtered_users = [sample_raw_data.study_users[0]]
        expected_loaded_data = LoadedData(
            posts_df=sample_raw_data.feed_input_data.consolidate_enrichment_integrations,
            user_to_social_network_map=sample_raw_data.feed_input_data.scraped_user_social_network,
            superposter_dids=sample_raw_data.feed_input_data.superposters,
            previous_feeds=sample_raw_data.latest_feeds,
            study_users=filtered_users,
        )

        loader.data_loading_service.load_raw_data.return_value = sample_raw_data
        loader.data_transformation_service.filter_study_users.return_value = filtered_users
        loader.data_transformation_service.transform_to_loaded_data.return_value = (
            expected_loaded_data
        )

        # Act
        result = loader.load_complete_data(
            test_mode=True, users_to_create_feeds_for=["user1"]
        )

        # Assert
        assert result == expected_loaded_data
        loader.data_loading_service.load_raw_data.assert_called_once_with(test_mode=True)
        loader.data_transformation_service.filter_study_users.assert_called_once_with(
            sample_raw_data.study_users, ["user1"]
        )
        loader.data_transformation_service.transform_to_loaded_data.assert_called_once_with(
            sample_raw_data, filtered_users, posts_df=None
        )

    def test_loads_complete_data_with_deduplication(self, loader, sample_raw_data):
        """Test that load_complete_data deduplicates posts when deduplicate_posts=True."""
        # Arrange
        deduplicated_posts_df = pd.DataFrame({"uri": ["post1"]})
        filtered_users = [sample_raw_data.study_users[0]]
        expected_loaded_data = LoadedData(
            posts_df=deduplicated_posts_df,
            user_to_social_network_map=sample_raw_data.feed_input_data.scraped_user_social_network,
            superposter_dids=sample_raw_data.feed_input_data.superposters,
            previous_feeds=sample_raw_data.latest_feeds,
            study_users=filtered_users,
        )

        loader.data_loading_service.load_raw_data.return_value = sample_raw_data
        loader.data_transformation_service.filter_study_users.return_value = filtered_users
        loader.data_transformation_service.deduplicate_and_filter_posts.return_value = (
            deduplicated_posts_df
        )
        loader.data_transformation_service.transform_to_loaded_data.return_value = (
            expected_loaded_data
        )

        # Act
        result = loader.load_complete_data(
            test_mode=False, users_to_create_feeds_for=None, deduplicate_posts=True
        )

        # Assert
        assert result == expected_loaded_data
        loader.data_loading_service.load_raw_data.assert_called_once_with(test_mode=False)
        loader.data_transformation_service.filter_study_users.assert_called_once_with(
            sample_raw_data.study_users, None
        )
        loader.data_transformation_service.deduplicate_and_filter_posts.assert_called_once_with(
            sample_raw_data.feed_input_data.consolidate_enrichment_integrations
        )
        loader.data_transformation_service.transform_to_loaded_data.assert_called_once_with(
            sample_raw_data, filtered_users, posts_df=deduplicated_posts_df
        )


class TestDeduplicateAndFilterPostsFacade:
    """Tests for FeedDataLoader.deduplicate_and_filter_posts."""

    @pytest.fixture
    def loader(self):
        """Create loader with mocked transformation service."""
        loader = FeedDataLoader(feed_config=FeedConfig(freshness_lookback_days=7))
        loader.data_transformation_service = Mock()
        return loader

    def test_delegates_to_transformation_service(self, loader):
        """Test that deduplicate_and_filter_posts delegates to DataTransformationService."""
        # Arrange
        input_df = pd.DataFrame({"uri": ["post1"], "author_did": ["did:1"]})
        expected_df = pd.DataFrame({"uri": ["post1"], "author_did": ["did:1"]})
        loader.data_transformation_service.deduplicate_and_filter_posts.return_value = (
            expected_df
        )

        # Act
        result = loader.deduplicate_and_filter_posts(input_df)

        # Assert
        assert result is expected_df
        loader.data_transformation_service.deduplicate_and_filter_posts.assert_called_once_with(
            input_df
        )
