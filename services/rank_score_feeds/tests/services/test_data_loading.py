"""Tests for DataLoadingService."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import FeedInputData, LatestFeeds
from services.rank_score_feeds.services.data_loading import DataLoadingService
from services.calculate_superposters.models import CalculateSuperposterSource
from lib.datetime_utils import TimestampFormat


class TestInit:
    """Tests for DataLoadingService.__init__."""

    def test_init_sets_dependencies(self):
        """Service initializes logger and Athena, and stores config."""
        # Arrange
        feed_config = FeedConfig(freshness_lookback_days=7)
        mock_logger = Mock()
        mock_athena = Mock()

        # Patch get_logger before any imports that might call it at module level
        # This includes S3 (imported by athena) and athena itself
        with patch("lib.log.logger.get_logger", return_value=mock_logger) as p_logger, patch(
            "lib.aws.athena.Athena", return_value=mock_athena
        ) as p_athena:
            # Act
            service = DataLoadingService(feed_config=feed_config)

        # Assert
        assert service.config == feed_config
        assert service.logger is mock_logger
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
