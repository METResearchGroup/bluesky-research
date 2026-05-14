"""Unit tests for get_posts_liked_by_study_users helpers."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd

from lib.db.models import StorageTier
from services.get_posts_liked_by_study_users import helper as helper_module
from services.get_posts_liked_by_study_users.constants import service_name
from services.get_posts_liked_by_study_users.helper import (
    get_and_export_liked_posts_for_partition_date,
    get_and_export_liked_posts_for_partition_dates,
    load_likes_for_lookback_range,
    load_likes_for_partition_date,
    load_raw_posts_for_likes_from_partition_date,
    load_raw_posts_with_lookback,
    load_replies_with_lookback,
    load_reposts_with_lookback,
)


def _assert_raw_sync_load_calls(mock_load: MagicMock, record_type: str) -> None:
    """Assert load_data_from_local_storage was used with ACTIVE then CACHE."""
    assert mock_load.call_count == 2
    for tier in (StorageTier.ACTIVE, StorageTier.CACHE):
        mock_load.assert_any_call(
            service="raw_sync",
            storage_tiers=[tier],
            custom_args={"record_type": record_type},
            start_partition_date="2024-10-01",
            end_partition_date="2024-10-05",
        )


class TestLoadRawPostsWithLookback:
    """Tests for load_raw_posts_with_lookback function."""

    @patch.object(helper_module, "load_data_from_local_storage")
    def test_concatenates_active_and_cache_for_posts(
        self, mock_load: MagicMock
    ) -> None:
        """Loads raw_sync posts from ACTIVE and CACHE tiers and concatenates."""
        active_df = pd.DataFrame({"uri": ["u1"], "synctimestamp": ["2024-10-01T12:00:00"]})
        cache_df = pd.DataFrame({"uri": ["u2"], "synctimestamp": ["2024-10-02T12:00:00"]})
        mock_load.side_effect = [active_df, cache_df]

        result = load_raw_posts_with_lookback("2024-10-01", "2024-10-05")

        _assert_raw_sync_load_calls(mock_load, "post")
        assert len(result) == 2
        assert set(result["uri"].tolist()) == {"u1", "u2"}


class TestLoadRepliesWithLookback:
    """Tests for load_replies_with_lookback function."""

    @patch.object(helper_module, "load_data_from_local_storage")
    def test_concatenates_active_and_cache_for_replies(
        self, mock_load: MagicMock
    ) -> None:
        """Loads raw_sync replies from both tiers."""
        mock_load.side_effect = [
            pd.DataFrame({"uri": ["r1"]}),
            pd.DataFrame({"uri": ["r2"]}),
        ]

        result = load_replies_with_lookback("2024-10-01", "2024-10-05")

        _assert_raw_sync_load_calls(mock_load, "reply")
        assert len(result) == 2


class TestLoadRepostsWithLookback:
    """Tests for load_reposts_with_lookback function."""

    @patch.object(helper_module, "load_data_from_local_storage")
    def test_concatenates_active_and_cache_for_reposts(
        self, mock_load: MagicMock
    ) -> None:
        """Loads raw_sync reposts from both tiers."""
        mock_load.side_effect = [
            pd.DataFrame({"uri": ["p1"]}),
            pd.DataFrame({"uri": ["p2"]}),
        ]

        result = load_reposts_with_lookback("2024-10-01", "2024-10-05")

        _assert_raw_sync_load_calls(mock_load, "repost")
        assert len(result) == 2


class TestLoadLikesForPartitionDate:
    """Tests for load_likes_for_partition_date function."""

    @patch.object(helper_module, "load_data_from_local_storage")
    def test_partitions_likes_on_single_date(self, mock_load: MagicMock) -> None:
        """Uses the same start and end partition date for likes."""
        mock_load.side_effect = [pd.DataFrame({"subject": []}), pd.DataFrame({})]

        load_likes_for_partition_date("2024-10-03")

        assert mock_load.call_count == 2
        for tier in (StorageTier.ACTIVE, StorageTier.CACHE):
            mock_load.assert_any_call(
                service="raw_sync",
                storage_tiers=[tier],
                custom_args={"record_type": "like"},
                start_partition_date="2024-10-03",
                end_partition_date="2024-10-03",
            )


class TestLoadLikesForLookbackRange:
    """Tests for load_likes_for_lookback_range function."""

    @patch.object(helper_module, "load_data_from_local_storage")
    def test_loads_likes_across_range(self, mock_load: MagicMock) -> None:
        """Passes lookback start/end through to storage loader."""
        mock_load.side_effect = [pd.DataFrame(), pd.DataFrame()]

        load_likes_for_lookback_range("2024-10-01", "2024-10-07")

        assert mock_load.call_count == 2
        for tier in (StorageTier.ACTIVE, StorageTier.CACHE):
            mock_load.assert_any_call(
                service="raw_sync",
                storage_tiers=[tier],
                custom_args={"record_type": "like"},
                start_partition_date="2024-10-01",
                end_partition_date="2024-10-07",
            )


class TestLoadRawPostsForLikesFromPartitionDate:
    """Tests for load_raw_posts_for_likes_from_partition_date function."""

    @patch.object(helper_module, "load_reposts_with_lookback")
    @patch.object(helper_module, "load_replies_with_lookback")
    @patch.object(helper_module, "load_raw_posts_with_lookback")
    @patch.object(helper_module, "load_likes_for_partition_date")
    def test_matches_posts_replies_and_reposts_by_liked_uri(
        self,
        mock_likes: MagicMock,
        mock_posts: MagicMock,
        mock_replies: MagicMock,
        mock_reposts: MagicMock,
    ) -> None:
        """Keeps raw records whose uri was liked on the partition date."""
        liked_uri = "at://did/post1"
        mock_likes.return_value = pd.DataFrame(
            {
                "subject": [
                    json.dumps({"uri": liked_uri}),
                ],
            }
        )
        mock_posts.return_value = pd.DataFrame(
            {
                "uri": [liked_uri, "at://other"],
                "synctimestamp": ["2024-10-05T00:00:00", "2024-10-05T01:00:00"],
            }
        )
        mock_replies.return_value = pd.DataFrame(
            {
                "uri": [liked_uri],
                "synctimestamp": ["2024-10-05T02:00:00"],
            }
        )
        mock_reposts.return_value = pd.DataFrame(
            {
                "uri": [liked_uri],
                "synctimestamp": ["2024-10-05T03:00:00"],
            }
        )

        result = load_raw_posts_for_likes_from_partition_date("2024-10-08")

        assert len(result) == 3
        assert (result["uri"] == liked_uri).all()

    @patch.object(helper_module, "load_reposts_with_lookback")
    @patch.object(helper_module, "load_replies_with_lookback")
    @patch.object(helper_module, "load_raw_posts_with_lookback")
    @patch.object(helper_module, "load_likes_for_partition_date")
    def test_no_likes_yields_empty_result_without_division_error(
        self,
        mock_likes: MagicMock,
        mock_posts: MagicMock,
        mock_replies: MagicMock,
        mock_reposts: MagicMock,
    ) -> None:
        """When there are zero likes, proportion logged is zero and frame is empty."""
        mock_likes.return_value = pd.DataFrame(columns=["subject"])
        mock_posts.return_value = pd.DataFrame(
            {"uri": ["at://x"], "synctimestamp": ["2024-10-05T00:00:00"]}
        )
        mock_replies.return_value = pd.DataFrame(columns=["uri", "synctimestamp"])
        mock_reposts.return_value = pd.DataFrame(columns=["uri", "synctimestamp"])

        result = load_raw_posts_for_likes_from_partition_date("2024-10-08")

        assert result.empty


class TestGetAndExportLikedPostsForPartitionDate:
    """Tests for get_and_export_liked_posts_for_partition_date function."""

    @patch.object(helper_module, "export_data_to_local_storage")
    @patch.object(helper_module, "load_raw_posts_for_likes_from_partition_date")
    def test_skips_export_when_no_matches(
        self, mock_load_posts: MagicMock, mock_export: MagicMock
    ) -> None:
        """Does not call export when the matched dataframe is empty."""
        mock_load_posts.return_value = pd.DataFrame()

        get_and_export_liked_posts_for_partition_date("2024-10-08")

        mock_export.assert_not_called()

    @patch.object(helper_module, "export_data_to_local_storage")
    @patch.object(helper_module, "load_raw_posts_for_likes_from_partition_date")
    def test_exports_non_empty_matches(
        self, mock_load_posts: MagicMock, mock_export: MagicMock
    ) -> None:
        """Exports parquet when there is at least one matched row."""
        mock_load_posts.return_value = pd.DataFrame(
            {
                "uri": ["at://a"],
                "synctimestamp": ["2024-10-08T10:00:00"],
            }
        )

        get_and_export_liked_posts_for_partition_date("2024-10-08")

        mock_export.assert_called_once()
        call_kw = mock_export.call_args.kwargs
        assert call_kw["service"] == service_name
        assert call_kw["export_format"] == "parquet"
        assert len(call_kw["df"]) == 1


class TestGetAndExportLikedPostsForPartitionDates:
    """Tests for get_and_export_liked_posts_for_partition_dates function."""

    @patch.object(helper_module, "get_and_export_liked_posts_for_partition_date")
    @patch.object(helper_module, "get_partition_dates")
    def test_processes_each_partition_date(
        self, mock_dates: MagicMock, mock_one_day: MagicMock
    ) -> None:
        """Iterates export helper for every date from get_partition_dates."""
        mock_dates.return_value = ["2024-10-01", "2024-10-02"]

        get_and_export_liked_posts_for_partition_dates(
            start_date="2024-10-01",
            end_date="2024-10-02",
            exclude_partition_dates=[],
        )

        mock_dates.assert_called_once_with(
            start_date="2024-10-01",
            end_date="2024-10-02",
            exclude_partition_dates=[],
        )
        assert mock_one_day.call_count == 2
        mock_one_day.assert_any_call("2024-10-01")
        mock_one_day.assert_any_call("2024-10-02")
