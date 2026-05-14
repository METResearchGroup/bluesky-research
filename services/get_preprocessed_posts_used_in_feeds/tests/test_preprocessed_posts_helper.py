"""Tests for helper.py in get_preprocessed_posts_used_in_feeds."""

import pandas as pd
from unittest.mock import patch

import pytest

from services.get_preprocessed_posts_used_in_feeds.helper import (
    load_preprocessed_posts_used_in_feeds_for_partition_date,
    get_and_export_preprocessed_posts_used_in_feeds_for_partition_date,
    get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates,
)


@pytest.fixture
def mock_df():
    """Create a mock DataFrame for testing."""
    return pd.DataFrame(
        {
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "partition_date": ["2024-01-01", "2024-01-01"],
        }
    )


class TestLoadPreprocessedPostsUsedInFeedsForPartitionDate:
    """Tests for load_preprocessed_posts_used_in_feeds_for_partition_date function."""

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.calculate_start_end_date_for_lookback"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.load_joined_preprocessed_posts_for_feed_day"
    )
    def test_successful_load(self, mock_load, mock_calc, mock_df):
        """Lookback is computed then joined loader receives partition and window args."""
        # Arrange
        mock_calc.return_value = ("2024-01-01", "2024-01-05")
        mock_load.return_value = mock_df

        # Act
        result = load_preprocessed_posts_used_in_feeds_for_partition_date("2024-01-05")

        # Assert
        mock_calc.assert_called_once_with(
            partition_date="2024-01-05",
            num_days_lookback=5,
            min_lookback_date="2024-09-15",
        )
        mock_load.assert_called_once_with(
            partition_date="2024-01-05",
            lookback_start_date="2024-01-01",
            lookback_end_date="2024-01-05",
            table_columns=None,
            dedupe_uri_keep_first=False,
        )
        assert len(result) == 2
        assert list(result["uri"]) == ["post1", "post2"]

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.calculate_start_end_date_for_lookback"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.load_joined_preprocessed_posts_for_feed_day"
    )
    def test_empty_result(self, mock_load, mock_calc):
        """Empty join result propagates as an empty DataFrame."""
        # Arrange
        mock_calc.return_value = ("2024-01-01", "2024-01-05")
        mock_load.return_value = pd.DataFrame()

        # Act
        result = load_preprocessed_posts_used_in_feeds_for_partition_date("2024-01-05")

        # Assert
        assert len(result) == 0


class TestGetAndExportPreprocessedPostsForPartitionDate:
    """Tests for get_and_export_preprocessed_posts_used_in_feeds_for_partition_date function."""

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.load_preprocessed_posts_used_in_feeds_for_partition_date"
    )
    @patch("services.get_preprocessed_posts_used_in_feeds.helper.export_data_to_local_storage")
    def test_successful_export(self, mock_export, mock_load, mock_df):
        """Exporter forwards the dataframe to local storage with expected service name."""
        # Arrange
        mock_load.return_value = mock_df

        # Act
        get_and_export_preprocessed_posts_used_in_feeds_for_partition_date("2024-01-05")

        # Assert
        mock_load.assert_called_once_with(partition_date="2024-01-05")
        mock_export.assert_called_once_with(
            service="preprocessed_posts_used_in_feeds",
            df=mock_df,
            export_format="parquet",
        )

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.load_preprocessed_posts_used_in_feeds_for_partition_date"
    )
    @patch("services.get_preprocessed_posts_used_in_feeds.helper.export_data_to_local_storage")
    def test_empty_export(self, mock_export, mock_load):
        """Empty dataframe is still exported (downstream may write an empty partition)."""
        # Arrange
        mock_load.return_value = pd.DataFrame()

        # Act
        get_and_export_preprocessed_posts_used_in_feeds_for_partition_date("2024-01-05")

        # Assert
        mock_export.assert_called_once()


class TestGetAndExportPreprocessedPostsForPartitionDates:
    """Tests for get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates function."""

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.get_partition_dates"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.get_and_export_preprocessed_posts_used_in_feeds_for_partition_date"
    )
    def test_multiple_dates(self, mock_export_single, mock_get_dates):
        """Each partition date in the resolved range triggers one export call."""
        # Arrange
        mock_get_dates.return_value = ["2024-01-01", "2024-01-02"]

        # Act
        get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-02",
        )

        # Assert
        assert mock_export_single.call_count == 2
        mock_export_single.assert_any_call(partition_date="2024-01-01")
        mock_export_single.assert_any_call(partition_date="2024-01-02")

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.get_partition_dates"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.get_and_export_preprocessed_posts_used_in_feeds_for_partition_date"
    )
    def test_excluded_dates(self, mock_export_single, mock_get_dates):
        """Exclude list is forwarded to get_partition_dates unchanged."""
        # Arrange
        mock_get_dates.return_value = ["2024-01-01", "2024-01-03"]

        # Act
        get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-03",
            exclude_partition_dates=["2024-01-02"],
        )

        # Assert
        mock_get_dates.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-03",
            exclude_partition_dates=["2024-01-02"],
        )
        assert mock_export_single.call_count == 2

    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.get_partition_dates"
    )
    @patch(
        "services.get_preprocessed_posts_used_in_feeds.helper.get_and_export_preprocessed_posts_used_in_feeds_for_partition_date"
    )
    def test_no_dates(self, mock_export_single, mock_get_dates):
        """When no dates remain after filtering, nothing is exported."""
        # Arrange
        mock_get_dates.return_value = []

        # Act
        get_and_export_preprocessed_posts_used_in_feeds_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-01",
        )

        # Assert
        mock_export_single.assert_not_called()
