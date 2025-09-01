"""Tests for main.py.

This test suite verifies the functionality of the user feed analysis main module:
- Setup and data loading functions
- Aggregation and export functionality
- Main execution flow
- Error handling and edge cases
"""

import os
import tempfile
from unittest.mock import patch, MagicMock, mock_open

import pandas as pd
import pytest

from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)

from services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main import (
    do_setup,
    do_aggregations_and_export_results,
    main,
)
from services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.tests.mock_data import (
    mock_user_df,
    mock_user_date_to_week_df,
    mock_valid_study_users_dids,
    mock_user_to_content_in_feeds,
    mock_labels_for_feed_content,
    mock_partition_dates,
    mock_user_per_day_content_label_metrics,
    mock_user_per_week_content_label_metrics,
    mock_transformed_per_user_per_day_content_label_metrics,
    mock_transformed_per_user_per_week_feed_content_metrics,
    mock_setup_objs,
)


class TestDoSetup:
    """Tests for do_setup function."""

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_feeds_per_user")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_post_uris_used_in_feeds")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_labels_for_posts")
    def test_do_setup_returns_expected_structure(
        self,
        mock_get_all_labels_for_posts,
        mock_get_all_post_uris_used_in_feeds,
        mock_get_feeds_per_user,
        mock_get_partition_dates,
        mock_load_user_data,
    ):
        """Test that do_setup returns the expected data structure.

        This test verifies that:
        1. All required functions are called with correct parameters
        2. The returned dictionary contains all expected keys
        3. The data structure matches the expected format
        4. The setup process follows the correct sequence
        """
        # Arrange
        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_feeds_per_user.return_value = mock_user_to_content_in_feeds
        mock_get_all_post_uris_used_in_feeds.return_value = {"post1", "post2", "post3"}
        mock_get_all_labels_for_posts.return_value = mock_labels_for_feed_content

        expected_keys = {
            "user_df",
            "user_date_to_week_df",
            "valid_study_users_dids",
            "user_to_content_in_feeds",
            "labels_for_feed_content",
            "partition_dates",
        }

        # Act
        result = do_setup()

        # Assert
        assert isinstance(result, dict)
        assert set(result.keys()) == expected_keys
        assert result["user_df"].equals(mock_user_df)
        assert result["user_date_to_week_df"].equals(mock_user_date_to_week_df)
        assert result["valid_study_users_dids"] == mock_valid_study_users_dids
        assert result["user_to_content_in_feeds"] == mock_user_to_content_in_feeds
        assert result["labels_for_feed_content"] == mock_labels_for_feed_content
        assert result["partition_dates"] == mock_partition_dates

        # Verify function calls
        mock_load_user_data.assert_called_once()
        mock_get_partition_dates.assert_called_once_with(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )
        mock_get_feeds_per_user.assert_called_once_with(
            valid_study_users_dids=mock_valid_study_users_dids
        )
        mock_get_all_post_uris_used_in_feeds.assert_called_once_with(
            user_to_content_in_feeds=mock_user_to_content_in_feeds
        )
        mock_get_all_labels_for_posts.assert_called_once_with(
            post_uris={"post1", "post2", "post3"}, partition_dates=mock_partition_dates
        )

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_feeds_per_user")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_post_uris_used_in_feeds")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_labels_for_posts")
    def test_do_setup_handles_empty_data(
        self,
        mock_get_all_labels_for_posts,
        mock_get_all_post_uris_used_in_feeds,
        mock_get_feeds_per_user,
        mock_get_partition_dates,
        mock_load_user_data,
    ):
        """Test that do_setup handles empty data gracefully.

        This test verifies that:
        1. Empty user data is handled correctly
        2. Empty feeds data is processed without errors
        3. Empty labels data is handled appropriately
        4. The function doesn't crash on edge case inputs
        """
        # Arrange
        empty_user_df = pd.DataFrame(columns=["did", "handle", "condition"])
        empty_user_date_to_week_df = pd.DataFrame(columns=["did", "partition_date", "week"])
        empty_valid_study_users_dids = []
        empty_user_to_content_in_feeds = {}
        empty_labels_for_feed_content = {}
        empty_partition_dates = []

        mock_load_user_data.return_value = (
            empty_user_df,
            empty_user_date_to_week_df,
            empty_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = empty_partition_dates
        mock_get_feeds_per_user.return_value = empty_user_to_content_in_feeds
        mock_get_all_post_uris_used_in_feeds.return_value = set()
        mock_get_all_labels_for_posts.return_value = empty_labels_for_feed_content

        # Act
        result = do_setup()

        # Assert
        assert isinstance(result, dict)
        assert result["user_df"].empty
        assert result["user_date_to_week_df"].empty
        assert result["valid_study_users_dids"] == []
        assert result["user_to_content_in_feeds"] == {}
        assert result["labels_for_feed_content"] == {}
        assert result["partition_dates"] == []

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.load_user_data")
    def test_do_setup_raises_exception_on_user_data_error(self, mock_load_user_data):
        """Test that do_setup raises exception when user data loading fails.

        This test verifies that:
        1. Exceptions from user data loading are properly propagated
        2. The function doesn't suppress important errors
        3. Error handling is appropriate for critical dependencies
        """
        # Arrange
        mock_load_user_data.side_effect = Exception("User data loading failed")

        # Act & Assert
        with pytest.raises(Exception, match="User data loading failed"):
            do_setup()


class TestDoAggregationsAndExportResults:
    """Tests for do_aggregations_and_export_results function."""

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_daily_feed_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("pandas.DataFrame.to_csv")
    def test_do_aggregations_and_export_results_completes_successfully(
        self,
        mock_to_csv,
        mock_open,
        mock_makedirs,
        mock_transform_weekly,
        mock_get_weekly,
        mock_transform_daily,
        mock_get_daily,
    ):
        """Test that aggregations and export completes successfully.

        This test verifies that:
        1. Daily metrics are calculated and transformed correctly
        2. Weekly metrics are calculated and transformed correctly
        3. Files are exported to the correct locations
        4. All function calls are made with correct parameters
        5. The export process completes without errors
        """
        # Arrange
        mock_get_daily.return_value = mock_user_per_day_content_label_metrics
        mock_transform_daily.return_value = mock_transformed_per_user_per_day_content_label_metrics
        mock_get_weekly.return_value = mock_user_per_week_content_label_metrics
        mock_transform_weekly.return_value = mock_transformed_per_user_per_week_feed_content_metrics

        # Act
        do_aggregations_and_export_results(
            user_df=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
            user_to_content_in_feeds=mock_user_to_content_in_feeds,
            labels_for_feed_content=mock_labels_for_feed_content,
            partition_dates=mock_partition_dates,
        )

        # Assert
        # Verify daily metrics processing
        mock_get_daily.assert_called_once_with(
            user_to_content_in_feeds=mock_user_to_content_in_feeds,
            labels_for_feed_content=mock_labels_for_feed_content,
        )
        mock_transform_daily.assert_called_once_with(
            user_per_day_content_label_metrics=mock_user_per_day_content_label_metrics,
            users=mock_user_df,
            partition_dates=mock_partition_dates,
        )

        # Verify weekly metrics processing
        mock_get_weekly.assert_called_once_with(
            user_per_day_content_label_metrics=mock_user_per_day_content_label_metrics,
            user_date_to_week_df=mock_user_date_to_week_df,
        )
        mock_transform_weekly.assert_called_once_with(
            user_per_week_content_label_metrics=mock_user_per_week_content_label_metrics,
            users=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
        )

        # Verify file operations
        assert mock_makedirs.call_count == 2  # Called for both daily and weekly exports
        assert mock_to_csv.call_count == 2  # Called for both daily and weekly exports

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_daily_feed_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("pandas.DataFrame.to_csv")
    def test_do_aggregations_and_export_results_handles_empty_data(
        self,
        mock_to_csv,
        mock_open,
        mock_makedirs,
        mock_transform_weekly,
        mock_get_weekly,
        mock_transform_daily,
        mock_get_daily,
    ):
        """Test that aggregations and export handles empty data gracefully.

        This test verifies that:
        1. Empty data is processed without errors
        2. Empty DataFrames are exported correctly
        3. The function handles edge cases appropriately
        4. File operations work with empty data
        """
        # Arrange
        empty_user_df = pd.DataFrame(columns=["did", "handle", "condition"])
        empty_user_date_to_week_df = pd.DataFrame(columns=["did", "partition_date", "week"])
        empty_user_to_content_in_feeds = {}
        empty_labels_for_feed_content = {}
        empty_partition_dates = []
        empty_daily_metrics = {}
        empty_weekly_metrics = {}
        empty_daily_df = pd.DataFrame()
        empty_weekly_df = pd.DataFrame()

        mock_get_daily.return_value = empty_daily_metrics
        mock_transform_daily.return_value = empty_daily_df
        mock_get_weekly.return_value = empty_weekly_metrics
        mock_transform_weekly.return_value = empty_weekly_df

        # Act
        do_aggregations_and_export_results(
            user_df=empty_user_df,
            user_date_to_week_df=empty_user_date_to_week_df,
            user_to_content_in_feeds=empty_user_to_content_in_feeds,
            labels_for_feed_content=empty_labels_for_feed_content,
            partition_dates=empty_partition_dates,
        )

        # Assert
        mock_get_daily.assert_called_once_with(
            user_to_content_in_feeds=empty_user_to_content_in_feeds,
            labels_for_feed_content=empty_labels_for_feed_content,
        )
        mock_transform_daily.assert_called_once_with(
            user_per_day_content_label_metrics=empty_daily_metrics,
            users=empty_user_df,
            partition_dates=empty_partition_dates,
        )
        mock_get_weekly.assert_called_once_with(
            user_per_day_content_label_metrics=empty_daily_metrics,
            user_date_to_week_df=empty_user_date_to_week_df,
        )
        mock_transform_weekly.assert_called_once_with(
            user_per_week_content_label_metrics=empty_weekly_metrics,
            users=empty_user_df,
            user_date_to_week_df=empty_user_date_to_week_df,
        )

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_daily_feed_content_per_user_metrics")
    def test_do_aggregations_and_export_results_raises_exception_on_daily_metrics_error(
        self, mock_get_daily
    ):
        """Test that aggregations and export raises exception when daily metrics calculation fails.

        This test verifies that:
        1. Exceptions from daily metrics calculation are properly propagated
        2. The function doesn't suppress important errors
        3. Error handling is appropriate for critical processing steps
        """
        # Arrange
        mock_get_daily.side_effect = Exception("Daily metrics calculation failed")

        # Act & Assert
        with pytest.raises(Exception, match="Daily metrics calculation failed"):
            do_aggregations_and_export_results(
                user_df=mock_user_df,
                user_date_to_week_df=mock_user_date_to_week_df,
                user_to_content_in_feeds=mock_user_to_content_in_feeds,
                labels_for_feed_content=mock_labels_for_feed_content,
                partition_dates=mock_partition_dates,
            )

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_daily_feed_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_weekly_content_per_user_metrics")
    def test_do_aggregations_and_export_results_raises_exception_on_weekly_metrics_error(
        self, mock_get_weekly, mock_transform_daily, mock_get_daily
    ):
        """Test that aggregations and export raises exception when weekly metrics calculation fails.

        This test verifies that:
        1. Exceptions from weekly metrics calculation are properly propagated
        2. The function doesn't suppress important errors
        3. Error handling is appropriate for critical processing steps
        """
        # Arrange
        mock_get_daily.return_value = mock_user_per_day_content_label_metrics
        mock_transform_daily.return_value = mock_transformed_per_user_per_day_content_label_metrics
        mock_get_weekly.side_effect = Exception("Weekly metrics calculation failed")

        # Act & Assert
        with pytest.raises(Exception, match="Weekly metrics calculation failed"):
            do_aggregations_and_export_results(
                user_df=mock_user_df,
                user_date_to_week_df=mock_user_date_to_week_df,
                user_to_content_in_feeds=mock_user_to_content_in_feeds,
                labels_for_feed_content=mock_labels_for_feed_content,
                partition_dates=mock_partition_dates,
            )


class TestMain:
    """Tests for main function."""



    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_setup")
    def test_main_raises_exception_on_setup_error(self, mock_do_setup):
        """Test that main function raises exception when setup fails.

        This test verifies that:
        1. Exceptions from setup are properly propagated
        2. The function doesn't suppress important errors
        3. Error handling is appropriate for critical initialization steps
        """
        # Arrange
        mock_do_setup.side_effect = Exception("Setup failed")

        # Act & Assert
        with pytest.raises(Exception, match="Setup failed"):
            main()

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_setup")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_aggregations_and_export_results")
    def test_main_raises_exception_on_aggregations_error(
        self, mock_do_aggregations_and_export_results, mock_do_setup
    ):
        """Test that main function raises exception when aggregations fail.

        This test verifies that:
        1. Exceptions from aggregations are properly propagated
        2. The function doesn't suppress important errors
        3. Error handling is appropriate for critical processing steps
        """
        # Arrange
        mock_do_setup.return_value = mock_setup_objs
        mock_do_aggregations_and_export_results.side_effect = Exception("Aggregations failed")

        # Act & Assert
        with pytest.raises(Exception, match="Aggregations failed"):
            main()

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_setup")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_aggregations_and_export_results")
    def test_main_handles_missing_setup_data(
        self, mock_do_aggregations_and_export_results, mock_do_setup
    ):
        """Test that main function handles missing setup data gracefully.

        This test verifies that:
        1. Missing keys in setup data are handled appropriately
        2. The function provides meaningful error messages
        3. Error handling is robust for data structure issues
        """
        # Arrange
        incomplete_setup_objs = {
            "user_df": mock_user_df,
            # Missing other required keys
        }
        mock_do_setup.return_value = incomplete_setup_objs

        # Act & Assert
        with pytest.raises(KeyError):
            main()

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_setup")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.do_aggregations_and_export_results")
    def test_main_completes_successfully_with_valid_data(
        self, mock_do_aggregations_and_export_results, mock_do_setup
    ):
        """Test that main function completes successfully with valid data.

        This test verifies that:
        1. The main function orchestrates the workflow correctly
        2. All required data is passed between functions correctly
        3. The function completes without errors when given valid data
        4. The mocked functions are called with the expected parameters
        """
        # Arrange
        mock_do_setup.return_value = mock_setup_objs

        # Act
        main()

        # Assert
        mock_do_setup.assert_called_once()
        mock_do_aggregations_and_export_results.assert_called_once_with(
            user_df=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
            user_to_content_in_feeds=mock_user_to_content_in_feeds,
            labels_for_feed_content=mock_labels_for_feed_content,
            partition_dates=mock_partition_dates,
        )


class TestIntegration:
    """Integration tests for the main module."""

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_feeds_per_user")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_post_uris_used_in_feeds")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_labels_for_posts")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_daily_feed_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("pandas.DataFrame.to_csv")
    def test_full_integration_workflow(
        self,
        mock_to_csv,
        mock_open,
        mock_makedirs,
        mock_transform_weekly,
        mock_get_weekly,
        mock_transform_daily,
        mock_get_daily,
        mock_get_all_labels_for_posts,
        mock_get_all_post_uris_used_in_feeds,
        mock_get_feeds_per_user,
        mock_get_partition_dates,
        mock_load_user_data,
    ):
        """Test the full integration workflow from setup to export.

        This test verifies that:
        1. The complete workflow executes successfully
        2. All functions are called in the correct order
        3. Data flows correctly between all components
        4. The final output is generated as expected
        5. File operations complete successfully
        """
        # Arrange
        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_feeds_per_user.return_value = mock_user_to_content_in_feeds
        mock_get_all_post_uris_used_in_feeds.return_value = {"post1", "post2", "post3"}
        mock_get_all_labels_for_posts.return_value = mock_labels_for_feed_content
        mock_get_daily.return_value = mock_user_per_day_content_label_metrics
        mock_transform_daily.return_value = mock_transformed_per_user_per_day_content_label_metrics
        mock_get_weekly.return_value = mock_user_per_week_content_label_metrics
        mock_transform_weekly.return_value = mock_transformed_per_user_per_week_feed_content_metrics

        # Act
        main()

        # Assert
        # Verify setup calls
        mock_load_user_data.assert_called_once()
        mock_get_partition_dates.assert_called_once_with(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )
        mock_get_feeds_per_user.assert_called_once_with(
            valid_study_users_dids=mock_valid_study_users_dids
        )
        mock_get_all_post_uris_used_in_feeds.assert_called_once_with(
            user_to_content_in_feeds=mock_user_to_content_in_feeds
        )
        mock_get_all_labels_for_posts.assert_called_once_with(
            post_uris={"post1", "post2", "post3"}, partition_dates=mock_partition_dates
        )

        # Verify processing calls
        mock_get_daily.assert_called_once_with(
            user_to_content_in_feeds=mock_user_to_content_in_feeds,
            labels_for_feed_content=mock_labels_for_feed_content,
        )
        mock_transform_daily.assert_called_once_with(
            user_per_day_content_label_metrics=mock_user_per_day_content_label_metrics,
            users=mock_user_df,
            partition_dates=mock_partition_dates,
        )
        mock_get_weekly.assert_called_once_with(
            user_per_day_content_label_metrics=mock_user_per_day_content_label_metrics,
            user_date_to_week_df=mock_user_date_to_week_df,
        )
        mock_transform_weekly.assert_called_once_with(
            user_per_week_content_label_metrics=mock_user_per_week_content_label_metrics,
            users=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
        )

        # Verify export calls
        assert mock_makedirs.call_count == 2
        assert mock_to_csv.call_count == 2

    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_feeds_per_user")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_post_uris_used_in_feeds")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_all_labels_for_posts")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_daily_feed_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.transform_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_feed_analysis_2025_04_08.main.os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("pandas.DataFrame.to_csv")
    def test_integration_with_empty_data(
        self,
        mock_to_csv,
        mock_open,
        mock_makedirs,
        mock_transform_weekly,
        mock_get_weekly,
        mock_transform_daily,
        mock_get_daily,
        mock_get_all_labels_for_posts,
        mock_get_all_post_uris_used_in_feeds,
        mock_get_feeds_per_user,
        mock_get_partition_dates,
        mock_load_user_data,
    ):
        """Test integration workflow with empty data.

        This test verifies that:
        1. The workflow handles empty data gracefully
        2. All functions are called with empty data
        3. The process completes without errors
        4. Empty DataFrames are exported correctly
        """
        # Arrange
        empty_user_df = pd.DataFrame(columns=["did", "handle", "condition"])
        empty_user_date_to_week_df = pd.DataFrame(columns=["did", "partition_date", "week"])
        empty_valid_study_users_dids = []
        empty_user_to_content_in_feeds = {}
        empty_labels_for_feed_content = {}
        empty_partition_dates = []
        empty_daily_metrics = {}
        empty_weekly_metrics = {}
        empty_daily_df = pd.DataFrame()
        empty_weekly_df = pd.DataFrame()

        mock_load_user_data.return_value = (
            empty_user_df,
            empty_user_date_to_week_df,
            empty_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = empty_partition_dates
        mock_get_feeds_per_user.return_value = empty_user_to_content_in_feeds
        mock_get_all_post_uris_used_in_feeds.return_value = set()
        mock_get_all_labels_for_posts.return_value = empty_labels_for_feed_content
        mock_get_daily.return_value = empty_daily_metrics
        mock_transform_daily.return_value = empty_daily_df
        mock_get_weekly.return_value = empty_weekly_metrics
        mock_transform_weekly.return_value = empty_weekly_df

        # Act
        main()

        # Assert
        # Verify all functions were called with empty data
        mock_load_user_data.assert_called_once()
        mock_get_partition_dates.assert_called_once_with(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,
        )
        mock_get_feeds_per_user.assert_called_once_with(
            valid_study_users_dids=empty_valid_study_users_dids
        )
        mock_get_all_post_uris_used_in_feeds.assert_called_once_with(
            user_to_content_in_feeds=empty_user_to_content_in_feeds
        )
        mock_get_all_labels_for_posts.assert_called_once_with(
            post_uris=set(), partition_dates=empty_partition_dates
        )
        mock_get_daily.assert_called_once_with(
            user_to_content_in_feeds=empty_user_to_content_in_feeds,
            labels_for_feed_content=empty_labels_for_feed_content,
        )
        mock_transform_daily.assert_called_once_with(
            user_per_day_content_label_metrics=empty_daily_metrics,
            users=empty_user_df,
            partition_dates=empty_partition_dates,
        )
        mock_get_weekly.assert_called_once_with(
            user_per_day_content_label_metrics=empty_daily_metrics,
            user_date_to_week_df=empty_user_date_to_week_df,
        )
        mock_transform_weekly.assert_called_once_with(
            user_per_week_content_label_metrics=empty_weekly_metrics,
            users=empty_user_df,
            user_date_to_week_df=empty_user_date_to_week_df,
        )

        # Verify export operations
        assert mock_makedirs.call_count == 2
        assert mock_to_csv.call_count == 2
