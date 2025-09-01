"""Tests for main.py functionality.

This test suite verifies the functionality of the user engagement analysis main script:
- Setup and data loading functions
- Aggregation and export functions
- Main execution flow
- Error handling and edge cases
- Data validation and integration
"""

import os
import tempfile
from unittest.mock import patch, MagicMock, mock_open

import pandas as pd
import pytest

from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.tests.mock_data import (
    mock_user_df,
    mock_user_date_to_week_df,
    mock_valid_study_users_dids,
    mock_partition_dates,
    mock_engaged_content,
    mock_user_to_content_engaged_with,
    mock_labels_for_engaged_content,
    mock_user_per_day_content_label_metrics,
    mock_user_per_week_content_label_metrics,
    mock_transformed_per_user_per_day_content_label_metrics,
    mock_transformed_user_per_week_content_label_metrics,
    mock_setup_objs,
)


class TestDoSetup:
    """Tests for do_setup function."""

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_engaged_content")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_content_engaged_with_per_user")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_all_labels_for_posts")
    def test_do_setup_returns_expected_structure(
        self,
        mock_get_all_labels_for_posts,
        mock_get_content_engaged_with_per_user,
        mock_get_engaged_content,
        mock_get_partition_dates,
        mock_load_user_data,
    ):
        """Test that do_setup returns the expected data structure.

        This test verifies that:
        1. All required functions are called with correct parameters
        2. The returned dictionary contains all expected keys
        3. The data structure matches the expected format
        4. The setup process completes successfully
        """
        # Arrange
        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_engaged_content.return_value = mock_engaged_content
        mock_get_content_engaged_with_per_user.return_value = mock_user_to_content_engaged_with
        mock_get_all_labels_for_posts.return_value = mock_labels_for_engaged_content

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_setup
        result = do_setup()

        # Assert
        assert isinstance(result, dict)
        assert "user_df" in result
        assert "user_date_to_week_df" in result
        assert "user_to_content_engaged_with" in result
        assert "labels_for_engaged_content" in result
        assert "partition_dates" in result

        # Verify function calls
        mock_load_user_data.assert_called_once()
        mock_get_partition_dates.assert_called_once()
        mock_get_engaged_content.assert_called_once_with(
            valid_study_users_dids=mock_valid_study_users_dids
        )
        mock_get_content_engaged_with_per_user.assert_called_once_with(
            engaged_content=mock_engaged_content
        )
        mock_get_all_labels_for_posts.assert_called_once_with(
            post_uris=list(mock_engaged_content.keys()),
            partition_dates=mock_partition_dates,
        )

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_engaged_content")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_content_engaged_with_per_user")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_all_labels_for_posts")
    def test_handle_empty_engaged_content(self, mock_get_all_labels_for_posts, mock_get_content_engaged_with_per_user, mock_get_engaged_content, mock_get_partition_dates, mock_load_user_data):
        """Test handling when engaged content data is empty.

        This test verifies that:
        1. Empty engaged content is handled gracefully
        2. The function continues to work with empty data
        3. No errors are raised due to empty data structure
        """
        # Arrange
        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        
        # Empty engaged content (empty dict instead of None)
        mock_get_engaged_content.return_value = {}
        mock_get_content_engaged_with_per_user.return_value = {}
        mock_get_all_labels_for_posts.return_value = {}

        # Act & Assert
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_setup
        # Should not raise an exception
        result = do_setup()
        
        # Verify result structure is maintained
        assert isinstance(result, dict)
        assert "user_df" in result
        assert "user_date_to_week_df" in result
        assert "user_to_content_engaged_with" in result
        assert "labels_for_engaged_content" in result
        assert "partition_dates" in result
        
        # Verify empty engaged content is handled correctly
        assert len(result["user_to_content_engaged_with"]) == 0
        assert len(result["labels_for_engaged_content"]) == 0


class TestErrorHandling:
    """Tests for error handling scenarios."""

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    def test_handle_empty_user_data(self, mock_load_user_data):
        """Test handling when load_user_data returns empty DataFrame.

        This test verifies that:
        1. Empty user data is handled gracefully
        2. The function continues to work with empty data
        3. No errors are raised due to empty DataFrames
        """
        # Arrange
        empty_user_df = pd.DataFrame()
        empty_user_date_to_week_df = pd.DataFrame()
        empty_valid_study_users_dids = []
        
        mock_load_user_data.return_value = (
            empty_user_df,
            empty_user_date_to_week_df,
            empty_valid_study_users_dids,
        )

        # Act & Assert
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_setup
        # Should not raise an exception
        result = do_setup()
        
        # Verify result structure is maintained even with empty data
        assert isinstance(result, dict)
        assert "user_df" in result
        assert "user_date_to_week_df" in result
        assert "user_to_content_engaged_with" in result
        assert "labels_for_engaged_content" in result
        assert "partition_dates" in result

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_daily_engaged_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_weekly_content_per_user_metrics")
    @patch("os.makedirs")
    def test_handle_file_io_errors(self, mock_makedirs, mock_transform_weekly, mock_get_weekly, mock_transform_daily, mock_get_daily):
        """Test handling when CSV export fails due to file I/O errors.

        This test verifies that:
        1. File I/O errors are properly handled
        2. The function raises appropriate exceptions
        3. Error handling works for both daily and weekly exports
        """
        # Arrange
        mock_get_daily.return_value = mock_user_per_day_content_label_metrics
        
        # Create mock DataFrame with to_csv method that raises OSError
        mock_daily_df = MagicMock()
        mock_daily_df.to_csv = MagicMock(side_effect=OSError("Disk full"))
        mock_transform_daily.return_value = mock_daily_df
        
        mock_get_weekly.return_value = mock_user_per_week_content_label_metrics
        
        # Create mock DataFrame with to_csv method
        mock_weekly_df = MagicMock()
        mock_weekly_df.to_csv = MagicMock()
        mock_transform_weekly.return_value = mock_weekly_df

        # Act & Assert
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_aggregations_and_export_results
        with pytest.raises(OSError, match="Disk full"):
            do_aggregations_and_export_results(
                user_df=mock_user_df,
                user_date_to_week_df=mock_user_date_to_week_df,
                user_to_content_engaged_with=mock_user_to_content_engaged_with,
                labels_for_engaged_content=mock_labels_for_engaged_content,
                partition_dates=mock_partition_dates,
            )


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_engaged_content")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_content_engaged_with_per_user")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_all_labels_for_posts")
    def test_single_user_engagement(self, mock_get_all_labels_for_posts, mock_get_content_engaged_with_per_user, mock_get_engaged_content, mock_get_partition_dates, mock_load_user_data):
        """Test with only one user having engagement data.

        This test verifies that:
        1. Single user engagement is handled correctly
        2. The function works with minimal data
        3. All processing steps complete successfully
        """
        # Arrange
        single_user_df = pd.DataFrame({"did": ["user1"], "handle": ["user1.bsky.social"]})
        single_user_date_to_week_df = pd.DataFrame({"did": ["user1"], "date": ["2024-09-30"], "week": ["1"]})
        single_valid_study_users_dids = ["user1"]
        
        single_engaged_content = {"post1": [{"uri": "post1", "did": "user1", "date": "2024-09-30", "record_type": "like"}]}
        single_user_to_content_engaged_with = {"user1": {"2024-09-30": {"like": ["post1"], "repost": [], "reply": []}}}
        single_labels_for_engaged_content = {"post1": {"prob_toxic": 0.5, "prob_constructive": 0.7}}

        mock_load_user_data.return_value = (
            single_user_df,
            single_user_date_to_week_df,
            single_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_engaged_content.return_value = single_engaged_content
        mock_get_content_engaged_with_per_user.return_value = single_user_to_content_engaged_with
        mock_get_all_labels_for_posts.return_value = single_labels_for_engaged_content

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_setup
        result = do_setup()

        # Assert
        assert isinstance(result, dict)
        assert len(result["user_df"]) == 1
        assert len(result["user_to_content_engaged_with"]) == 1
        assert len(result["labels_for_engaged_content"]) == 1

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_engaged_content")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_content_engaged_with_per_user")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_all_labels_for_posts")
    def test_no_engagement_data(self, mock_get_all_labels_for_posts, mock_get_content_engaged_with_per_user, mock_get_engaged_content, mock_get_partition_dates, mock_load_user_data):
        """Test when no users have any engagement data.

        This test verifies that:
        1. Zero engagement data is handled correctly
        2. The function works with empty engagement data
        3. All processing steps complete successfully
        """
        # Arrange
        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_engaged_content.return_value = {}
        mock_get_content_engaged_with_per_user.return_value = {}
        mock_get_all_labels_for_posts.return_value = {}

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_setup
        result = do_setup()

        # Assert
        assert isinstance(result, dict)
        assert len(result["user_to_content_engaged_with"]) == 0
        assert len(result["labels_for_engaged_content"]) == 0

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_engaged_content")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_content_engaged_with_per_user")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_all_labels_for_posts")
    def test_missing_labels(self, mock_get_all_labels_for_posts, mock_get_content_engaged_with_per_user, mock_get_engaged_content, mock_get_partition_dates, mock_load_user_data):
        """Test when some posts have missing labels.

        This test verifies that:
        1. Missing labels are handled gracefully
        2. The function continues to work with partial label data
        3. No errors are raised due to missing labels
        """
        # Arrange
        engaged_content_with_missing_labels = {
            "post1": [{"uri": "post1", "did": "user1", "date": "2024-09-30", "record_type": "like"}],
            "post2": [{"uri": "post2", "did": "user1", "date": "2024-09-30", "record_type": "repost"}]
        }
        labels_with_missing = {"post1": {"prob_toxic": 0.5}}  # post2 missing

        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_engaged_content.return_value = engaged_content_with_missing_labels
        mock_get_content_engaged_with_per_user.return_value = {"user1": {"2024-09-30": {"like": ["post1"], "repost": ["post2"], "reply": []}}}
        mock_get_all_labels_for_posts.return_value = labels_with_missing

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_setup
        result = do_setup()

        # Assert
        assert isinstance(result, dict)
        assert len(result["labels_for_engaged_content"]) == 1  # Only post1 has labels
        assert "post1" in result["labels_for_engaged_content"]
        assert "post2" not in result["labels_for_engaged_content"]


class TestDataValidation:
    """Tests for data validation and integrity."""

    def test_user_data_structure(self):
        """Test that user data has required columns.

        This test verifies that:
        1. User DataFrame contains expected columns
        2. Data types are correct
        3. Required fields are present
        """
        # Arrange
        expected_columns = ["did", "handle", "study_group", "enrollment_date"]
        
        # Act
        user_df = mock_user_df
        
        # Assert
        assert isinstance(user_df, pd.DataFrame)
        for column in expected_columns:
            assert column in user_df.columns, f"Missing required column: {column}"
        
        # Check data types
        assert user_df["did"].dtype == "object"
        assert user_df["handle"].dtype == "object"
        assert user_df["study_group"].dtype == "object"
        assert user_df["enrollment_date"].dtype == "object"

    def test_engagement_data_structure(self):
        """Test that engagement data has valid structure.

        This test verifies that:
        1. Engagement data has expected structure
        2. Record types are valid
        3. Data relationships are correct
        """
        # Arrange
        valid_record_types = ["like", "repost", "reply", "post"]
        
        # Act
        engagement_data = mock_engaged_content
        
        # Assert
        assert isinstance(engagement_data, dict)
        for post_uri, engagements in engagement_data.items():
            assert isinstance(engagements, list)
            for engagement in engagements:
                assert "uri" in engagement
                assert "did" in engagement
                assert "date" in engagement
                assert "record_type" in engagement
                assert engagement["record_type"] in valid_record_types

    def test_label_data_structure(self):
        """Test that label data has valid structure.

        This test verifies that:
        1. Label data has expected structure
        2. Probability values are in valid range
        3. Required label fields are present
        """
        # Arrange
        expected_label_types = ["prob_toxic", "prob_constructive", "is_sociopolitical", "valence_clf_score"]
        
        # Act
        label_data = mock_labels_for_engaged_content
        
        # Assert
        assert isinstance(label_data, dict)
        for post_uri, labels in label_data.items():
            assert isinstance(labels, dict)
            # Check that at least some expected labels are present
            found_labels = [label for label in expected_label_types if label in labels]
            assert len(found_labels) > 0, f"No expected labels found for {post_uri}"
            
            # Check probability values are in valid range [0, 1]
            for label_name, value in labels.items():
                if label_name.startswith("prob_") and isinstance(value, (int, float)):
                    assert 0 <= value <= 1, f"Invalid probability value: {value} for {label_name}"


class TestDoAggregationsAndExportResults:
    """Tests for do_aggregations_and_export_results function."""

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_daily_engaged_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_weekly_content_per_user_metrics")
    @patch("os.makedirs")
    def test_do_aggregations_and_export_results_completes_successfully(
        self,
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
        3. Files are created and written to the expected locations
        4. All functions are called with correct parameters
        5. The export process completes without errors
        """
        # Arrange
        mock_get_daily.return_value = mock_user_per_day_content_label_metrics
        
        # Create mock DataFrames with to_csv method
        mock_daily_df = MagicMock()
        mock_daily_df.to_csv = MagicMock()
        mock_transform_daily.return_value = mock_daily_df
        
        mock_get_weekly.return_value = mock_user_per_week_content_label_metrics
        
        # Create mock DataFrames with to_csv method
        mock_weekly_df = MagicMock()
        mock_weekly_df.to_csv = MagicMock()
        mock_transform_weekly.return_value = mock_weekly_df

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_aggregations_and_export_results
        do_aggregations_and_export_results(
            user_df=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
            user_to_content_engaged_with=mock_user_to_content_engaged_with,
            labels_for_engaged_content=mock_labels_for_engaged_content,
            partition_dates=mock_partition_dates,
        )

        # Assert
        # Verify daily metrics processing
        mock_get_daily.assert_called_once_with(
            user_to_content_engaged_with=mock_user_to_content_engaged_with,
            labels_for_engaged_content=mock_labels_for_engaged_content,
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
        assert mock_makedirs.call_count == 2  # Called twice for daily and weekly results directories
        assert mock_daily_df.to_csv.call_count == 1
        assert mock_weekly_df.to_csv.call_count == 1

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_daily_engaged_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_weekly_content_per_user_metrics")
    @patch("os.makedirs")
    def test_do_aggregations_and_export_results_handles_empty_data(
        self,
        mock_makedirs,
        mock_transform_weekly,
        mock_get_weekly,
        mock_transform_daily,
        mock_get_daily,
    ):
        """Test that aggregations and export handles empty data gracefully.

        This test verifies that:
        1. Empty data is handled without errors
        2. All functions are still called with empty data
        3. File operations still occur
        4. The process completes successfully with empty results
        """
        # Arrange
        empty_daily_metrics = {}
        empty_weekly_metrics = {}
        
        # Create mock DataFrames with to_csv method
        empty_daily_df = MagicMock()
        empty_daily_df.to_csv = MagicMock()
        empty_weekly_df = MagicMock()
        empty_weekly_df.to_csv = MagicMock()

        mock_get_daily.return_value = empty_daily_metrics
        mock_transform_daily.return_value = empty_daily_df
        mock_get_weekly.return_value = empty_weekly_metrics
        mock_transform_weekly.return_value = empty_weekly_df

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import do_aggregations_and_export_results
        do_aggregations_and_export_results(
            user_df=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
            user_to_content_engaged_with={},
            labels_for_engaged_content={},
            partition_dates=mock_partition_dates,
        )

        # Assert
        # Verify functions are called with empty data
        mock_get_daily.assert_called_once_with(
            user_to_content_engaged_with={},
            labels_for_engaged_content={},
        )
        mock_get_weekly.assert_called_once_with(
            user_per_day_content_label_metrics=empty_daily_metrics,
            user_date_to_week_df=mock_user_date_to_week_df,
        )

        # Verify file operations still occur
        assert mock_makedirs.call_count == 2
        assert empty_daily_df.to_csv.call_count == 1
        assert empty_weekly_df.to_csv.call_count == 1


class TestMain:
    """Tests for main function."""

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.do_setup")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.do_aggregations_and_export_results")
    def test_main_executes_successfully(
        self,
        mock_do_aggregations_and_export_results,
        mock_do_setup,
    ):
        """Test that main function executes successfully.

        This test verifies that:
        1. do_setup is called first
        2. do_aggregations_and_export_results is called with setup results
        3. The main function completes without errors
        4. All required parameters are passed correctly
        """
        # Arrange
        mock_do_setup.return_value = mock_setup_objs

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import main
        main()

        # Assert
        mock_do_setup.assert_called_once()
        mock_do_aggregations_and_export_results.assert_called_once_with(
            user_df=mock_user_df,
            user_date_to_week_df=mock_user_date_to_week_df,
            user_to_content_engaged_with=mock_user_to_content_engaged_with,
            labels_for_engaged_content=mock_labels_for_engaged_content,
            partition_dates=mock_partition_dates,
        )

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.do_setup")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.do_aggregations_and_export_results")
    def test_main_handles_setup_failure(
        self,
        mock_do_aggregations_and_export_results,
        mock_do_setup,
    ):
        """Test that main function handles setup failure gracefully.

        This test verifies that:
        1. Setup failures are properly propagated
        2. The main function doesn't continue with failed setup
        3. Error handling works as expected
        """
        # Arrange
        mock_do_setup.side_effect = Exception("Setup failed")

        # Act & Assert
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import main
        with pytest.raises(Exception, match="Setup failed"):
            main()

        # Verify that aggregations are not called when setup fails
        mock_do_aggregations_and_export_results.assert_not_called()

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.do_setup")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.do_aggregations_and_export_results")
    def test_main_handles_export_failure(
        self,
        mock_do_aggregations_and_export_results,
        mock_do_setup,
    ):
        """Test that main function handles export failure gracefully.

        This test verifies that:
        1. Export failures are properly propagated
        2. Setup completes successfully before export failure
        3. Error handling works as expected
        """
        # Arrange
        mock_do_setup.return_value = mock_setup_objs
        mock_do_aggregations_and_export_results.side_effect = Exception("Export failed")

        # Act & Assert
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import main
        with pytest.raises(Exception, match="Export failed"):
            main()

        # Verify that setup was called but export failed
        mock_do_setup.assert_called_once()
        mock_do_aggregations_and_export_results.assert_called_once()


class TestFileExportPaths:
    """Tests for file export path generation and handling."""

    def test_export_file_paths_are_generated_correctly(self):
        """Test that export file paths are generated correctly.

        This test verifies that:
        1. File paths include the correct directory structure
        2. File names include a current datetime string
        3. File paths are properly formatted
        4. The datetime generation works correctly
        """
        # Act - Import actual production file paths
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import (
            engaged_content_daily_aggregated_results_export_fp,
            engaged_content_weekly_aggregated_results_export_fp,
        )

        # Assert
        # Check that paths contain the expected structure
        assert "daily_content_label_proportions_per_user_" in engaged_content_daily_aggregated_results_export_fp
        assert "weekly_content_label_proportions_per_user_" in engaged_content_weekly_aggregated_results_export_fp
        assert ".csv" in engaged_content_daily_aggregated_results_export_fp
        assert ".csv" in engaged_content_weekly_aggregated_results_export_fp
        assert "results" in engaged_content_daily_aggregated_results_export_fp
        assert "results" in engaged_content_weekly_aggregated_results_export_fp

        # Check that datetime string is present and properly formatted
        # Extract the datetime part from the filename
        daily_filename = os.path.basename(engaged_content_daily_aggregated_results_export_fp)
        weekly_filename = os.path.basename(engaged_content_weekly_aggregated_results_export_fp)
        
        # Verify datetime format (YYYY-MM-DD-HH:MM:SS)
        daily_datetime_part = daily_filename.replace("daily_content_label_proportions_per_user_", "").replace(".csv", "")
        weekly_datetime_part = weekly_filename.replace("weekly_content_label_proportions_per_user_", "").replace(".csv", "")
        
        # Check that datetime parts are the same (generated at same time)
        assert daily_datetime_part == weekly_datetime_part
        
        # Check datetime format: should be YYYY-MM-DD-HH:MM:SS
        import re
        datetime_pattern = r'\d{4}-\d{2}-\d{2}-\d{2}:\d{2}:\d{2}'
        assert re.match(datetime_pattern, daily_datetime_part), f"Invalid datetime format: {daily_datetime_part}"

    def test_export_file_paths_are_absolute_paths(self):
        """Test that export file paths are absolute paths.

        This test verifies that:
        1. File paths are absolute paths, not relative
        2. The paths are properly constructed
        3. The directory structure is correct
        """
        # Act - Import actual production file paths
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import (
            engaged_content_daily_aggregated_results_export_fp,
            engaged_content_weekly_aggregated_results_export_fp,
        )

        # Assert
        assert os.path.isabs(engaged_content_daily_aggregated_results_export_fp)
        assert os.path.isabs(engaged_content_weekly_aggregated_results_export_fp)




class TestIntegrationFlow:
    """Integration tests for the complete analysis flow."""

    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.load_user_data")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_partition_dates")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_engaged_content")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_content_engaged_with_per_user")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_all_labels_for_posts")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_daily_engaged_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_daily_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.get_weekly_content_per_user_metrics")
    @patch("services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main.transform_weekly_content_per_user_metrics")
    @patch("os.makedirs")
    def test_complete_analysis_flow(
        self,
        mock_makedirs,
        mock_transform_weekly,
        mock_get_weekly,
        mock_transform_daily,
        mock_get_daily,
        mock_get_all_labels,
        mock_get_content_engaged,
        mock_get_engaged,
        mock_get_partition_dates,
        mock_load_user_data,
    ):
        """Test the complete analysis flow from start to finish.

        This test verifies that:
        1. All functions are called in the correct order
        2. Data flows correctly between functions
        3. The complete pipeline executes successfully
        4. All expected outputs are generated
        """
        # Arrange
        mock_load_user_data.return_value = (
            mock_user_df,
            mock_user_date_to_week_df,
            mock_valid_study_users_dids,
        )
        mock_get_partition_dates.return_value = mock_partition_dates
        mock_get_engaged.return_value = mock_engaged_content
        mock_get_content_engaged.return_value = mock_user_to_content_engaged_with
        mock_get_all_labels.return_value = mock_labels_for_engaged_content
        mock_get_daily.return_value = mock_user_per_day_content_label_metrics
        
        # Create mock DataFrames with to_csv method
        mock_daily_df = MagicMock()
        mock_daily_df.to_csv = MagicMock()
        mock_transform_daily.return_value = mock_daily_df
        
        mock_get_weekly.return_value = mock_user_per_week_content_label_metrics
        
        # Create mock DataFrames with to_csv method
        mock_weekly_df = MagicMock()
        mock_weekly_df.to_csv = MagicMock()
        mock_transform_weekly.return_value = mock_weekly_df

        # Act
        from services.calculate_analytics.analyses.user_engagement_analysis_2025_06_16.main import main
        main()

        # Assert
        # Verify the complete call sequence
        mock_load_user_data.assert_called_once()
        mock_get_partition_dates.assert_called_once()
        mock_get_engaged.assert_called_once()
        mock_get_content_engaged.assert_called_once()
        mock_get_all_labels.assert_called_once()
        mock_get_daily.assert_called_once()
        mock_transform_daily.assert_called_once()
        mock_get_weekly.assert_called_once()
        mock_transform_weekly.assert_called_once()
        assert mock_makedirs.call_count == 2
        assert mock_daily_df.to_csv.call_count == 1
        assert mock_weekly_df.to_csv.call_count == 1

        # Verify data flow between functions
        mock_get_content_engaged.assert_called_once_with(engaged_content=mock_engaged_content)
        mock_get_all_labels.assert_called_once_with(
            post_uris=list(mock_engaged_content.keys()),
            partition_dates=mock_partition_dates,
        )
        mock_get_daily.assert_called_once_with(
            user_to_content_engaged_with=mock_user_to_content_engaged_with,
            labels_for_engaged_content=mock_labels_for_engaged_content,
        )
        mock_get_weekly.assert_called_once_with(
            user_per_day_content_label_metrics=mock_user_per_day_content_label_metrics,
            user_date_to_week_df=mock_user_date_to_week_df,
        )
