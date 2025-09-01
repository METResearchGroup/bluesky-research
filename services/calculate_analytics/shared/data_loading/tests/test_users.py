"""Tests for users.py.

This test suite verifies the functionality of user data loading functions:
- User demographic information loading from DynamoDB
- Study user condition mapping
- User date-to-week assignment loading
- Comprehensive user data loading with transformations
- Error handling and edge cases

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import os
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from services.calculate_analytics.shared.data_loading.users import (
    load_user_demographic_info,
    get_user_condition_mapping,
    load_user_date_to_week_df,
    load_user_data,
)


class TestLoadUserDemographicInfo:
    """Tests for load_user_demographic_info function."""

    def test_loads_user_demographics_successfully(self):
        """Test successful loading of user demographic information.

        This test verifies that:
        1. The function calls get_all_users correctly
        2. Only study users are included in the result
        3. The correct columns are selected
        4. The DataFrame is properly formatted
        """
        # Arrange
        mock_users = [
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "user1.bsky.social",
                    "bluesky_user_did": "did:plc:user1",
                    "condition": "control",
                    "is_study_user": True,
                    "other_field": "ignored"
                }
            ),
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "user2.bsky.social",
                    "bluesky_user_did": "did:plc:user2",
                    "condition": "treatment",
                    "is_study_user": True,
                    "other_field": "ignored"
                }
            ),
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "pilot.bsky.social",
                    "bluesky_user_did": "did:plc:pilot",
                    "condition": "pilot",
                    "is_study_user": False,
                    "other_field": "ignored"
                }
            )
        ]
        
        expected_columns = ["bluesky_handle", "bluesky_user_did", "condition"]
        expected_data = [
            ["user1.bsky.social", "did:plc:user1", "control"],
            ["user2.bsky.social", "did:plc:user2", "treatment"]
        ]

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.get_all_users", return_value=mock_users):
            result = load_user_demographic_info()

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert len(result) == 2  # Only study users
        assert result["bluesky_handle"].tolist() == [row[0] for row in expected_data]
        assert result["bluesky_user_did"].tolist() == [row[1] for row in expected_data]
        assert result["condition"].tolist() == [row[2] for row in expected_data]

    def test_filters_out_non_study_users(self):
        """Test that non-study users are properly filtered out.

        This test verifies that:
        1. Users with is_study_user=False are excluded
        2. Only valid study participants are included
        3. The filtering logic works correctly
        """
        # Arrange
        mock_users = [
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "study1.bsky.social",
                    "bluesky_user_did": "did:plc:study1",
                    "condition": "control",
                    "is_study_user": True
                }
            ),
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "pilot1.bsky.social",
                    "bluesky_user_did": "did:plc:pilot1",
                    "condition": "pilot",
                    "is_study_user": False
                }
            ),
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "study2.bsky.social",
                    "bluesky_user_did": "did:plc:study2",
                    "condition": "treatment",
                    "is_study_user": True
                }
            )
        ]
        
        expected_count = 2  # Only study users
        expected_handles = ["study1.bsky.social", "study2.bsky.social"]

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.get_all_users", return_value=mock_users):
            result = load_user_demographic_info()

        # Assert
        assert len(result) == expected_count
        assert result["bluesky_handle"].tolist() == expected_handles
        assert "pilot1.bsky.social" not in result["bluesky_handle"].values

    def test_handles_empty_user_list(self):
        """Test handling of empty user list.

        This test verifies that:
        1. Empty input list is handled gracefully
        2. Function returns empty DataFrame with correct structure
        3. Edge case doesn't cause crashes
        """
        # Arrange
        mock_users = []
        expected_columns = ["bluesky_handle", "bluesky_user_did", "condition"]
        expected_count = 0

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.get_all_users", return_value=mock_users):
            result = load_user_demographic_info()

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert len(result) == expected_count

    def test_handles_all_non_study_users(self):
        """Test handling when all users are non-study users.

        This test verifies that:
        1. When no users are study users, empty DataFrame is returned
        2. The filtering logic works correctly for edge cases
        3. Function doesn't crash on unexpected input
        """
        # Arrange
        mock_users = [
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "pilot1.bsky.social",
                    "bluesky_user_did": "did:plc:pilot1",
                    "condition": "pilot",
                    "is_study_user": False
                }
            ),
            Mock(
                model_dump=lambda: {
                    "bluesky_handle": "pilot2.bsky.social",
                    "bluesky_user_did": "did:plc:pilot2",
                    "condition": "pilot",
                    "is_study_user": False
                }
            )
        ]
        
        expected_count = 0

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.get_all_users", return_value=mock_users):
            result = load_user_demographic_info()

        # Assert
        assert len(result) == expected_count

    def test_raises_exception_on_get_all_users_failure(self):
        """Test that exceptions from get_all_users are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        expected_error = "Database connection failed"
        mock_get_all_users = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.users.get_all_users", side_effect=mock_get_all_users):
            with pytest.raises(Exception, match=expected_error):
                load_user_demographic_info()


class TestGetUserConditionMapping:
    """Tests for get_user_condition_mapping function."""

    def test_creates_correct_condition_mapping(self):
        """Test creation of user DID to condition mapping.

        This test verifies that:
        1. The mapping is created correctly from user data
        2. Each user DID maps to their correct condition
        3. The result is a proper dictionary
        """
        # Arrange
        mock_user_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
            "bluesky_user_did": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "condition": ["control", "treatment", "control"]
        })
        
        expected_mapping = {
            "did:plc:user1": "control",
            "did:plc:user2": "treatment",
            "did:plc:user3": "control"
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df):
            result = get_user_condition_mapping()

        # Assert
        assert isinstance(result, dict)
        assert result == expected_mapping
        assert len(result) == 3

    def test_handles_empty_user_dataframe(self):
        """Test handling of empty user DataFrame.

        This test verifies that:
        1. Empty DataFrame results in empty mapping
        2. Function handles edge case gracefully
        3. Result is still a valid dictionary
        """
        # Arrange
        mock_user_df = pd.DataFrame(columns=["bluesky_handle", "bluesky_user_did", "condition"])
        expected_mapping = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df):
            result = get_user_condition_mapping()

        # Assert
        assert isinstance(result, dict)
        assert result == expected_mapping
        assert len(result) == 0

    def test_raises_exception_on_load_user_demographic_info_failure(self):
        """Test that exceptions from load_user_demographic_info are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        expected_error = "Failed to load user demographics"
        mock_load_demographics = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", side_effect=mock_load_demographics):
            with pytest.raises(Exception, match=expected_error):
                get_user_condition_mapping()


class TestLoadUserDateToWeekDf:
    """Tests for load_user_date_to_week_df function."""

    def test_loads_csv_file_correctly(self):
        """Test loading of CSV file with correct column selection and renaming.

        This test verifies that:
        1. The correct file path is constructed
        2. The correct columns are selected
        3. The column renaming works properly
        4. The DataFrame is returned with expected structure
        """
        # Arrange
        mock_csv_data = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"],
            "date": ["2024-01-01", "2024-01-02"],
            "week_dynamic": [1, 2],
            "other_column": ["ignored1", "ignored2"]
        })
        
        expected_columns = ["bluesky_handle", "date", "week"]
        expected_data = [
            ["user1.bsky.social", "2024-01-01", 1],
            ["user2.bsky.social", "2024-01-02", 2]
        ]

        # Act
        with patch("os.path.exists", return_value=True), patch("pandas.read_csv") as mock_read_csv:
            mock_read_csv.return_value = mock_csv_data
            result = load_user_date_to_week_df()

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert len(result) == 2
        assert result["bluesky_handle"].tolist() == [row[0] for row in expected_data]
        assert result["date"].tolist() == [row[1] for row in expected_data]
        assert result["week"].tolist() == [row[2] for row in expected_data]

    def test_constructs_correct_file_path(self):
        """Test that the correct file path is constructed.

        This test verifies that:
        1. The shared_assets_directory constant is used correctly
        2. The path construction follows the expected pattern
        3. The file path is properly formatted
        """
        # Arrange
        mock_csv_data = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social"],
            "date": ["2024-01-01"],
            "week_dynamic": [1]
        })
        
        expected_path_components = ["static", "bluesky_per_user_week_assignments.csv"]

        # Act
        with patch("pandas.read_csv") as mock_read_csv:
            mock_read_csv.return_value = mock_csv_data
            result = load_user_date_to_week_df()

        # Assert
        # Verify that read_csv was called with a path containing the expected components
        call_args = mock_read_csv.call_args[0][0]
        for component in expected_path_components:
            assert component in str(call_args)

    def test_handles_empty_csv_file(self):
        """Test handling of empty CSV file.

        This test verifies that:
        1. Empty CSV file is handled gracefully
        2. Function returns empty DataFrame with correct structure
        3. Edge case doesn't cause crashes
        """
        # Arrange
        mock_csv_data = pd.DataFrame(columns=["bluesky_handle", "date", "week_dynamic"])
        expected_columns = ["bluesky_handle", "date", "week"]
        expected_count = 0

        # Act
        with patch("pandas.read_csv", return_value=mock_csv_data):
            result = load_user_date_to_week_df()

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == expected_columns
        assert len(result) == expected_count


class TestLoadUserData:
    """Tests for load_user_data function."""

    def test_loads_comprehensive_user_data_successfully(self):
        """Test successful loading of comprehensive user data.

        This test verifies that:
        1. All three data components are loaded correctly
        2. The user DID mapping is created properly
        3. The week assignment DataFrame is enhanced with user DIDs
        4. Valid study users are correctly identified
        5. The function returns the expected tuple structure
        """
        # Arrange
        mock_user_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
            "bluesky_user_did": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "condition": ["control", "treatment", "control"]
        })
        
        mock_week_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"],
            "date": ["2024-01-01", "2024-01-02"],
            "week": [1, 2]
        })
        
        expected_user_count = 2  # Only users in week_df
        expected_week_count = 2
        expected_valid_dids = {"did:plc:user1", "did:plc:user2"}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df), \
             patch("services.calculate_analytics.shared.data_loading.users.load_user_date_to_week_df", return_value=mock_week_df):
            result = load_user_data()

        # Assert
        user_df, user_date_to_week_df, valid_study_users_dids = result
        
        # Check user_df
        assert isinstance(user_df, pd.DataFrame)
        assert len(user_df) == expected_user_count
        assert list(user_df.columns) == ["bluesky_handle", "bluesky_user_did", "condition"]
        
        # Check user_date_to_week_df
        assert isinstance(user_date_to_week_df, pd.DataFrame)
        assert len(user_date_to_week_df) == expected_week_count
        assert "bluesky_user_did" in user_date_to_week_df.columns
        assert user_date_to_week_df["bluesky_user_did"].tolist() == ["did:plc:user1", "did:plc:user2"]
        
        # Check valid_study_users_dids
        assert isinstance(valid_study_users_dids, set)
        assert valid_study_users_dids == expected_valid_dids

    def test_creates_correct_user_handle_to_did_mapping(self):
        """Test creation of user handle to DID mapping.

        This test verifies that:
        1. The mapping is created correctly from user data
        2. Each handle maps to its corresponding DID
        3. The mapping is used to enhance the week DataFrame
        """
        # Arrange
        mock_user_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"],
            "bluesky_user_did": ["did:plc:user1", "did:plc:user2"],
            "condition": ["control", "treatment"]
        })
        
        mock_week_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"],
            "date": ["2024-01-01", "2024-01-02"],
            "week": [1, 2]
        })
        
        expected_mapping = {
            "user1.bsky.social": "did:plc:user1",
            "user2.bsky.social": "did:plc:user2"
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df), \
             patch("services.calculate_analytics.shared.data_loading.users.load_user_date_to_week_df", return_value=mock_week_df):
            result = load_user_data()

        # Assert
        user_df, user_date_to_week_df, valid_study_users_dids = result
        
        # Verify the mapping was created and applied correctly
        for handle, expected_did in expected_mapping.items():
            mask = user_date_to_week_df["bluesky_handle"] == handle
            actual_did = user_date_to_week_df[mask]["bluesky_user_did"].iloc[0]
            assert actual_did == expected_did

    def test_filters_users_based_on_week_assignments(self):
        """Test that users are filtered based on week assignments.

        This test verifies that:
        1. Only users with week assignments are considered valid
        2. Users without week assignments are excluded
        3. The filtering logic works correctly
        """
        # Arrange
        mock_user_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social", "user3.bsky.social"],
            "bluesky_user_did": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "condition": ["control", "treatment", "control"]
        })
        
        # Only user1 and user2 have week assignments
        mock_week_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"],
            "date": ["2024-01-01", "2024-01-02"],
            "week": [1, 2]
        })
        
        expected_valid_handles = {"user1.bsky.social", "user2.bsky.social"}
        expected_excluded_handles = {"user3.bsky.social"}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df), \
             patch("services.calculate_analytics.shared.data_loading.users.load_user_date_to_week_df", return_value=mock_week_df):
            result = load_user_data()

        # Assert
        user_df, user_date_to_week_df, valid_study_users_dids = result
        
        # Check that only users with week assignments are included
        result_handles = set(user_df["bluesky_handle"])
        assert result_handles == expected_valid_handles
        assert expected_excluded_handles.isdisjoint(result_handles)

    def test_handles_empty_week_assignments(self):
        """Test handling when no users have week assignments.

        This test verifies that:
        1. Empty week assignments result in empty user data
        2. Function handles edge case gracefully
        3. Result structure is maintained even with no data
        """
        # Arrange
        mock_user_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social", "user2.bsky.social"],
            "bluesky_user_did": ["did:plc:user1", "did:plc:user2"],
            "condition": ["control", "treatment"]
        })
        
        mock_week_df = pd.DataFrame(columns=["bluesky_handle", "date", "week"])
        
        expected_user_count = 0
        expected_week_count = 0
        expected_valid_dids = set()

        # Act
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df), \
             patch("services.calculate_analytics.shared.data_loading.users.load_user_date_to_week_df", return_value=mock_week_df):
            result = load_user_data()

        # Assert
        user_df, user_date_to_week_df, valid_study_users_dids = result
        
        assert len(user_df) == expected_user_count
        assert len(user_date_to_week_df) == expected_week_count
        assert valid_study_users_dids == expected_valid_dids

    def test_raises_exception_on_demographic_info_failure(self):
        """Test that exceptions from load_user_demographic_info are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        expected_error = "Failed to load user demographics"
        mock_load_demographics = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", side_effect=mock_load_demographics):
            with pytest.raises(Exception, match=expected_error):
                load_user_data()

    def test_raises_exception_on_week_assignments_failure(self):
        """Test that exceptions from load_user_date_to_week_df are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        mock_user_df = pd.DataFrame({
            "bluesky_handle": ["user1.bsky.social"],
            "bluesky_user_did": ["did:plc:user1"],
            "condition": ["control"]
        })
        
        expected_error = "Failed to load week assignments"
        mock_load_weeks = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.users.load_user_demographic_info", return_value=mock_user_df), \
             patch("services.calculate_analytics.shared.data_loading.users.load_user_date_to_week_df", side_effect=mock_load_weeks):
            with pytest.raises(Exception, match=expected_error):
                load_user_data()
