"""Tests for feeds.py.

This test suite verifies the functionality of feed data loading functions:
- Loading feeds for specific partition dates
- Mapping users to posts used in their feeds
- Getting feeds per user with date-based organization
- Extracting all post URIs used in feeds across users

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import json
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from services.calculate_analytics.shared.data_loading.feeds import (
    get_feeds_for_partition_date,
    map_users_to_posts_used_in_feeds,
    get_post_uris_used_in_feeds_per_user_per_day,
    get_all_post_uris_used_in_feeds,
)


class TestGetFeedsForPartitionDate:
    """Tests for get_feeds_for_partition_date function."""

    def test_loads_feeds_successfully(self):
        """Test successful loading of feeds for a partition date.

        This test verifies that:
        1. The function calls load_data_from_local_storage correctly
        2. The correct service and directory parameters are used
        3. The partition date is passed correctly
        4. The function returns the expected DataFrame
        5. Logging occurs with the correct information
        """
        # Arrange
        partition_date = "2024-01-01"
        mock_feeds_df = pd.DataFrame({
            "user": ["user1", "user2"],
            "feed": ['[{"item": "post1"}]', '[{"item": "post2"}]'],
            "feed_generation_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00"]
        })
        expected_count = 2

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", return_value=mock_feeds_df):
            result = get_feeds_for_partition_date(partition_date)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == expected_count
        assert list(result.columns) == ["user", "feed", "feed_generation_timestamp"]

    def test_calls_load_data_with_correct_parameters(self):
        """Test that load_data_from_local_storage is called with correct parameters.

        This test verifies that:
        1. The correct service name is used
        2. The correct directory is specified
        3. The partition date is passed correctly
        4. The function parameters match expected values
        """
        # Arrange
        partition_date = "2024-01-15"
        mock_feeds_df = pd.DataFrame({
            "user": ["user1"],
            "feed": ['[{"item": "post1"}]'],
            "feed_generation_timestamp": ["2024-01-15-00:00:00"]
        })

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage") as mock_load_data:
            mock_load_data.return_value = mock_feeds_df
            result = get_feeds_for_partition_date(partition_date)

        # Assert
        mock_load_data.assert_called_once_with(
            service="generated_feeds",
            storage_tiers=["cache"],
            partition_date=partition_date
        )

    def test_handles_empty_feeds_dataframe(self):
        """Test handling of empty feeds DataFrame.

        This test verifies that:
        1. Empty DataFrame is handled gracefully
        2. Function returns empty DataFrame with correct structure
        3. Edge case doesn't cause crashes
        4. Logging occurs correctly even with no data
        """
        # Arrange
        partition_date = "2024-01-01"
        mock_feeds_df = pd.DataFrame(columns=["user", "feed", "feed_generation_timestamp"])
        expected_count = 0

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", return_value=mock_feeds_df):
            result = get_feeds_for_partition_date(partition_date)

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == expected_count
        assert list(result.columns) == ["user", "feed", "feed_generation_timestamp"]

    def test_raises_exception_on_load_data_failure(self):
        """Test that exceptions from load_data_from_local_storage are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        partition_date = "2024-01-01"
        expected_error = "Database connection failed"
        mock_load_data = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", side_effect=mock_load_data):
            with pytest.raises(Exception, match=expected_error):
                get_feeds_for_partition_date(partition_date)


class TestMapUsersToPostsUsedInFeeds:
    """Tests for map_users_to_posts_used_in_feeds function."""

    def test_maps_users_to_posts_correctly(self):
        """Test correct mapping of users to posts used in feeds.

        This test verifies that:
        1. The function loads feeds for the partition date
        2. Each user is mapped to their corresponding posts
        3. Post URIs are extracted correctly from feed JSON
        4. The mapping structure is correct
        5. Logging occurs with the correct information
        """
        # Arrange
        partition_date = "2024-01-01"
        mock_feeds_df = pd.DataFrame({
            "user": ["user1", "user2", "user1"],
            "feed": [
                '[{"item": "post1"}, {"item": "post2"}]',
                '[{"item": "post3"}]',
                '[{"item": "post4"}, {"item": "post1"}]'
            ],
            "feed_generation_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00", "2024-01-01-00:00:00"]
        })
        
        expected_mapping = {
            "user1": {"post1", "post2", "post4"},
            "user2": {"post3"}
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.get_feeds_for_partition_date", return_value=mock_feeds_df):
            result = map_users_to_posts_used_in_feeds(partition_date)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2
        assert result == expected_mapping

    def test_handles_duplicate_posts_for_same_user(self):
        """Test handling of duplicate posts for the same user.

        This test verifies that:
        1. Duplicate posts are properly deduplicated using sets
        2. The same user can have multiple feeds
        3. Post URIs are correctly aggregated across feeds
        4. The deduplication logic works correctly
        """
        # Arrange
        partition_date = "2024-01-01"
        mock_feeds_df = pd.DataFrame({
            "user": ["user1", "user1", "user1"],
            "feed": [
                '[{"item": "post1"}, {"item": "post2"}]',
                '[{"item": "post2"}, {"item": "post3"}]',
                '[{"item": "post1"}, {"item": "post4"}]'
            ],
            "feed_generation_timestamp": ["2024-01-01-00:00:00", "2024-01-01-00:00:00", "2024-01-01-00:00:00"]
        })
        
        expected_posts = {"post1", "post2", "post3", "post4"}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.get_feeds_for_partition_date", return_value=mock_feeds_df):
            result = map_users_to_posts_used_in_feeds(partition_date)

        # Assert
        assert "user1" in result
        assert result["user1"] == expected_posts
        assert len(result["user1"]) == 4  # All unique posts

    def test_handles_empty_feeds_dataframe(self):
        """Test handling of empty feeds DataFrame.

        This test verifies that:
        1. Empty DataFrame results in empty mapping
        2. Function handles edge case gracefully
        3. Result is still a valid dictionary
        4. Logging occurs correctly even with no data
        """
        # Arrange
        partition_date = "2024-01-01"
        mock_feeds_df = pd.DataFrame(columns=["user", "feed", "feed_generation_timestamp"])
        expected_mapping = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.get_feeds_for_partition_date", return_value=mock_feeds_df):
            result = map_users_to_posts_used_in_feeds(partition_date)

        # Assert
        assert isinstance(result, dict)
        assert result == expected_mapping
        assert len(result) == 0

    def test_handles_malformed_feed_json(self):
        """Test handling of malformed feed JSON.

        This test verifies that:
        1. Malformed JSON is handled gracefully
        2. The function doesn't crash on invalid data
        3. Error handling works correctly
        """
        # Arrange
        partition_date = "2024-01-01"
        mock_feeds_df = pd.DataFrame({
            "user": ["user1"],
            "feed": ['invalid_json_string'],
            "feed_generation_timestamp": ["2024-01-01-00:00:00"]
        })

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.feeds.get_feeds_for_partition_date", return_value=mock_feeds_df):
            with pytest.raises(json.JSONDecodeError):
                map_users_to_posts_used_in_feeds(partition_date)

    def test_raises_exception_on_get_feeds_failure(self):
        """Test that exceptions from get_feeds_for_partition_date are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        partition_date = "2024-01-01"
        expected_error = "Failed to load feeds"
        mock_get_feeds = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.feeds.get_feeds_for_partition_date", side_effect=mock_get_feeds):
            with pytest.raises(Exception, match=expected_error):
                map_users_to_posts_used_in_feeds(partition_date)


class TestGetFeedsPerUser:
    """Tests for get_post_uris_used_in_feeds_per_user_per_day function."""

    def test_gets_feeds_per_user_correctly(self):
        """Test correct retrieval of feeds per user with date organization.

        This test verifies that:
        1. The function loads data from local storage with correct parameters
        2. Only valid study users are included in the results
        3. Feeds are properly organized by user DID and date
        4. Post URIs are extracted and deduplicated correctly
        5. The nested structure is created properly
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        mock_feeds_df = pd.DataFrame({
            "bluesky_user_did": ["did:plc:user1", "did:plc:user1", "did:plc:user2"],
            "feed": [
                '[{"item": "post1"}, {"item": "post2"}]',
                '[{"item": "post3"}, {"item": "post1"}]',
                '[{"item": "post4"}]'
            ],
            "feed_generation_timestamp": [
                "2024-01-01-00:00:00",
                "2024-01-02-00:00:00",
                "2024-01-01-00:00:00"
            ]
        })
        
        expected_structure = {
            "did:plc:user1": {
                "2024-01-01": {"post1", "post2"},
                "2024-01-02": {"post1", "post3"}
            },
            "did:plc:user2": {
                "2024-01-01": {"post4"}
            }
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", return_value=mock_feeds_df):
            result = get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2
        assert result == expected_structure

    def test_filters_for_valid_study_users_only(self):
        """Test that only valid study users are included in results.

        This test verifies that:
        1. Users not in valid_study_users_dids are filtered out
        2. Only study participants are included
        3. The filtering logic works correctly
        4. Non-study users don't appear in the results
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        mock_feeds_df = pd.DataFrame({
            "bluesky_user_did": ["did:plc:user1", "did:plc:pilot", "did:plc:user2"],
            "feed": [
                '[{"item": "post1"}]',
                '[{"item": "post2"}]',
                '[{"item": "post3"}]'
            ],
            "feed_generation_timestamp": [
                "2024-01-01-00:00:00",
                "2024-01-01-00:00:00",
                "2024-01-01-00:00:00"
            ]
        })
        
        expected_users = {"did:plc:user1"}
        expected_excluded_users = {"did:plc:pilot", "did:plc:user2"}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", return_value=mock_feeds_df):
            result = get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)

        # Assert
        assert set(result.keys()) == expected_users
        assert expected_excluded_users.isdisjoint(set(result.keys()))

    def test_handles_empty_valid_study_users(self):
        """Test handling when no valid study users are provided.

        This test verifies that:
        1. Empty valid_study_users_dids results in empty results
        2. Function handles edge case gracefully
        3. Result structure is maintained even with no data
        """
        # Arrange
        valid_study_users_dids = set()
        mock_feeds_df = pd.DataFrame({
            "bluesky_user_did": ["did:plc:user1"],
            "feed": ['[{"item": "post1"}]'],
            "feed_generation_timestamp": ["2024-01-01-00:00:00"]
        })
        
        expected_result = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", return_value=mock_feeds_df):
            result = get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)

        # Assert
        assert result == expected_result
        assert len(result) == 0

    def test_handles_empty_feeds_dataframe(self):
        """Test handling of empty feeds DataFrame.

        This test verifies that:
        1. Empty DataFrame results in empty results
        2. Function handles edge case gracefully
        3. Result structure is maintained even with no data
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        mock_feeds_df = pd.DataFrame(columns=["bluesky_user_did", "feed", "feed_generation_timestamp"])
        
        expected_result = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", return_value=mock_feeds_df):
            result = get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)

        # Assert
        assert result == expected_result
        assert len(result) == 0

    def test_calls_load_data_with_correct_parameters(self):
        """Test that load_data_from_local_storage is called with correct parameters.

        This test verifies that:
        1. The correct service name is used
        2. The correct directory is specified
        3. The correct export format is used
        4. The correct query and metadata are provided
        5. The correct date range is used
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        mock_feeds_df = pd.DataFrame({
            "bluesky_user_did": ["did:plc:user1"],
            "feed": ['[{"item": "post1"}]'],
            "feed_generation_timestamp": ["2024-01-01-00:00:00"]
        })

        # Act
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage") as mock_load_data:
            mock_load_data.return_value = mock_feeds_df
            result = get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)

        # Assert
        mock_load_data.assert_called_once()
        call_args = mock_load_data.call_args[1]
        assert call_args["service"] == "generated_feeds"
        assert call_args["storage_tiers"] == ["cache"]
        assert "duckdb_query" in call_args
        assert "query_metadata" in call_args
        assert "start_partition_date" in call_args
        assert "end_partition_date" in call_args

    def test_raises_exception_on_load_data_failure(self):
        """Test that exceptions from load_data_from_local_storage are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        expected_error = "Failed to load feeds data"
        mock_load_data = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.feeds.load_data_from_local_storage", side_effect=mock_load_data):
            with pytest.raises(Exception, match=expected_error):
                get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)


class TestGetAllPostUrisUsedInFeeds:
    """Tests for get_all_post_uris_used_in_feeds function."""

    def test_gets_all_post_uris_correctly(self):
        """Test correct extraction of all post URIs used in feeds.

        This test verifies that:
        1. All post URIs from all users and dates are collected
        2. Duplicate URIs are properly deduplicated
        3. The result is a set of unique post URIs
        4. All nested levels are traversed correctly
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": {"post1", "post2"},
                "2024-01-02": {"post2", "post3"}
            },
            "user2": {
                "2024-01-01": {"post3", "post4"},
                "2024-01-02": {"post1", "post5"}
            }
        }
        
        expected_uris = {"post1", "post2", "post3", "post4", "post5"}

        # Act
        result = get_all_post_uris_used_in_feeds(user_to_content_in_feeds)

        # Assert
        assert isinstance(result, set)
        assert result == expected_uris
        assert len(result) == 5

    def test_handles_empty_user_to_content_mapping(self):
        """Test handling of empty user to content mapping.

        This test verifies that:
        1. Empty input results in empty set
        2. Function handles edge case gracefully
        3. Result is still a valid set
        4. Edge case doesn't cause crashes
        """
        # Arrange
        user_to_content_in_feeds = {}
        expected_uris = set()

        # Act
        result = get_all_post_uris_used_in_feeds(user_to_content_in_feeds)

        # Assert
        assert isinstance(result, set)
        assert result == expected_uris
        assert len(result) == 0

    def test_handles_users_with_no_content(self):
        """Test handling of users with no content in feeds.

        This test verifies that:
        1. Users with empty content dictionaries are handled correctly
        2. Empty sets don't cause issues
        3. The function processes all users regardless of content
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {},
            "user2": {"2024-01-01": {"post1"}},
            "user3": {}
        }
        
        expected_uris = {"post1"}

        # Act
        result = get_all_post_uris_used_in_feeds(user_to_content_in_feeds)

        # Assert
        assert isinstance(result, set)
        assert result == expected_uris
        assert len(result) == 1

    def test_handles_single_user_single_date(self):
        """Test handling of single user with single date content.

        This test verifies that:
        1. Single user scenarios are handled correctly
        2. Single date scenarios work properly
        3. The function processes minimal input correctly
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": {"post1", "post2"}
            }
        }
        
        expected_uris = {"post1", "post2"}

        # Act
        result = get_all_post_uris_used_in_feeds(user_to_content_in_feeds)

        # Assert
        assert isinstance(result, set)
        assert result == expected_uris
        assert len(result) == 2

    def test_handles_duplicate_posts_across_users_and_dates(self):
        """Test handling of duplicate posts across different users and dates.

        This test verifies that:
        1. Duplicate posts are properly deduplicated
        2. Posts appearing in multiple users/dates are counted only once
        3. The deduplication logic works correctly across all levels
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": {"post1", "post2"},
                "2024-01-02": {"post2", "post3"}
            },
            "user2": {
                "2024-01-01": {"post1", "post4"},
                "2024-01-02": {"post2", "post5"}
            }
        }
        
        expected_uris = {"post1", "post2", "post3", "post4", "post5"}

        # Act
        result = get_all_post_uris_used_in_feeds(user_to_content_in_feeds)

        # Assert
        assert isinstance(result, set)
        assert result == expected_uris
        assert len(result) == 5  # post2 appears 3 times but is counted only once

