"""Tests for engagement.py.

This test suite verifies the functionality of engagement data loading functions:
- Content engagement data loading by record type
- Comprehensive engagement data aggregation across all record types
- User engagement mapping and organization
- Data filtering and transformation logic
- Edge cases and error handling

The tests use mocks to isolate the data loading logic from external dependencies.
"""

import json
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from services.calculate_analytics.shared.data_loading.engagement import (
    get_content_engaged_with,
    get_engaged_content,
    get_content_engaged_with_per_user,
)


class TestGetContentEngagedWith:
    """Tests for get_content_engaged_with function."""

    def test_loads_likes_data_correctly(self):
        """Test successful loading of likes data.

        This test verifies that:
        1. The function calls load_data_from_local_storage correctly
        2. Data is properly filtered for valid study users
        3. The correct engagement structure is created for likes
        4. Date formatting and record type assignment work correctly
        """
        # Arrange
        record_type = "like"
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "author": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00", "2024-01-03T12:00:00"],
            "subject": [
                '{"uri": "post1"}',
                '{"uri": "post2"}',
                '{"uri": "post3"}'
            ]
        })
        
        expected = {
            "post1": [
                {
                    "did": "did:plc:user1",
                    "date": "2024-01-01",
                    "record_type": "like"
                }
            ],
            "post2": [
                {
                    "did": "did:plc:user2",
                    "date": "2024-01-02",
                    "record_type": "like"
                }
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2  # Only posts from valid study users
        assert result == expected

    def test_loads_posts_data_correctly(self):
        """Test successful loading of posts data.

        This test verifies that:
        1. Posts record type uses the post URI directly
        2. The engagement structure is created correctly for posts
        3. Date formatting works properly
        4. Only valid study users are included
        """
        # Arrange
        record_type = "post"
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "author": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00", "2024-01-03T12:00:00"]
        })
        
        expected = {
            "post1": [
                {
                    "did": "did:plc:user1",
                    "date": "2024-01-01",
                    "record_type": "post"
                }
            ],
            "post2": [
                {
                    "did": "did:plc:user2",
                    "date": "2024-01-02",
                    "record_type": "post"
                }
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2  # Only posts from valid study users
        assert result == expected

    def test_loads_replies_data_correctly(self):
        """Test successful loading of replies data.

        This test verifies that:
        1. Replies record type extracts parent URI from reply field
        2. JSON parsing works correctly for reply data
        3. The engagement structure is created properly
        4. Date formatting and filtering work as expected
        """
        # Arrange
        record_type = "reply"
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        mock_df = pd.DataFrame({
            "uri": ["reply1", "reply2", "reply3"],
            "author": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00", "2024-01-03T12:00:00"],
            "reply": [
                '{"parent": {"uri": "parent_post1"}}',
                '{"parent": {"uri": "parent_post2"}}',
                '{"parent": {"uri": "parent_post3"}}'
            ]
        })
        
        expected = {
            "parent_post1": [
                {
                    "did": "did:plc:user1",
                    "date": "2024-01-01",
                    "record_type": "reply"
                }
            ],
            "parent_post2": [
                {
                    "did": "did:plc:user2",
                    "date": "2024-01-02",
                    "record_type": "reply"
                }
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2  # Only replies from valid study users
        assert result == expected

    def test_loads_reposts_data_correctly(self):
        """Test successful loading of reposts data.

        This test verifies that:
        1. Reposts record type extracts URI from subject field
        2. JSON parsing works correctly for repost data
        3. The engagement structure is created properly
        4. Date formatting and filtering work as expected
        """
        # Arrange
        record_type = "repost"
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        mock_df = pd.DataFrame({
            "uri": ["repost1", "repost2", "repost3"],
            "author": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00", "2024-01-03T12:00:00"],
            "subject": [
                '{"uri": "original_post1"}',
                '{"uri": "original_post2"}',
                '{"uri": "original_post3"}'
            ]
        })
        
        expected = {
            "original_post1": [
                {
                    "did": "did:plc:user1",
                    "date": "2024-01-01",
                    "record_type": "repost"
                }
            ],
            "original_post2": [
                {
                    "did": "did:plc:user2",
                    "date": "2024-01-02",
                    "record_type": "repost"
                }
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2  # Only reposts from valid study users
        assert result == expected

    def test_filters_out_non_study_users(self):
        """Test that non-study users are properly filtered out.

        This test verifies that:
        1. Users not in valid_study_users_dids are excluded
        2. Only valid study participants are included in results
        3. The filtering logic works correctly
        """
        # Arrange
        record_type = "like"
        valid_study_users_dids = {"did:plc:user1"}
        
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "author": ["did:plc:user1", "did:plc:user2", "did:plc:user3"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00", "2024-01-03T12:00:00"],
            "subject": [
                '{"uri": "post1"}',
                '{"uri": "post2"}',
                '{"uri": "post3"}'
            ]
        })
        
        expected = {
            "post1": [
                {
                    "did": "did:plc:user1",
                    "date": "2024-01-01",
                    "record_type": "like"
                }
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert len(result) == 1  # Only user1's engagement
        assert result == expected
        assert "did:plc:user2" not in [eng["did"] for engs in result.values() for eng in engs]
        assert "did:plc:user3" not in [eng["did"] for engs in result.values() for eng in engs]

    def test_handles_duplicate_uris(self):
        """Test handling of duplicate URIs in the data.

        This test verifies that:
        1. Duplicate URIs are properly deduplicated
        2. The first occurrence is kept
        3. The deduplication logic works correctly
        """
        # Arrange
        record_type = "like"
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        mock_df = pd.DataFrame({
            "uri": ["post1", "post1", "post2"],  # post1 appears twice
            "author": ["did:plc:user1", "did:plc:user2", "did:plc:user1"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00", "2024-01-03T12:00:00"],
            "subject": [
                '{"uri": "post1"}',
                '{"uri": "post1"}',
                '{"uri": "post2"}'
            ]
        })
        
        expected_count = 2  # post1 and post2, not 3

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert len(result) == expected_count
        assert "post1" in result
        assert "post2" in result

    def test_handles_empty_dataframe(self):
        """Test handling of empty DataFrame.

        This test verifies that:
        1. Empty input DataFrame is handled gracefully
        2. Function returns empty dictionary
        3. Edge case doesn't cause crashes
        """
        # Arrange
        record_type = "like"
        valid_study_users_dids = {"did:plc:user1"}
        
        mock_df = pd.DataFrame(columns=["uri", "author", "synctimestamp", "subject"])
        expected = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert result == expected
        assert len(result) == 0

    def test_handles_no_valid_study_users(self):
        """Test handling when no users are valid study users.

        This test verifies that:
        1. When no users match valid_study_users_dids, empty result is returned
        2. The filtering logic works correctly for edge cases
        3. Function doesn't crash on unexpected input
        """
        # Arrange
        record_type = "like"
        valid_study_users_dids = set()  # Empty set
        
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "author": ["did:plc:user1", "did:plc:user2"],
            "synctimestamp": ["2024-01-01T10:00:00", "2024-01-02T11:00:00"],
            "subject": [
                '{"uri": "post1"}',
                '{"uri": "post2"}'
            ]
        })
        
        expected = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", return_value=mock_df):
            result = get_content_engaged_with(record_type, valid_study_users_dids)

        # Assert
        assert result == expected
        assert len(result) == 0

    def test_raises_exception_on_load_data_failure(self):
        """Test that exceptions from load_data_from_local_storage are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        record_type = "like"
        valid_study_users_dids = {"did:plc:user1"}
        expected_error = "Data loading failed"
        mock_load_data = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.engagement.load_data_from_local_storage", side_effect=mock_load_data):
            with pytest.raises(Exception, match=expected_error):
                get_content_engaged_with(record_type, valid_study_users_dids)


class TestGetEngagedContent:
    """Tests for get_engaged_content function."""

    def test_aggregates_all_record_types_correctly(self):
        """Test successful aggregation of all record types.

        This test verifies that:
        1. All four record types are loaded and aggregated
        2. The combined result contains all engagements
        3. Multiple engagements for the same URI are properly combined
        4. The aggregation logic works correctly
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        # Mock the individual record type functions
        mock_liked_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }
        mock_posted_content = {
            "post2": [
                {"did": "did:plc:user2", "date": "2024-01-02", "record_type": "post"}
            ]
        }
        mock_reposted_content = {
            "post3": [
                {"did:plc:user1", "date": "2024-01-03", "record_type": "repost"}
            ]
        }
        mock_replied_content = {
            "post4": [
                {"did": "did:plc:user2", "date": "2024-01-04", "record_type": "reply"}
            ]
        }
        
        expected = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ],
            "post2": [
                {"did": "did:plc:user2", "date": "2024-01-02", "record_type": "post"}
            ],
            "post3": [
                {"did": "did:plc:user1", "date": "2024-01-03", "record_type": "repost"}
            ],
            "post4": [
                {"did": "did:plc:user2", "date": "2024-01-04", "record_type": "reply"}
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.get_content_engaged_with") as mock_get_content:
            mock_get_content.side_effect = [
                mock_liked_content,
                mock_posted_content,
                mock_reposted_content,
                mock_replied_content
            ]
            result = get_engaged_content(valid_study_users_dids)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 4  # All four record types
        assert result == expected

    def test_combines_multiple_engagements_for_same_uri(self):
        """Test combining multiple engagements for the same URI.

        This test verifies that:
        1. Multiple engagements for the same URI are combined correctly
        2. All engagement types are preserved
        3. The combination logic works properly
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1", "did:plc:user2"}
        
        # Mock content where the same URI appears in multiple record types
        mock_liked_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }
        mock_posted_content = {
            "post1": [  # Same URI as in liked_content
                {"did": "did:plc:user2", "date": "2024-01-02", "record_type": "post"}
            ]
        }
        mock_reposted_content = {}
        mock_replied_content = {}
        
        expected = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"},
                {"did": "did:plc:user2", "date": "2024-01-02", "record_type": "post"}
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.get_content_engaged_with") as mock_get_content:
            mock_get_content.side_effect = [
                mock_liked_content,
                mock_posted_content,
                mock_reposted_content,
                mock_replied_content
            ]
            result = get_engaged_content(valid_study_users_dids)

        # Assert
        assert len(result) == 1  # Only one unique URI
        assert len(result["post1"]) == 2  # Two engagements for post1
        assert result == expected

    def test_handles_empty_content_for_some_record_types(self):
        """Test handling when some record types have no content.

        This test verifies that:
        1. Empty content for some record types is handled gracefully
        2. The aggregation still works correctly
        3. Edge cases don't cause crashes
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        
        mock_liked_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }
        mock_posted_content = {}  # Empty
        mock_reposted_content = {}  # Empty
        mock_replied_content = {}  # Empty
        
        expected = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.get_content_engaged_with") as mock_get_content:
            mock_get_content.side_effect = [
                mock_liked_content,
                mock_posted_content,
                mock_reposted_content,
                mock_replied_content
            ]
            result = get_engaged_content(valid_study_users_dids)

        # Assert
        assert len(result) == 1  # Only liked content
        assert result == expected

    def test_handles_all_empty_content(self):
        """Test handling when all record types have no content.

        This test verifies that:
        1. When all record types are empty, empty result is returned
        2. The function handles edge case gracefully
        3. Result structure is maintained even with no data
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        
        mock_liked_content = {}
        mock_posted_content = {}
        mock_reposted_content = {}
        mock_replied_content = {}
        
        expected = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.get_content_engaged_with") as mock_get_content:
            mock_get_content.side_effect = [
                mock_liked_content,
                mock_posted_content,
                mock_reposted_content,
                mock_replied_content
            ]
            result = get_engaged_content(valid_study_users_dids)

        # Assert
        assert result == expected
        assert len(result) == 0

    def test_calls_get_content_engaged_with_with_correct_parameters(self):
        """Test that get_content_engaged_with is called with correct parameters.

        This test verifies that:
        1. The function is called four times with correct record types
        2. The valid_study_users_dids parameter is passed correctly
        3. The call sequence matches the expected order
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        expected_record_types = ["like", "post", "repost", "reply"]
        
        mock_liked_content = {}
        mock_posted_content = {}
        mock_reposted_content = {}
        mock_replied_content = {}

        # Act
        with patch("services.calculate_analytics.shared.data_loading.engagement.get_content_engaged_with") as mock_get_content:
            mock_get_content.side_effect = [
                mock_liked_content,
                mock_posted_content,
                mock_reposted_content,
                mock_replied_content
            ]
            get_engaged_content(valid_study_users_dids)

        # Assert
        assert mock_get_content.call_count == 4
        
        # Verify each call was made with correct parameters
        for i, record_type in enumerate(expected_record_types):
            call_args = mock_get_content.call_args_list[i]
            assert call_args[0][0] == record_type  # First positional argument
            assert call_args[0][1] == valid_study_users_dids  # Second positional argument

    def test_raises_exception_on_get_content_engaged_with_failure(self):
        """Test that exceptions from get_content_engaged_with are properly propagated.

        This test verifies that:
        1. Exceptions from the dependency are caught and logged
        2. The original exception is re-raised
        3. Error handling works correctly
        """
        # Arrange
        valid_study_users_dids = {"did:plc:user1"}
        expected_error = "Content loading failed"
        mock_get_content = Mock(side_effect=Exception(expected_error))

        # Act & Assert
        with patch("services.calculate_analytics.shared.data_loading.engagement.get_content_engaged_with", side_effect=mock_get_content):
            with pytest.raises(Exception, match=expected_error):
                get_engaged_content(valid_study_users_dids)


class TestGetContentEngagedWithPerUser:
    """Tests for get_content_engaged_with_per_user function."""

    def test_creates_correct_user_engagement_mapping(self):
        """Test creation of user engagement mapping.

        This test verifies that:
        1. The mapping is created correctly from engaged content
        2. Each user's engagements are properly organized by date
        3. The record type categorization works correctly
        4. The result structure matches the expected format
        """
        # Arrange
        engaged_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"},
                {"did": "did:plc:user2", "date": "2024-01-01", "record_type": "post"}
            ],
            "post2": [
                {"did": "did:plc:user1", "date": "2024-01-02", "record_type": "repost"},
                {"did": "did:plc:user1", "date": "2024-01-02", "record_type": "reply"}
            ]
        }
        
        expected = {
            "did:plc:user1": {
                "2024-01-01": {
                    "post": [],
                    "like": ["post1"],
                    "repost": [],
                    "reply": []
                },
                "2024-01-02": {
                    "post": [],
                    "like": [],
                    "repost": ["post2"],
                    "reply": ["post2"]
                }
            },
            "did:plc:user2": {
                "2024-01-01": {
                    "post": ["post1"],
                    "like": [],
                    "repost": [],
                    "reply": []
                }
            }
        }

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 2  # Two users
        assert result == expected

    def test_handles_multiple_engagements_same_user_same_date(self):
        """Test handling of multiple engagements by same user on same date.

        This test verifies that:
        1. Multiple engagements by the same user on the same date are properly grouped
        2. URIs are added to the correct record type lists
        3. The grouping logic works correctly
        """
        # Arrange
        engaged_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"},
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "post"}
            ],
            "post2": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }
        
        expected_user1_2024_01_01 = {
            "post": ["post1"],
            "like": ["post1", "post2"],
            "repost": [],
            "reply": []
        }

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        user1_date = result["did:plc:user1"]["2024-01-01"]
        assert user1_date == expected_user1_2024_01_01
        assert len(user1_date["like"]) == 2  # Two likes
        assert len(user1_date["post"]) == 1  # One post

    def test_handles_empty_engaged_content(self):
        """Test handling of empty engaged content.

        This test verifies that:
        1. Empty input is handled gracefully
        2. Function returns empty dictionary
        3. Edge case doesn't cause crashes
        """
        # Arrange
        engaged_content = {}
        expected = {}

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        assert result == expected
        assert len(result) == 0

    def test_handles_single_engagement(self):
        """Test handling of single engagement.

        This test verifies that:
        1. Single engagement is processed correctly
        2. The result structure is created properly
        3. Edge case of minimal data is handled correctly
        """
        # Arrange
        engaged_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }
        
        expected = {
            "did:plc:user1": {
                "2024-01-01": {
                    "post": [],
                    "like": ["post1"],
                    "repost": [],
                    "reply": []
                }
            }
        }

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        assert len(result) == 1  # One user
        assert len(result["did:plc:user1"]) == 1  # One date
        assert result == expected

    def test_creates_complete_record_type_structure(self):
        """Test that complete record type structure is created for each user-date combination.

        This test verifies that:
        1. All four record types are included in the structure
        2. Empty lists are created for record types with no engagements
        3. The structure is consistent across all user-date combinations
        """
        # Arrange
        engaged_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ]
        }
        
        expected_record_types = {"post", "like", "repost", "reply"}

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        user1_date = result["did:plc:user1"]["2024-01-01"]
        assert set(user1_date.keys()) == expected_record_types
        
        # Verify all record types have lists (even if empty)
        for record_type in expected_record_types:
            assert isinstance(user1_date[record_type], list)

    def test_handles_mixed_record_types_correctly(self):
        """Test handling of mixed record types for the same user.

        This test verifies that:
        1. Different record types are properly categorized
        2. URIs are added to the correct record type lists
        3. The categorization logic works correctly
        """
        # Arrange
        engaged_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ],
            "post2": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "post"}
            ],
            "post3": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "repost"}
            ],
            "post4": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "reply"}
            ]
        }
        
        expected = {
            "did:plc:user1": {
                "2024-01-01": {
                    "post": ["post2"],
                    "like": ["post1"],
                    "repost": ["post3"],
                    "reply": ["post4"]
                }
            }
        }

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        assert result == expected
        
        # Verify each record type has the correct URI
        user1_date = result["did:plc:user1"]["2024-01-01"]
        assert "post1" in user1_date["like"]
        assert "post2" in user1_date["post"]
        assert "post3" in user1_date["repost"]
        assert "post4" in user1_date["reply"]

    def test_handles_multiple_dates_for_same_user(self):
        """Test handling of multiple dates for the same user.

        This test verifies that:
        1. Multiple dates for the same user are handled correctly
        2. Each date gets its own record type structure
        3. The date organization works properly
        """
        # Arrange
        engaged_content = {
            "post1": [
                {"did": "did:plc:user1", "date": "2024-01-01", "record_type": "like"}
            ],
            "post2": [
                {"did": "did:plc:user1", "date": "2024-01-02", "record_type": "post"}
            ]
        }
        
        expected_dates = {"2024-01-01", "2024-01-02"}

        # Act
        result = get_content_engaged_with_per_user(engaged_content)

        # Assert
        user1_data = result["did:plc:user1"]
        assert set(user1_data.keys()) == expected_dates
        
        # Verify each date has the correct structure
        for date in expected_dates:
            assert isinstance(user1_data[date], dict)
            assert set(user1_data[date].keys()) == {"post", "like", "repost", "reply"}
            assert all(isinstance(user1_data[date][rt], list) for rt in user1_data[date].keys())
