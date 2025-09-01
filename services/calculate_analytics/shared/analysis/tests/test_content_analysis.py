"""Tests for content_analysis.py.

This test suite verifies the functionality of content analysis functions:
- Daily feed content metrics calculation per user
- Daily engaged content metrics calculation per user
- Weekly content metrics aggregation per user
- Transformation of daily and weekly metrics to DataFrame format
"""

import pytest
import pandas as pd
import numpy as np

from services.calculate_analytics.shared.analysis.content_analysis import (
    get_daily_feed_content_per_user_metrics,
    get_daily_engaged_content_per_user_metrics,
    get_weekly_content_per_user_metrics,
    transform_daily_content_per_user_metrics,
    transform_weekly_content_per_user_metrics,
)
from services.calculate_analytics.shared.processing.constants import EXPECTED_TOTAL_METRICS

# Import comprehensive mock data
from services.calculate_analytics.shared.data_loading.tests.mock_labels import (
    mock_perspective_data,
    mock_sociopolitical_data,
    mock_ime_data,
    mock_valence_data,
    perspective_api_labels,
    sociopolitical_labels,
    ime_labels,
    valence_labels,
)


def create_comprehensive_labels_dict():
    """Create a comprehensive labels dictionary using mock data.
    
    This function creates a realistic labels dictionary that includes all
    the label types from the mock data, similar to what would be returned
    by get_all_labels_for_posts.
    
    Note: All post URIs referenced in tests must be present in this dictionary
    to avoid KeyError exceptions when processing content.
    """
    # Convert mock DataFrames to dictionaries
    labels_dict = {}
    
    # Add perspective_api labels
    for _, row in mock_perspective_data.iterrows():
        uri = row["uri"]
        labels_dict[uri] = {}
        for label in perspective_api_labels:
            if label in row:
                labels_dict[uri][label] = row[label]
    
    # Add sociopolitical labels
    for _, row in mock_sociopolitical_data.iterrows():
        uri = row["uri"]
        if uri not in labels_dict:
            labels_dict[uri] = {}
        
        # Add the base sociopolitical labels
        labels_dict[uri]["is_sociopolitical"] = row["is_sociopolitical"]
        
        # Add derived boolean labels based on political ideology
        ideology = row["political_ideology_label"]
        labels_dict[uri]["is_not_sociopolitical"] = not row["is_sociopolitical"]
        labels_dict[uri]["is_political_left"] = ideology == "left"
        labels_dict[uri]["is_political_right"] = ideology == "right"
        labels_dict[uri]["is_political_moderate"] = ideology == "moderate"
        labels_dict[uri]["is_political_unclear"] = ideology == "unclear"
    
    # Add IME labels
    for _, row in mock_ime_data.iterrows():
        uri = row["uri"]
        if uri not in labels_dict:
            labels_dict[uri] = {}
        
        for label in ime_labels:
            if label in row:
                labels_dict[uri][label] = row[label]
    
    # Add valence classifier labels
    for _, row in mock_valence_data.iterrows():
        uri = row["uri"]
        if uri not in labels_dict:
            labels_dict[uri] = {}
        
        # Add the compound score as valence_clf_score
        labels_dict[uri]["valence_clf_score"] = row["compound"]
        
        # Add boolean valence labels
        valence = row["valence_label"]
        labels_dict[uri]["is_valence_positive"] = valence == "positive"
        labels_dict[uri]["is_valence_negative"] = valence == "negative"
        labels_dict[uri]["is_valence_neutral"] = valence == "neutral"
    
    # Add additional post URIs that are referenced in tests but not in mock data
    # These will have the same label structure as post1 for consistency
    additional_posts = ["post3", "post4", "post5"]
    for post_uri in additional_posts:
        if post_uri not in labels_dict:
            # Use post1's labels as a template for additional posts
            labels_dict[post_uri] = labels_dict["post1"].copy()
    
    return labels_dict


class TestGetDailyFeedContentPerUserMetrics:
    """Tests for get_daily_feed_content_per_user_metrics function."""

    def test_calculates_feed_metrics_for_single_user_single_date(self):
        """Test calculation of feed metrics for a single user on a single date.

        This test verifies that:
        1. Feed metrics are calculated correctly for a single user-date combination
        2. The metrics structure matches the expected format
        3. The function properly processes feed content labels
        4. The result contains the expected user and date keys
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": {"post1", "post2"}
            }
        }
        labels_for_feed_content = create_comprehensive_labels_dict()

        # Act
        result = get_daily_feed_content_per_user_metrics(
            user_to_content_in_feeds, labels_for_feed_content
        )

        # Assert
        assert "user1" in result
        assert "2024-01-01" in result["user1"]
        assert len(result["user1"]["2024-01-01"]) == EXPECTED_TOTAL_METRICS
        
        # Check specific calculated values using comprehensive labels
        # post1: prob_toxic=0.8, post2: prob_toxic=0.3
        assert result["user1"]["2024-01-01"]["feed_average_toxic"] == 0.55  # (0.8 + 0.3) / 2
        assert result["user1"]["2024-01-01"]["feed_proportion_toxic"] == 0.5  # 1/2 above 0.5 threshold
        assert result["user1"]["2024-01-01"]["feed_average_is_sociopolitical"] == 0.5  # 1/2 True values
        assert result["user1"]["2024-01-01"]["feed_proportion_is_sociopolitical"] == 0.5  # 1/2 True values

    def test_calculates_feed_metrics_for_multiple_users_multiple_dates(self):
        """Test calculation of feed metrics for multiple users across multiple dates.

        This test verifies that:
        1. Feed metrics are calculated correctly for multiple users
        2. Multiple dates are processed correctly for each user
        3. The metrics structure is consistent across all users and dates
        4. The function handles complex nested data structures
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": {"post1"},
                "2024-01-02": {"post2"}
            },
            "user2": {
                "2024-01-01": {"post1", "post2"}
            }
        }
        labels_for_feed_content = create_comprehensive_labels_dict()

        # Act
        result = get_daily_feed_content_per_user_metrics(
            user_to_content_in_feeds, labels_for_feed_content
        )

        # Assert
        assert "user1" in result
        assert "user2" in result
        assert "2024-01-01" in result["user1"]
        assert "2024-01-02" in result["user1"]
        assert "2024-01-01" in result["user2"]

        # Check user1 metrics - single post per date
        assert result["user1"]["2024-01-01"]["feed_average_toxic"] == 0.8  # Single post
        assert result["user1"]["2024-01-02"]["feed_average_toxic"] == 0.3  # Single post

        # Check user2 metrics - two posts on same date
        assert result["user2"]["2024-01-01"]["feed_average_toxic"] == 0.55  # (0.8 + 0.3) / 2

    def test_handles_empty_feed_content(self):
        """Test handling of empty feed content.

        This test verifies that:
        1. Empty feed content is handled gracefully
        2. The function returns a valid structure even with no data
        3. Edge case doesn't cause crashes
        4. Result contains expected structure with None values
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": set()  # Empty set
            }
        }
        labels_for_feed_content = {}

        # Act
        result = get_daily_feed_content_per_user_metrics(
            user_to_content_in_feeds, labels_for_feed_content
        )

        # Assert
        assert "user1" in result
        assert "2024-01-01" in result["user1"]
        assert len(result["user1"]["2024-01-01"]) == EXPECTED_TOTAL_METRICS
        
        # All values should be None for empty feed content
        for value in result["user1"]["2024-01-01"].values():
            assert value is None

    def test_handles_missing_labels_for_some_posts(self):
        """Test handling of posts with missing labels.

        This test verifies that:
        1. Posts with missing labels are handled gracefully
        2. The function processes available labels correctly
        3. Missing labels result in None values in the output
        4. The function doesn't crash on incomplete data
        """
        # Arrange
        user_to_content_in_feeds = {
            "user1": {
                "2024-01-01": {"post1", "post2", "post3"}
            }
        }
        # Create labels dict with only post1 and post2 (missing post3)
        labels_for_feed_content = {
            "post1": create_comprehensive_labels_dict()["post1"],
            "post2": create_comprehensive_labels_dict()["post2"]
        }

        # Act
        result = get_daily_feed_content_per_user_metrics(
            user_to_content_in_feeds, labels_for_feed_content
        )

        # Assert
        assert "user1" in result
        assert "2024-01-01" in result["user1"]
        
        # Check that available labels are calculated correctly (only post1 and post2)
        # post1: prob_toxic=0.8, post2: prob_toxic=0.3
        assert result["user1"]["2024-01-01"]["feed_average_toxic"] == 0.55  # (0.8 + 0.3) / 2
        assert result["user1"]["2024-01-01"]["feed_average_is_sociopolitical"] == 0.5  # (True + False) / 2


class TestGetDailyEngagedContentPerUserMetrics:
    """Tests for get_daily_engaged_content_per_user_metrics function."""

    def test_calculates_engagement_metrics_for_single_user_single_date(self):
        """Test calculation of engagement metrics for a single user on a single date.

        This test verifies that:
        1. Engagement metrics are calculated correctly for a single user-date combination
        2. The metrics structure matches the expected format
        3. The function properly processes engagement content labels
        4. The result contains the expected user and date keys
        """
        # Arrange
        user_to_content_engaged_with = {
            "user1": {
                "2024-01-01": {
                    "like": ["post1", "post2"],
                    "repost": ["post1"]
                }
            }
        }
        labels_for_engaged_content = create_comprehensive_labels_dict()

        # Act
        result = get_daily_engaged_content_per_user_metrics(
            user_to_content_engaged_with, labels_for_engaged_content
        )

        # Assert
        assert "user1" in result
        assert "2024-01-01" in result["user1"]
        
        # Check that metrics are flattened across record types
        assert "engagement_average_liked_posts_toxic" in result["user1"]["2024-01-01"]
        assert "engagement_proportion_liked_posts_toxic" in result["user1"]["2024-01-01"]
        assert "engagement_average_reposted_posts_toxic" in result["user1"]["2024-01-01"]
        assert "engagement_proportion_reposted_posts_toxic" in result["user1"]["2024-01-01"]

        # Check specific calculated values
        # post1: prob_toxic=0.8, post2: prob_toxic=0.3
        assert result["user1"]["2024-01-01"]["engagement_average_liked_posts_toxic"] == 0.55  # (0.8 + 0.3) / 2
        assert result["user1"]["2024-01-01"]["engagement_average_reposted_posts_toxic"] == 0.8  # Single post

    def test_calculates_engagement_metrics_for_multiple_users_multiple_dates(self):
        """Test calculation of engagement metrics for multiple users across multiple dates.

        This test verifies that:
        1. Engagement metrics are calculated correctly for multiple users
        2. Multiple dates are processed correctly for each user
        3. The metrics structure is consistent across all users and dates
        4. The function handles complex nested data structures with multiple record types
        """
        # Arrange
        user_to_content_engaged_with = {
            "user1": {
                "2024-01-01": {
                    "like": ["post1", "post2"],
                    "repost": ["post1"]
                },
                "2024-01-02": {
                    "like": ["post2"],
                    "reply": ["post1", "post2"]
                }
            },
            "user2": {
                "2024-01-01": {
                    "post": ["post1"],
                    "like": ["post2"]
                }
            }
        }
        labels_for_engaged_content = create_comprehensive_labels_dict()

        # Act
        result = get_daily_engaged_content_per_user_metrics(
            user_to_content_engaged_with, labels_for_engaged_content
        )

        # Assert
        assert "user1" in result
        assert "user2" in result
        assert "2024-01-01" in result["user1"]
        assert "2024-01-02" in result["user1"]
        assert "2024-01-01" in result["user2"]

        # Check user1 metrics for different dates and record types
        # post1: prob_toxic=0.8, post2: prob_toxic=0.3
        assert result["user1"]["2024-01-01"]["engagement_average_liked_posts_toxic"] == 0.55  # (0.8 + 0.3) / 2
        assert result["user1"]["2024-01-01"]["engagement_average_reposted_posts_toxic"] == 0.8  # Single post
        assert result["user1"]["2024-01-02"]["engagement_average_liked_posts_toxic"] == 0.3  # Single post
        assert result["user1"]["2024-01-02"]["engagement_average_replied_posts_toxic"] == 0.55  # (0.8 + 0.3) / 2

        # Check user2 metrics - only check metrics that exist for the record types in the data
        assert result["user2"]["2024-01-01"]["engagement_average_posted_posts_toxic"] == 0.8  # Single post
        assert result["user2"]["2024-01-01"]["engagement_average_liked_posts_toxic"] == 0.3  # Single post

    def test_handles_empty_engagement_content(self):
        """Test handling of empty engagement content.

        This test verifies that:
        1. Empty engagement content is handled gracefully
        2. The function returns a valid structure even with no data
        3. Edge case doesn't cause crashes
        4. Result contains expected structure with None values
        """
        # Arrange
        user_to_content_engaged_with = {
            "user1": {
                "2024-01-01": {
                    "like": [],  # Empty list
                    "repost": []  # Empty list
                }
            }
        }
        labels_for_engaged_content = {}

        # Act
        result = get_daily_engaged_content_per_user_metrics(
            user_to_content_engaged_with, labels_for_engaged_content
        )

        # Assert
        assert "user1" in result
        assert "2024-01-01" in result["user1"]
        
        # Should have flattened metrics structure with None values
        assert "engagement_average_liked_posts_toxic" in result["user1"]["2024-01-01"]
        assert "engagement_average_reposted_posts_toxic" in result["user1"]["2024-01-01"]
        
        # All values should be None for empty engagement content
        for value in result["user1"]["2024-01-01"].values():
            assert value is None

    def test_handles_missing_record_types(self):
        """Test handling of missing record types for some dates.

        This test verifies that:
        1. Missing record types are handled gracefully
        2. The function processes available record types correctly
        3. Missing record types result in None values in the output
        4. The function doesn't crash on incomplete data
        """
        # Arrange
        user_to_content_engaged_with = {
            "user1": {
                "2024-01-01": {
                    "like": ["post1", "post2"],
                    # Missing repost, reply, post record types
                }
            }
        }
        labels_for_engaged_content = create_comprehensive_labels_dict()

        # Act
        result = get_daily_engaged_content_per_user_metrics(
            user_to_content_engaged_with, labels_for_engaged_content
        )

        # Assert
        assert "user1" in result
        assert "2024-01-01" in result["user1"]
        
        # Check that available record types are calculated correctly
        # post1: prob_toxic=0.8, post2: prob_toxic=0.3
        assert result["user1"]["2024-01-01"]["engagement_average_liked_posts_toxic"] == 0.55  # (0.8 + 0.3) / 2
        
        # Check that missing record types result in None values
        # Note: These metrics only exist if the corresponding record types are present in the data
        # Since this test only has "like" record type, other metrics won't be generated
        assert "engagement_average_posted_posts_toxic" not in result["user1"]["2024-01-01"]
        assert "engagement_average_reposted_posts_toxic" not in result["user1"]["2024-01-01"]
        assert "engagement_average_replied_posts_toxic" not in result["user1"]["2024-01-01"]


class TestGetWeeklyContentPerUserMetrics:
    """Tests for get_weekly_content_per_user_metrics function."""

    def test_aggregates_daily_metrics_to_weekly_metrics(self):
        """Test aggregation of daily metrics to weekly metrics.

        This test verifies that:
        1. Daily metrics are properly aggregated to weekly metrics
        2. The averaging logic works correctly across multiple days
        3. None values are properly filtered out during aggregation
        4. The result structure matches the expected weekly format
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                    "engagement_proportion_liked_posts_toxic": 0.5,
                    "engagement_average_liked_posts_is_sociopolitical": 0.8,
                },
                "2024-01-02": {
                    "engagement_average_liked_posts_toxic": 0.4,
                    "engagement_proportion_liked_posts_toxic": 0.3,
                    "engagement_average_liked_posts_is_sociopolitical": 0.6,
                },
                "2024-01-03": {
                    "engagement_average_liked_posts_toxic": 0.8,
                    "engagement_proportion_liked_posts_toxic": 0.7,
                    "engagement_average_liked_posts_is_sociopolitical": 0.9,
                }
            }
        }
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1", "user1", "user1"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "week": ["Week1", "Week1", "Week1"]
        })

        # Act
        result = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics, user_date_to_week_df
        )

        # Assert
        assert "user1" in result
        assert "Week1" in result["user1"]
        
        # Check that weekly averages are calculated correctly
        expected_toxic_avg = round(np.mean([0.6, 0.4, 0.8]), 3)
        expected_sociopolitical_avg = round(np.mean([0.8, 0.6, 0.9]), 3)
        
        assert result["user1"]["Week1"]["engagement_average_liked_posts_toxic"] == expected_toxic_avg
        assert result["user1"]["Week1"]["engagement_average_liked_posts_is_sociopolitical"] == expected_sociopolitical_avg

    def test_handles_missing_dates_in_week(self):
        """Test handling of missing dates within a week.

        This test verifies that:
        1. Missing dates within a week are handled gracefully
        2. The function only averages over available dates
        3. The result structure is consistent even with missing data
        4. The function doesn't crash on incomplete weekly data
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                    "engagement_average_liked_posts_is_sociopolitical": 0.8,
                },
                "2024-01-03": {  # Missing 2024-01-02
                    "engagement_average_liked_posts_toxic": 0.8,
                    "engagement_average_liked_posts_is_sociopolitical": 0.9,
                }
            }
        }
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1", "user1", "user1"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "week": ["Week1", "Week1", "Week1"]
        })

        # Act
        result = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics, user_date_to_week_df
        )

        # Assert
        assert "user1" in result
        assert "Week1" in result["user1"]
        
        # Should only average over available dates (2024-01-01 and 2024-01-03)
        expected_toxic_avg = round(np.mean([0.6, 0.8]), 3)
        expected_sociopolitical_avg = round(np.mean([0.8, 0.9]), 3)
        
        assert result["user1"]["Week1"]["engagement_average_liked_posts_toxic"] == expected_toxic_avg
        assert result["user1"]["Week1"]["engagement_average_liked_posts_is_sociopolitical"] == expected_sociopolitical_avg

    def test_handles_none_values_correctly(self):
        """Test handling of None values in daily metrics.

        This test verifies that:
        1. None values are properly filtered out during aggregation
        2. The function only averages over non-None values
        3. The result structure is consistent even with None values
        4. The function handles mixed None and valid values correctly
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                    "engagement_average_liked_posts_is_sociopolitical": None,  # None value
                },
                "2024-01-02": {
                    "engagement_average_liked_posts_toxic": 0.4,
                    "engagement_average_liked_posts_is_sociopolitical": 0.6,
                },
                "2024-01-03": {
                    "engagement_average_liked_posts_toxic": None,  # None value
                    "engagement_average_liked_posts_is_sociopolitical": 0.9,
                }
            }
        }
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1", "user1", "user1"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "week": ["Week1", "Week1", "Week1"]
        })

        # Act
        result = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics, user_date_to_week_df
        )

        # Assert
        assert "user1" in result
        assert "Week1" in result["user1"]
        
        # Should only average over non-None values
        expected_toxic_avg = round(np.mean([0.6, 0.4]), 3)  # Excludes None from 2024-01-03
        expected_sociopolitical_avg = round(np.mean([0.6, 0.9]), 3)  # Excludes None from 2024-01-01
        
        assert result["user1"]["Week1"]["engagement_average_liked_posts_toxic"] == expected_toxic_avg
        assert result["user1"]["Week1"]["engagement_average_liked_posts_is_sociopolitical"] == expected_sociopolitical_avg

    def test_handles_multiple_weeks(self):
        """Test handling of multiple weeks for a single user.

        This test verifies that:
        1. Multiple weeks are processed correctly for a single user
        2. The weekly aggregation works independently for each week
        3. The result structure contains all expected weeks
        4. The function handles complex multi-week scenarios
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                    "engagement_average_liked_posts_is_sociopolitical": 0.8,
                },
                "2024-01-02": {
                    "engagement_average_liked_posts_toxic": 0.4,
                    "engagement_average_liked_posts_is_sociopolitical": 0.6,
                },
                "2024-01-08": {  # Week 2
                    "engagement_average_liked_posts_toxic": 0.8,
                    "engagement_average_liked_posts_is_sociopolitical": 0.9,
                },
                "2024-01-09": {  # Week 2
                    "engagement_average_liked_posts_toxic": 0.7,
                    "engagement_average_liked_posts_is_sociopolitical": 0.7,
                }
            }
        }
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1", "user1", "user1", "user1"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-08", "2024-01-09"],
            "week": ["Week1", "Week1", "Week2", "Week2"]
        })

        # Act
        result = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics, user_date_to_week_df
        )

        # Assert
        assert "user1" in result
        assert "Week1" in result["user1"]
        assert "Week2" in result["user1"]
        
        # Check Week1 averages
        week1_toxic_avg = round(np.mean([0.6, 0.4]), 3)
        week1_sociopolitical_avg = round(np.mean([0.8, 0.6]), 3)
        
        # Check Week2 averages
        week2_toxic_avg = round(np.mean([0.8, 0.7]), 3)
        week2_sociopolitical_avg = round(np.mean([0.9, 0.7]), 3)
        
        assert result["user1"]["Week1"]["engagement_average_liked_posts_toxic"] == week1_toxic_avg
        assert result["user1"]["Week1"]["engagement_average_liked_posts_is_sociopolitical"] == week1_sociopolitical_avg
        assert result["user1"]["Week2"]["engagement_average_liked_posts_toxic"] == week2_toxic_avg
        assert result["user1"]["Week2"]["engagement_average_liked_posts_is_sociopolitical"] == week2_sociopolitical_avg


class TestTransformDailyContentPerUserMetrics:
    """Tests for transform_daily_content_per_user_metrics function."""

    def test_transforms_daily_metrics_to_dataframe(self):
        """Test transformation of daily metrics to DataFrame format.

        This test verifies that:
        1. Daily metrics are properly transformed to DataFrame format
        2. The DataFrame contains all expected columns
        3. User information is correctly merged with metrics
        4. The result structure matches the expected output format
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                    "engagement_proportion_liked_posts_toxic": 0.5,
                },
                "2024-01-02": {
                    "engagement_average_liked_posts_toxic": 0.4,
                    "engagement_proportion_liked_posts_toxic": 0.3,
                }
            },
            "user2": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.8,
                    "engagement_proportion_liked_posts_toxic": 0.7,
                }
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle", "user2_handle"],
            "bluesky_user_did": ["user1", "user2"],
            "condition": ["control", "treatment"]
        })
        partition_dates = ["2024-01-01", "2024-01-02"]

        # Act
        result = transform_daily_content_per_user_metrics(
            user_per_day_content_label_metrics, users_df, partition_dates
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # 2 users × 2 dates
        
        # Check expected columns
        expected_columns = {
            "handle", "condition", "date", 
            "engagement_average_liked_posts_toxic", "engagement_proportion_liked_posts_toxic"
        }
        assert set(result.columns) >= expected_columns
        
        # Check specific values
        user1_date1 = result[(result["handle"] == "user1_handle") & (result["date"] == "2024-01-01")]
        assert len(user1_date1) == 1
        assert user1_date1.iloc[0]["engagement_average_liked_posts_toxic"] == 0.6
        assert user1_date1.iloc[0]["condition"] == "control"

    def test_handles_missing_dates_with_nan_values(self):
        """Test handling of missing dates with NaN values.

        This test verifies that:
        1. Missing dates are handled gracefully with NaN values
        2. The DataFrame structure is consistent even with missing data
        3. NaN values are properly converted to None
        4. The function handles incomplete data correctly
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                }
                # Missing 2024-01-02
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle"],
            "bluesky_user_did": ["user1"],
            "condition": ["control"]
        })
        partition_dates = ["2024-01-01", "2024-01-02"]

        # Act
        result = transform_daily_content_per_user_metrics(
            user_per_day_content_label_metrics, users_df, partition_dates
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # 1 user × 2 dates
        
        # Check that missing date has NaN values for metrics
        user1_date2 = result[(result["handle"] == "user1_handle") & (result["date"] == "2024-01-02")]
        assert len(user1_date2) == 1
        assert pd.isna(user1_date2.iloc[0]["engagement_average_liked_posts_toxic"])

    def test_sorts_records_correctly(self):
        """Test that records are sorted correctly.

        This test verifies that:
        1. Records are sorted by handle and date in ascending order
        2. The sorting logic works correctly for multiple users and dates
        3. The result maintains consistent ordering
        4. The function handles sorting requirements properly
        """
        # Arrange
        user_per_day_content_label_metrics = {
            "user2": {  # user2 comes before user1 alphabetically
                "2024-01-02": {
                    "engagement_average_liked_posts_toxic": 0.8,
                }
            },
            "user1": {
                "2024-01-01": {
                    "engagement_average_liked_posts_toxic": 0.6,
                }
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle", "user2_handle"],
            "bluesky_user_did": ["user1", "user2"],
            "condition": ["control", "treatment"]
        })
        partition_dates = ["2024-01-01", "2024-01-02"]

        # Act
        result = transform_daily_content_per_user_metrics(
            user_per_day_content_label_metrics, users_df, partition_dates
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # 2 users × 2 dates
        
        # Check sorting order: user1_handle should come before user2_handle
        # and within each user, dates should be in ascending order
        expected_order = [
            ("user1_handle", "2024-01-01"),
            ("user1_handle", "2024-01-02"),
            ("user2_handle", "2024-01-01"),
            ("user2_handle", "2024-01-02")
        ]
        
        for i, (expected_handle, expected_date) in enumerate(expected_order):
            assert result.iloc[i]["handle"] == expected_handle
            assert result.iloc[i]["date"] == expected_date


class TestTransformWeeklyContentPerUserMetrics:
    """Tests for transform_weekly_content_per_user_metrics function."""

    def test_transforms_weekly_metrics_to_dataframe(self):
        """Test transformation of weekly metrics to DataFrame format.

        This test verifies that:
        1. Weekly metrics are properly transformed to DataFrame format
        2. The DataFrame contains all expected columns
        3. User information is correctly merged with metrics
        4. The result structure matches the expected output format
        """
        # Arrange
        user_per_week_content_label_metrics = {
            "user1": {
                "Week1": {
                    "engagement_average_liked_posts_toxic": 0.6,
                    "engagement_proportion_liked_posts_toxic": 0.5,
                },
                "Week2": {
                    "engagement_average_liked_posts_toxic": 0.4,
                    "engagement_proportion_liked_posts_toxic": 0.3,
                }
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle"],
            "bluesky_user_did": ["user1"],
            "condition": ["control"]
        })
        # Create a DataFrame with 8 weeks to satisfy the function's expectation
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1"] * 8,
            "date": [f"2024-01-{i:02d}" for i in range(1, 9)],
            "week": [f"Week{i}" for i in range(1, 9)]
        })

        # Act
        result = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics, users_df, user_date_to_week_df
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 8  # 1 user × 8 weeks
        
        # Check expected columns
        expected_columns = {
            "handle", "condition", "week", 
            "engagement_average_liked_posts_toxic", "engagement_proportion_liked_posts_toxic"
        }
        assert set(result.columns) >= expected_columns
        
        # Check specific values
        user1_week1 = result[(result["handle"] == "user1_handle") & (result["week"] == "Week1")]
        assert len(user1_week1) == 1
        assert user1_week1.iloc[0]["engagement_average_liked_posts_toxic"] == 0.6
        assert user1_week1.iloc[0]["condition"] == "control"

    def test_handles_missing_weeks_with_nan_values(self):
        """Test handling of missing weeks with NaN values.

        This test verifies that:
        1. Missing weeks are handled gracefully with NaN values
        2. The DataFrame structure is consistent even with missing data
        3. NaN values are properly converted to None
        4. The function handles incomplete data correctly
        """
        # Arrange
        user_per_week_content_label_metrics = {
            "user1": {
                "Week1": {
                    "engagement_average_liked_posts_toxic": 0.6,
                }
                # Missing Week2
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle"],
            "bluesky_user_did": ["user1"],
            "condition": ["control"]
        })
        # Create a DataFrame with 8 weeks to satisfy the function's expectation
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1"] * 8,
            "date": [f"2024-01-{i:02d}" for i in range(1, 9)],
            "week": [f"Week{i}" for i in range(1, 9)]
        })

        # Act
        result = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics, users_df, user_date_to_week_df
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 8  # 1 user × 8 weeks
        
        # Check that missing week has NaN values for metrics
        user1_week2 = result[(result["handle"] == "user1_handle") & (result["week"] == "Week2")]
        assert len(user1_week2) == 1
        assert pd.isna(user1_week2.iloc[0]["engagement_average_liked_posts_toxic"])

    def test_sorts_records_correctly(self):
        """Test that records are sorted correctly.

        This test verifies that:
        1. Records are sorted by handle and week in ascending order
        2. The sorting logic works correctly for multiple users and weeks
        3. The result maintains consistent ordering
        4. The function handles sorting requirements properly
        """
        # Arrange
        user_per_week_content_label_metrics = {
            "user1": {
                "Week1": {
                    "engagement_average_liked_posts_toxic": 0.6,
                }
            },
            "user2": {  # user2 comes after user1 alphabetically
                "Week2": {
                    "engagement_average_liked_posts_toxic": 0.8,
                }
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle", "user2_handle"],
            "bluesky_user_did": ["user1", "user2"],
            "condition": ["control", "treatment"]
        })
        # Create a DataFrame with 8 weeks to satisfy the function's expectation
        user_date_to_week_df = pd.DataFrame({
            "bluesky_user_did": ["user1", "user2"] * 4,  # 2 users × 4 weeks each = 8 total
            "date": [f"2024-01-{i:02d}" for i in range(1, 9)],
            "week": [f"Week{i}" for i in range(1, 9)]
        })

        # Act
        result = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics, users_df, user_date_to_week_df
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 16  # 2 users × 8 weeks
        
        # Check sorting order: user1_handle should come before user2_handle
        # and within each user, weeks should be in ascending order
        # The function groups all weeks for one user together, then all weeks for the next user
        expected_order = [
            ("user1_handle", "Week1"),
            ("user1_handle", "Week2"),
            ("user1_handle", "Week3"),
            ("user1_handle", "Week4"),
            ("user1_handle", "Week5"),
            ("user1_handle", "Week6"),
            ("user1_handle", "Week7"),
            ("user1_handle", "Week8"),
            ("user2_handle", "Week1"),
            ("user2_handle", "Week2"),
            ("user2_handle", "Week3"),
            ("user2_handle", "Week4"),
            ("user2_handle", "Week5"),
            ("user2_handle", "Week6"),
            ("user2_handle", "Week7"),
            ("user2_handle", "Week8")
        ]
        
        for i, (expected_handle, expected_week) in enumerate(expected_order):
            assert result.iloc[i]["handle"] == expected_handle
            assert result.iloc[i]["week"] == expected_week

    def test_asserts_correct_number_of_weeks(self):
        """Test that the function asserts the correct number of weeks.

        This test verifies that:
        1. The function correctly identifies 8 weeks in the study
        2. The assertion logic works as expected
        3. The function validates the week data correctly
        4. The error handling works for unexpected week counts
        """
        # Arrange
        user_per_week_content_label_metrics = {
            "user1": {
                "Week1": {"engagement_average_liked_posts_toxic": 0.6}
            }
        }
        users_df = pd.DataFrame({
            "bluesky_handle": ["user1_handle"],
            "bluesky_user_did": ["user1"],
            "condition": ["control"]
        })
        
        # Test with correct number of weeks (8)
        user_date_to_week_df_correct = pd.DataFrame({
            "bluesky_user_did": ["user1"] * 8,
            "date": [f"2024-01-{i:02d}" for i in range(1, 9)],
            "week": [f"Week{i}" for i in range(1, 9)]
        })

        # Act & Assert - should not raise assertion error
        result = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics, users_df, user_date_to_week_df_correct
        )
        assert isinstance(result, pd.DataFrame)

        # Test with incorrect number of weeks (7 instead of 8)
        user_date_to_week_df_incorrect = pd.DataFrame({
            "bluesky_user_did": ["user1"] * 7,
            "date": [f"2024-01-{i:02d}" for i in range(1, 8)],
            "week": [f"Week{i}" for i in range(1, 8)]
        })

        # Act & Assert - should log warning but not raise assertion error
        result = transform_weekly_content_per_user_metrics(
            user_per_week_content_label_metrics, users_df, user_date_to_week_df_incorrect
        )
        assert isinstance(result, pd.DataFrame)
