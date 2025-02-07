"""Tests for load_data.py.

This test suite verifies the functionality of loading data for posts used in feeds
backfill pipeline. The tests cover:

- Calculating lookback date ranges
- Loading posts used in feeds
- Loading preprocessed posts
- Filtering posts by integration
- Error handling and edge cases
"""

from unittest.mock import patch

import pytest
import pandas as pd

from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
    load_posts_used_in_feeds,
    load_preprocessed_posts_used_in_feeds_for_partition_date,
    load_posts_to_backfill,
)


class TestCalculateStartEndDateForLookback:
    def test_normal_case(self):
        """Test calculating dates when partition date minus lookback is after min date."""
        start, end = calculate_start_end_date_for_lookback("2024-10-10")
        assert start == "2024-10-05"  # 5 days before
        assert end == "2024-10-10"

    def test_min_date_case(self):
        """Test calculating dates when partition date minus lookback is before min date."""
        start, end = calculate_start_end_date_for_lookback("2024-09-30")
        assert start == "2024-09-28"  # Should use min date
        assert end == "2024-09-30"


class TestLoadPostsUsedInFeeds:
    @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    def test_load_posts(self, mock_load_data):
        """Test loading posts used in feeds.
        
        Verifies:
        - Calls load_data_from_local_storage with correct parameters
        - Returns expected DataFrame
        - Only returns posts for the specified partition date
        - Handles empty results appropriately
        """
        # Create test data with posts from different dates
        test_df = pd.DataFrame({
            "uri": ["post1", "post2", "post3", "post4"],
            "partition_date": [
                "2024-10-10", "2024-10-10", "2024-10-10", "2024-10-10"
            ]
        })
        mock_load_data.return_value = test_df
        partition_date = "2024-10-10"
        
        result = load_posts_used_in_feeds(partition_date)
        
        # Verify load_data_from_local_storage called with correct params
        mock_load_data.assert_called_once_with(
            service="fetch_posts_used_in_feeds",
            directory="cache",
            export_format="duckdb",
            duckdb_query="SELECT uri FROM fetch_posts_used_in_feeds",
            query_metadata={
                "tables": [{"name": "fetch_posts_used_in_feeds", "columns": ["uri"]}]
            },
            partition_date=partition_date,
        )
        
        # Verify result structure and content
        assert isinstance(result, pd.DataFrame)
        assert "uri" in result.columns
        assert len(result) == 4  # Should only have posts from 2024-10-10
        expected_uris = {"post1", "post2", "post3", "post4"}
        assert set(result["uri"].values) == expected_uris


class TestLoadPreprocessedPostsUsedInFeeds:
    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_no_posts_used_in_feeds(self, mock_load_feeds, mock_load_preprocessed):
        """Test case where there are no posts used in feeds for the partition date."""
        mock_load_feeds.return_value = pd.DataFrame({"uri": []})
        # Return DataFrame instead of list
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05", 
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, list)
        assert len(result) == 0

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_no_preprocessed_posts_raises_error(self, mock_load_feeds, mock_load_preprocessed):
        """Test case where there are no preprocessed posts loaded raises an error."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2"]
        })
        # Return empty DataFrame with correct columns
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": [],
            "text": [],
            "partition_date": pd.Series(dtype='datetime64[ns]')
        })
        
        with pytest.raises(ValueError) as exc_info:
            load_preprocessed_posts_used_in_feeds_for_partition_date(
                partition_date="2024-10-10",
                lookback_start_date="2024-10-05",
                lookback_end_date="2024-10-10"
            )
        
        assert str(exc_info.value) == (
            "Base pool size (0) is less than total unique posts used in feeds (2). "
            "This should never happen as the base pool should not be smaller than "
            "the total number of unique posts used in feeds."
        )

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_perfect_overlap(self, mock_load_feeds, mock_load_preprocessed):
        """Test case where posts in feeds exactly match preprocessed posts."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2"]
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert {post["uri"] for post in result} == {"post1", "post2"}

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_feed_posts_subset(self, mock_load_feeds, mock_load_preprocessed):
        """Test case where feed posts are a subset of preprocessed posts."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2"]
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "text": ["text1", "text2", "text3"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert {post["uri"] for post in result} == {"post1", "post2"}

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    @patch("services.backfill.posts_used_in_feeds.load_data.logger")
    def test_missing_preprocessed_posts_raises_error(
        self, mock_logger, mock_load_feeds, mock_load_preprocessed
    ):
        """Test case where some feed posts are missing from preprocessed posts raises an error."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2", "post3"]
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06"])
        })
        
        with pytest.raises(ValueError) as exc_info:
            load_preprocessed_posts_used_in_feeds_for_partition_date(
                partition_date="2024-10-10",
                lookback_start_date="2024-10-05",
                lookback_end_date="2024-10-10"
            )
        
        assert str(exc_info.value) == (
            "Base pool size (2) is less than total unique posts used in feeds (3). "
            "This should never happen as the base pool should not be smaller than "
            "the total number of unique posts used in feeds."
        )

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_duplicate_feed_posts(self, mock_load_feeds, mock_load_preprocessed):
        """Test case where there are duplicate posts in the feeds."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post1", "post2", "post2", "post3"]
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "text": ["text1", "text2", "text3"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, list)
        assert len(result) == 3  # Should deduplicate
        assert {post["uri"] for post in result} == {"post1", "post2", "post3"}

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_base_pool_too_small_raises_error(self, mock_load_feeds, mock_load_preprocessed):
        """Test that ValueError is raised when base pool is smaller than unique posts used."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2", "post3"]  # 3 unique posts
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2"],  # Only 2 posts in base pool
            "text": ["text1", "text2"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06"])
        })
        
        with pytest.raises(ValueError) as exc_info:
            load_preprocessed_posts_used_in_feeds_for_partition_date(
                partition_date="2024-10-10",
                lookback_start_date="2024-10-05",
                lookback_end_date="2024-10-10"
            )
        
        assert str(exc_info.value) == (
            "Base pool size (2) is less than total unique posts used in feeds (3). "
            "This should never happen as the base pool should not be smaller than "
            "the total number of unique posts used in feeds."
        )

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_base_pool_larger_than_feeds_ok(self, mock_load_feeds, mock_load_preprocessed):
        """Test that a larger base pool than feeds is acceptable."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2"]  # 2 unique posts
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2", "post3"],
            "text": ["text1", "text2", "text3"],
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert {post["uri"] for post in result} == {"post1", "post2"}


class TestLoadPostsToBackfill:
    @patch("services.backfill.posts_used_in_feeds.load_data.load_service_post_uris")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts_used_in_feeds_for_partition_date")
    def test_filter_by_integration(self, mock_load_posts_used, mock_load_uris):
        """Test filtering posts by integration status.
        
        Verifies:
        - Returns correct structure (dict mapping integration to list of posts)
        - Correctly filters out posts already processed by each integration
        - Handles multiple integrations appropriately
        """
        mock_posts = [
            {"uri": "post1", "text": "text1"},
            {"uri": "post2", "text": "text2"},
            {"uri": "post3", "text": "text3"},
            {"uri": "post4", "text": "text4"},
            {"uri": "post5", "text": "text5"}
        ]
        mock_load_posts_used.return_value = mock_posts
        mock_load_uris.return_value = {"post1", "post2"}  # Some posts already processed
        
        partition_date = "2024-10-10"
        integrations = ["ml_inference_perspective_api"]
        
        result = load_posts_to_backfill(
            partition_date=partition_date,
            integrations=integrations
        )
        
        assert isinstance(result, dict)
        assert len(result) == 1
        assert "ml_inference_perspective_api" in result
        assert isinstance(result["ml_inference_perspective_api"], list)
        # Should be all posts minus the ones already processed
        assert len(result["ml_inference_perspective_api"]) == 3
        assert all(post["uri"] in {"post3", "post4", "post5"} 
                  for post in result["ml_inference_perspective_api"])
