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
    default_num_days_lookback,
    default_min_lookback_date,
    default_preprocessed_posts_columns,
    INTEGRATIONS_LIST,
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
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06"]),
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05", 
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_no_preprocessed_posts_raises_error(self, mock_load_feeds, mock_load_preprocessed):
        """Test case where there are no preprocessed posts loaded raises an error."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2"]
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": [],
            "text": [],
            "preprocessing_timestamp": pd.Series(dtype='datetime64[ns]'),
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
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06"]),
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result["uri"].values) == {"post1", "post2"}

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
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"]),
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result["uri"].values) == {"post1", "post2"}

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
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06"]),
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
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"]),
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3  # Should deduplicate
        assert set(result["uri"].values) == {"post1", "post2", "post3"}

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
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06"]),
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
            "preprocessing_timestamp": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"]),
            "partition_date": pd.to_datetime(["2024-10-05", "2024-10-06", "2024-10-07"])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result["uri"].values) == {"post1", "post2"}

    @patch("services.backfill.posts_used_in_feeds.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_posts_used_in_feeds")
    def test_deduplication_by_preprocessing_timestamp(self, mock_load_feeds, mock_load_preprocessed):
        """Test that posts are deduplicated by keeping earliest preprocessing_timestamp."""
        mock_load_feeds.return_value = pd.DataFrame({
            "uri": ["post1", "post2"]
        })
        mock_load_preprocessed.return_value = pd.DataFrame({
            "uri": ["post1", "post1", "post2", "post2"],
            "text": ["text1_v2", "text1_v1", "text2_v2", "text2_v1"],
            "preprocessing_timestamp": pd.to_datetime([
                "2024-10-06",  # post1 v2
                "2024-10-05",  # post1 v1 - should keep this one
                "2024-10-07",  # post2 v2
                "2024-10-06",  # post2 v1 - should keep this one
            ]),
            "partition_date": pd.to_datetime([
                "2024-10-06",
                "2024-10-05",
                "2024-10-07",
                "2024-10-06"
            ])
        })
        
        result = load_preprocessed_posts_used_in_feeds_for_partition_date(
            partition_date="2024-10-10",
            lookback_start_date="2024-10-05",
            lookback_end_date="2024-10-10"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # Should deduplicate to 2 posts
        assert set(result["uri"].values) == {"post1", "post2"}
        
        # Verify we kept the earliest versions
        post1_row = result[result["uri"] == "post1"].iloc[0]
        post2_row = result[result["uri"] == "post2"].iloc[0]
        assert post1_row["text"] == "text1_v1"  # Earlier timestamp
        assert post2_row["text"] == "text2_v1"  # Earlier timestamp
        assert post1_row["preprocessing_timestamp"] == pd.to_datetime("2024-10-05")
        assert post2_row["preprocessing_timestamp"] == pd.to_datetime("2024-10-06")


class TestLoadPostsToBackfill:
    """Tests for load_posts_to_backfill function."""

    @pytest.fixture
    def mock_posts_df(self):
        """Fixture for mock posts DataFrame."""
        return pd.DataFrame([
            {"uri": "post1", "text": "text1"},
            {"uri": "post2", "text": "text2"},
            {"uri": "post3", "text": "text3"},
            {"uri": "post4", "text": "text4"},
            {"uri": "post5", "text": "text5"}
        ])

    @patch("services.backfill.posts_used_in_feeds.load_data.load_service_post_uris")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    def test_filter_by_integration(self, mock_load_data, mock_load_uris, mock_posts_df):
        """Test filtering posts by integration status.
        
        Verifies:
        - Returns correct structure (dict mapping integration to list of posts)
        - Correctly filters out posts already processed by each integration
        - Handles multiple integrations appropriately
        """
        mock_load_data.return_value = mock_posts_df
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
        
        # Verify correct parameters were passed to load_data_from_local_storage
        mock_load_data.assert_called_once_with(
            service="preprocessed_posts_used_in_feeds",
            directory="cache",
            export_format="duckdb",
            duckdb_query=f"SELECT {','.join(default_preprocessed_posts_columns)} FROM preprocessed_posts_used_in_feeds;",
            query_metadata={
                "tables": [
                    {
                        "name": "preprocessed_posts_used_in_feeds",
                        "columns": default_preprocessed_posts_columns,
                    }
                ]
            },
            partition_date=partition_date,
        )

    @patch("services.backfill.posts_used_in_feeds.load_data.load_service_post_uris")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    def test_empty_integrations_list(self, mock_load_data, mock_load_uris, mock_posts_df):
        """Test behavior when no integrations are specified.
        
        Verifies:
        - Uses default INTEGRATIONS_LIST when no integrations provided
        - Processes all default integrations correctly
        - Returns results for all default integrations
        """
        mock_load_data.return_value = mock_posts_df
        mock_load_uris.return_value = {"post1"}  # One post processed
        
        result = load_posts_to_backfill(
            partition_date="2024-10-10",
            integrations=None
        )
        
        assert isinstance(result, dict)
        assert len(result) == len(INTEGRATIONS_LIST)
        for integration in INTEGRATIONS_LIST:
            assert integration in result
            assert isinstance(result[integration], list)
            assert len(result[integration]) == 4  # All posts except post1
            assert all(post["uri"] in {"post2", "post3", "post4", "post5"} 
                      for post in result[integration])

    @patch("services.backfill.posts_used_in_feeds.load_data.load_service_post_uris")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    def test_multiple_integrations(self, mock_load_data, mock_load_uris, mock_posts_df):
        """Test handling multiple integrations.
        
        Verifies:
        - Can process multiple integrations simultaneously
        - Each integration gets correct filtered posts
        - Different processed posts per integration handled correctly
        """
        mock_load_data.return_value = mock_posts_df
        
        def mock_load_uris_impl(service, **kwargs):
            if service == "ml_inference_perspective_api":
                return {"post1", "post2"}
            elif service == "ml_inference_sociopolitical":
                return {"post3", "post4"}
            return set()
            
        mock_load_uris.side_effect = mock_load_uris_impl
        
        result = load_posts_to_backfill(
            partition_date="2024-10-10",
            integrations=["ml_inference_perspective_api", "ml_inference_sociopolitical"]
        )
        
        assert len(result) == 2
        assert len(result["ml_inference_perspective_api"]) == 3  # posts 3,4,5
        assert len(result["ml_inference_sociopolitical"]) == 3  # posts 1,2,5
        assert all(post["uri"] in {"post3", "post4", "post5"} 
                  for post in result["ml_inference_perspective_api"])
        assert all(post["uri"] in {"post1", "post2", "post5"} 
                  for post in result["ml_inference_sociopolitical"])

    @patch("services.backfill.posts_used_in_feeds.load_data.load_service_post_uris")
    @patch("services.backfill.posts_used_in_feeds.load_data.load_data_from_local_storage")
    def test_date_range_validation(self, mock_load_data, mock_load_uris, mock_posts_df):
        """Test that date ranges are properly validated and passed.
        
        Verifies:
        - Correct date range calculation using default lookback
        - Proper passing of date range to load_service_post_uris
        - Handling of min_lookback_date constraint
        """
        mock_load_data.return_value = mock_posts_df
        mock_load_uris.return_value = {"post1"}
        
        partition_date = "2024-10-10"
        result = load_posts_to_backfill(
            partition_date=partition_date,
            integrations=["ml_inference_perspective_api"]
        )
        
        # Verify date range passed to load_service_post_uris
        mock_load_uris.assert_called_once_with(
            service="ml_inference_perspective_api",
            start_date="2024-10-05",  # 5 days before partition_date
            end_date="2024-10-10"
        )
        
        assert len(result) == 1
        assert len(result["ml_inference_perspective_api"]) == 4
