"""Tests for load_data.py."""

from unittest.mock import patch

import pandas as pd
import pytest
import pandas as pd

from lib.db.sql.helper import normalize_sql
from services.backfill.posts.load_data import (
    INTEGRATIONS_LIST,
    load_preprocessed_posts,
    load_posts_to_backfill,
    load_service_post_uris,
)


class TestLoadPreprocessedPosts:
    """Tests for load_preprocessed_posts function."""

    def assert_sql_contains(self, actual_query: str, expected_substrings: list):
        """Assert that SQL query contains expected substrings."""
        for substring in expected_substrings:
            assert substring in actual_query, f"Expected '{substring}' to be in query"

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_base_case(self, mock_load_data):
        """Test basic functionality with valid start and end dates.
        
        Expected inputs:
            - start_date: "2023-01-01"
            - end_date: "2023-01-31"
            
        Mock behavior:
            - Both cached_df and active_df return empty DataFrames
            
        Expected outputs:
            - Empty list (since mock returns empty DataFrames)
            - Query contains expected clauses
        """
        empty_df = pd.DataFrame(columns=["uri", "text", "created_at"])
        mock_load_data.return_value = empty_df

        result = load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31"
        )

        assert isinstance(result, list)
        assert len(result) == 0
        
        expected_substrings = [
            "FROM preprocessed_posts",
            "SELECT uri, text, created_at",
            "WHERE text IS NOT NULL",
            "text != ''"
        ]
        
        actual_query = mock_load_data.call_args[1]['duckdb_query']
        self.assert_sql_contains(actual_query, expected_substrings)

    def test_missing_start_date(self):
        """Test that error is raised when start_date is not provided.
        
        Expected behavior:
            - Raises ValueError with message about missing start_date
        """
        with pytest.raises(ValueError, match="start_date and end_date must be provided"):
            load_preprocessed_posts(end_date="2023-01-31")

    def test_missing_end_date(self):
        """Test that error is raised when end_date is not provided.
        
        Expected behavior:
            - Raises ValueError with message about missing end_date
        """
        with pytest.raises(ValueError, match="start_date and end_date must be provided"):
            load_preprocessed_posts(start_date="2023-01-01")

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_output_format_list(self, mock_load_data):
        """Test output_format='list' returns list of dictionaries.
        
        Expected inputs:
            - output_format: "list"
            
        Mock behavior:
            - Returns DataFrame with 2 rows
            
        Expected outputs:
            - List of 2 dictionaries
        """
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "created_at": ["2023-01-01", "2023-01-02"]
        })
        mock_load_data.return_value = mock_df

        result = load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            output_format="list"
        )

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(item, dict) for item in result)

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_output_format_df(self, mock_load_data):
        """Test output_format='df' returns DataFrame.
        
        Expected inputs:
            - output_format: "df"
            
        Mock behavior:
            - Returns DataFrame with 2 rows
            
        Expected outputs:
            - DataFrame with 2 rows
        """
        mock_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "created_at": ["2023-01-01", "2023-01-02"]
        })
        mock_load_data.return_value = mock_df

        result = load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            output_format="df"
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_sorted_ascending(self, mock_load_data):
        """Test sorted_by_partition_date=True and ascending=True.
        
        Expected behavior:
            - Query includes ORDER BY partition_date ASC
            - partition_date added to columns
        """
        mock_load_data.return_value = pd.DataFrame()

        load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            sorted_by_partition_date=True,
            ascending=True
        )

        expected_substrings = [
            "SELECT uri, text, created_at, partition_date",
            "FROM preprocessed_posts",
            "ORDER BY partition_date ASC"
        ]
        
        actual_query = mock_load_data.call_args[1]['duckdb_query']
        self.assert_sql_contains(actual_query, expected_substrings)

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_sorted_descending(self, mock_load_data):
        """Test sorted_by_partition_date=True and ascending=False.
        
        Expected behavior:
            - Query includes ORDER BY partition_date DESC
            - partition_date added to columns
        """
        mock_load_data.return_value = pd.DataFrame()

        load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            sorted_by_partition_date=True,
            ascending=False
        )

        expected_substrings = [
            "SELECT uri, text, created_at, partition_date",
            "FROM preprocessed_posts",
            "ORDER BY partition_date DESC"
        ]
        
        actual_query = mock_load_data.call_args[1]['duckdb_query']
        self.assert_sql_contains(actual_query, expected_substrings)

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_no_sort_with_ascending(self, mock_load_data):
        """Test sorted_by_partition_date=False with ascending=True.
        
        Expected behavior:
            - Query has no ORDER BY clause
            - partition_date not added to columns
        """
        mock_load_data.return_value = pd.DataFrame()

        load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            sorted_by_partition_date=False,
            ascending=True
        )

        expected_substrings = [
            "SELECT uri, text, created_at",
            "FROM preprocessed_posts"
        ]
        
        actual_query = mock_load_data.call_args[1]['duckdb_query']
        self.assert_sql_contains(actual_query, expected_substrings)
        assert "ORDER BY" not in actual_query

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_no_partition_date_when_not_sorted(self, mock_load_data):
        """Test partition_date not added when sorted_by_partition_date=False.
        
        Expected behavior:
            - partition_date not in columns
            - No ORDER BY clause
        """
        mock_load_data.return_value = pd.DataFrame()

        load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            sorted_by_partition_date=False
        )

        expected_substrings = [
            "SELECT uri, text, created_at",
            "FROM preprocessed_posts"
        ]
        
        actual_query = mock_load_data.call_args[1]['duckdb_query']
        self.assert_sql_contains(actual_query, expected_substrings)
        assert "partition_date" not in actual_query
        assert "ORDER BY" not in actual_query

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_partition_date_added_when_sorted(self, mock_load_data):
        """Test partition_date is added to columns when sorting."""
        mock_load_data.return_value = pd.DataFrame()
        
        load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            sorted_by_partition_date=True
        )

        expected_substrings = [
            "SELECT uri, text, created_at, partition_date",
            "FROM preprocessed_posts",
            "ORDER BY partition_date DESC"
        ]
        
        actual_query = mock_load_data.call_args[1]['duckdb_query']
        self.assert_sql_contains(actual_query, expected_substrings)

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_cached_only_results(self, mock_load_data):
        """Test when results come only from cached storage.
        
        Mock behavior:
            - cached_df returns DataFrame with 2 rows
            - active_df returns empty DataFrame
            
        Expected outputs:
            - Combined DataFrame has 2 rows from cache
        """
        cached_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "created_at": ["2023-01-01", "2023-01-02"]
        })
        empty_df = pd.DataFrame(columns=["uri", "text", "created_at"])
        
        mock_load_data.side_effect = [cached_df, empty_df]

        result = load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            output_format="df"
        )

        assert len(result) == 2
        assert mock_load_data.call_count == 2

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_active_only_results(self, mock_load_data):
        """Test when results come only from active storage.
        
        Mock behavior:
            - cached_df returns empty DataFrame
            - active_df returns DataFrame with 2 rows
            
        Expected outputs:
            - Combined DataFrame has 2 rows from active
        """
        empty_df = pd.DataFrame(columns=["uri", "text", "created_at"])
        active_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "created_at": ["2023-01-01", "2023-01-02"]
        })
        
        mock_load_data.side_effect = [empty_df, active_df]

        result = load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            output_format="df"
        )

        assert len(result) == 2
        assert mock_load_data.call_count == 2

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_combined_results(self, mock_load_data):
        """Test when results come from both cached and active storage.
        
        Mock behavior:
            - cached_df returns DataFrame with 2 rows
            - active_df returns DataFrame with 2 different rows
            
        Expected outputs:
            - Combined DataFrame has 4 rows total
        """
        cached_df = pd.DataFrame({
            "uri": ["post1", "post2"],
            "text": ["text1", "text2"],
            "created_at": ["2023-01-01", "2023-01-02"]
        })
        active_df = pd.DataFrame({
            "uri": ["post3", "post4"],
            "text": ["text3", "text4"],
            "created_at": ["2023-01-03", "2023-01-04"]
        })
        
        mock_load_data.side_effect = [cached_df, active_df]

        result = load_preprocessed_posts(
            start_date="2023-01-01",
            end_date="2023-01-31",
            output_format="df"
        )

        assert len(result) == 4
        assert mock_load_data.call_count == 2


class TestLoadServicePostUris:
    """Tests for load_service_post_uris function."""

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_load_service_post_uris_cached_only(self, mock_load_data):
        """Test when all posts come from cached storage only.
        
        Expected inputs:
            - service: "test_service"
            - id_field: "uri" (default)
            - start_date: None
            - end_date: None
            
        Mock behavior:
            - cached_df returns DataFrame with 3 URIs
            - active_df returns empty DataFrame
            
        Expected outputs:
            - Returns set of 3 URIs from cached storage
        """
        # Setup mock returns
        cached_df = pd.DataFrame({"uri": ["post1", "post2", "post3"]})
        empty_df = pd.DataFrame({"uri": []})
        
        mock_load_data.side_effect = [cached_df, empty_df]
        
        result = load_service_post_uris("test_service")
        
        assert isinstance(result, set)
        assert result == {"post1", "post2", "post3"}
        assert mock_load_data.call_count == 2

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_load_service_post_uris_active_only(self, mock_load_data):
        """Test when all posts come from active storage only.
        
        Expected inputs:
            - service: "test_service"
            - id_field: "uri" (default)
            - start_date: None
            - end_date: None
            
        Mock behavior:
            - cached_df returns empty DataFrame
            - active_df returns DataFrame with 3 URIs
            
        Expected outputs:
            - Returns set of 3 URIs from active storage
        """
        # Setup mock returns
        empty_df = pd.DataFrame({"uri": []})
        active_df = pd.DataFrame({"uri": ["post4", "post5", "post6"]})
        
        mock_load_data.side_effect = [empty_df, active_df]
        
        result = load_service_post_uris("test_service")
        
        assert isinstance(result, set)
        assert result == {"post4", "post5", "post6"}
        assert mock_load_data.call_count == 2

    @patch("services.backfill.posts.load_data.load_data_from_local_storage")
    def test_load_service_post_uris_combined(self, mock_load_data):
        """Test when posts come from both cached and active storage.
        
        Expected inputs:
            - service: "test_service"
            - id_field: "uri" (default)
            - start_date: None
            - end_date: None
            
        Mock behavior:
            - cached_df returns DataFrame with 2 URIs
            - active_df returns DataFrame with 2 URIs (1 overlapping)
            
        Expected outputs:
            - Returns set of 3 unique URIs combined from both storages
            - Duplicate URIs are handled by set deduplication
        """
        # Setup mock returns
        cached_df = pd.DataFrame({"uri": ["post1", "post2"]})
        active_df = pd.DataFrame({"uri": ["post2", "post3"]})  # post2 is duplicate
        
        mock_load_data.side_effect = [cached_df, active_df]
        
        result = load_service_post_uris("test_service")
        
        assert isinstance(result, set)
        assert result == {"post1", "post2", "post3"}  # post2 appears only once
        assert mock_load_data.call_count == 2


class TestLoadPostsToBackfill:
    """Tests for load_posts_to_backfill function."""

    @pytest.fixture
    def mock_preprocessed_posts(self):
        """Fixture providing mock preprocessed posts data."""
        return [
            {"uri": "post1", "text": "text1"},
            {"uri": "post2", "text": "text2"},
            {"uri": "post3", "text": "text3"},
            {"uri": "post4", "text": "text4"},
            {"uri": "post5", "text": "text5"}
        ]

    @pytest.fixture
    def mock_post_uris(self):
        """Fixture providing mock processed post URIs for each service."""
        return {
            "ml_inference_perspective_api": {"post1", "post2"},
            "ml_inference_sociopolitical": {"post1", "post3", "post4"},
            "ml_inference_ime": set()
        }

    @patch("services.backfill.posts.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts.load_data.load_service_post_uris") 
    def test_load_posts_to_backfill_specific_integration(self, mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
        """Test loading posts to backfill for a specific integration.
        
        Expected inputs:
            - integrations: ["ml_inference_perspective_api"]
            - start_date: None
            - end_date: None
            
        Mock behavior:
            - mock_preprocessed_posts returns list of 5 posts
            - mock_post_uris shows "post1" and "post2" already processed for perspective_api
            
        Expected outputs:
            - Returns dict with single key "ml_inference_perspective_api"
            - Value is list of 3 posts (post3, post4, post5) that need processing
            - Each post is a dict with "uri" and "text" fields
        """
        mock_load_posts.return_value = mock_preprocessed_posts
        
        def mock_load_service_post_uris(service, start_date=None, end_date=None):
            return mock_post_uris[service]
        
        mock_load_uris.side_effect = mock_load_service_post_uris
        
        # Test with a single integration
        result = load_posts_to_backfill(["ml_inference_perspective_api"])

        assert isinstance(result, dict)
        assert len(result) == 1
        assert "ml_inference_perspective_api" in result
        assert isinstance(result["ml_inference_perspective_api"], list)
        assert all(isinstance(post, dict) for post in result["ml_inference_perspective_api"])
        # Should be all posts minus the ones already in ml_inference_perspective_api
        assert len(result["ml_inference_perspective_api"]) == 3
        assert all(post["uri"] in {"post3", "post4", "post5"} for post in result["ml_inference_perspective_api"])

        # Verify default table_columns were used
        mock_load_posts.assert_called_once_with(
            start_date=None,
            end_date=None,
            sorted_by_partition_date=False,
            ascending=False,
            table_columns=["uri", "text", "created_at"],
            output_format="list"
        )

    @patch("services.backfill.posts.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts.load_data.load_service_post_uris")
    def test_load_posts_to_backfill_no_integrations(self, mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
        """Test loading posts to backfill when no integrations specified.
        
        Expected inputs:
            - integrations: None (should default to all integrations)
            - start_date: None
            - end_date: None
            
        Mock behavior:
            - mock_preprocessed_posts returns list of 5 posts
            - mock_post_uris shows different processed posts per integration:
                - perspective_api: post1, post2 processed
                - sociopolitical: post1, post3, post4 processed
                - ime: no posts processed
                
        Expected outputs:
            - Returns dict with all 3 integration keys
            - perspective_api: list of 3 posts (post3, post4, post5)
            - sociopolitical: list of 2 posts (post2, post5)
            - ime: list of all 5 posts
            - Each post is a dict with "uri" and "text" fields
        """
        mock_load_posts.return_value = mock_preprocessed_posts
        
        def mock_load_service_post_uris(service, start_date=None, end_date=None):
            return mock_post_uris[service]
        
        mock_load_uris.side_effect = mock_load_service_post_uris
        
        # Test with None for integrations (should use all integrations)
        result = load_posts_to_backfill(None)
        
        assert isinstance(result, dict)
        assert len(result) == len(INTEGRATIONS_LIST)
        assert all(isinstance(posts, list) for posts in result.values())
        assert all(isinstance(post, dict) for posts in result.values() for post in posts)
        
        # For each integration, should be all posts minus the ones already in that integration
        assert len(result["ml_inference_perspective_api"]) == 3
        assert all(post["uri"] in {"post3", "post4", "post5"} for post in result["ml_inference_perspective_api"])
        
        assert len(result["ml_inference_sociopolitical"]) == 2
        assert all(post["uri"] in {"post2", "post5"} for post in result["ml_inference_sociopolitical"])
        
        assert len(result["ml_inference_ime"]) == 5
        assert all(post["uri"] in {"post1", "post2", "post3", "post4", "post5"} for post in result["ml_inference_ime"])

        # Verify default table_columns were used
        mock_load_posts.assert_called_once_with(
            start_date=None,
            end_date=None,
            sorted_by_partition_date=False,
            ascending=False,
            table_columns=["uri", "text", "created_at"],
            output_format="list"
        )

    @patch("services.backfill.posts.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts.load_data.load_service_post_uris")
    def test_load_posts_to_backfill_multiple_integrations(self, mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
        """Test loading posts to backfill for multiple specific integrations.
        
        Expected inputs:
            - integrations: ["ml_inference_perspective_api", "ml_inference_ime"]
            - start_date: None
            - end_date: None
            
        Mock behavior:
            - mock_preprocessed_posts returns list of 5 posts
            - mock_post_uris shows:
                - perspective_api: post1, post2 processed
                - ime: no posts processed
                
        Expected outputs:
            - Returns dict with 2 integration keys
            - perspective_api: list of 3 posts (post3, post4, post5)
            - ime: list of all 5 posts
            - Each post is a dict with "uri" and "text" fields
        """
        mock_load_posts.return_value = mock_preprocessed_posts
        
        def mock_load_service_post_uris(service, start_date=None, end_date=None):
            return mock_post_uris[service]
        
        mock_load_uris.side_effect = mock_load_service_post_uris
        
        # Test with multiple specific integrations
        result = load_posts_to_backfill([
            "ml_inference_perspective_api",
            "ml_inference_ime"
        ])

        assert isinstance(result, dict)
        assert len(result) == 2
        assert "ml_inference_perspective_api" in result
        assert "ml_inference_ime" in result
        assert all(isinstance(posts, list) for posts in result.values())
        assert all(isinstance(post, dict) for posts in result.values() for post in posts)
        
        assert len(result["ml_inference_perspective_api"]) == 3
        assert all(post["uri"] in {"post3", "post4", "post5"} for post in result["ml_inference_perspective_api"])
        
        assert len(result["ml_inference_ime"]) == 5
        assert all(post["uri"] in {"post1", "post2", "post3", "post4", "post5"} for post in result["ml_inference_ime"])

        # Verify default table_columns were used
        mock_load_posts.assert_called_once_with(
            start_date=None,
            end_date=None,
            sorted_by_partition_date=False,
            ascending=False,
            table_columns=["uri", "text", "created_at"],
            output_format="list"
        )

    @patch("services.backfill.posts.load_data.load_preprocessed_posts")
    @patch("services.backfill.posts.load_data.load_service_post_uris")
    def test_load_posts_to_backfill_with_date_range(self, mock_load_uris, mock_load_posts, mock_preprocessed_posts, mock_post_uris):
        """Test loading posts to backfill with date range filters.
        
        Expected inputs:
            - integrations: ["ml_inference_perspective_api"]
            - start_date: "2024-01-01"
            - end_date: "2024-01-31"
            
        Mock behavior:
            - mock_preprocessed_posts returns list of 5 posts
            - mock_post_uris shows post1, post2 processed for perspective_api
            - Date range should be passed through to both load functions
            
        Expected outputs:
            - Returns dict with single key "ml_inference_perspective_api"
            - Both mock functions should be called with the date range parameters
        """
        mock_load_posts.return_value = mock_preprocessed_posts
        
        def mock_load_service_post_uris(service, start_date=None, end_date=None):
            return mock_post_uris[service]
        
        mock_load_uris.side_effect = mock_load_service_post_uris
        
        # Test with date range
        result = load_posts_to_backfill(
            ["ml_inference_perspective_api"],
            start_date="2024-01-01",
            end_date="2024-01-31"
        )

        # Verify the date range was passed through with default table_columns
        mock_load_posts.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-31",
            sorted_by_partition_date=False,
            ascending=False,
            table_columns=["uri", "text", "created_at"],
            output_format="list"
        )
        mock_load_uris.assert_called_once_with(
            service="ml_inference_perspective_api",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )

        assert isinstance(result, dict)
        assert "ml_inference_perspective_api" in result
