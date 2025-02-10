"""Tests for helper.py.

This test suite verifies the functionality of URI to creation timestamp mapping functions:
- map_uris_to_created_at: Loads and extracts URI/timestamp mappings
- map_uris_to_created_at_for_partition_date: Handles single date export
- map_uris_to_created_at_for_partition_dates: Handles multiple dates
"""

import pandas as pd
from unittest.mock import patch, MagicMock

import pytest

from services.map_uri_to_created_at.helper import (
    map_uris_to_created_at,
    map_uris_to_created_at_for_partition_date,
    map_uris_to_created_at_for_partition_dates,
)


@pytest.fixture
def mock_df():
    """Create a mock DataFrame for testing."""
    return pd.DataFrame({
        "uri": ["post1", "post2"],
        "created_at": ["2024-01-01", "2024-01-01"]
    })


class TestMapUrisToCreatedAt:
    """Tests for map_uris_to_created_at function."""
    
    @patch("services.map_uri_to_created_at.helper.load_preprocessed_posts")
    def test_successful_mapping(self, mock_load, mock_df):
        """Test successful loading and mapping of URIs to timestamps."""
        mock_load.return_value = mock_df
        
        result = map_uris_to_created_at("2024-01-01")
        
        mock_load.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-01",
            table_columns=["uri", "created_at"],
            output_format="df"
        )
        assert len(result) == 2
        assert list(result.columns) == ["uri", "created_at"]
        assert list(result["uri"]) == ["post1", "post2"]

    @patch("services.map_uri_to_created_at.helper.load_preprocessed_posts")
    def test_empty_result(self, mock_load):
        """Test handling of empty DataFrame result."""
        mock_load.return_value = pd.DataFrame()
        
        result = map_uris_to_created_at("2024-01-01")
        
        assert len(result) == 0
        assert list(result.columns) == ["uri", "created_at"]


class TestMapUrisToCreatedAtForPartitionDate:
    """Tests for map_uris_to_created_at_for_partition_date function."""

    @patch("services.map_uri_to_created_at.helper.map_uris_to_created_at")
    @patch("services.map_uri_to_created_at.helper.export_data_to_local_storage")
    def test_successful_export(self, mock_export, mock_map, mock_df):
        """Test successful export of URI mappings."""
        mock_map.return_value = mock_df
        
        map_uris_to_created_at_for_partition_date("2024-01-01")
        
        mock_map.assert_called_once_with(partition_date="2024-01-01")
        mock_export.assert_called_once_with(
            service="uris_to_created_at",
            df=mock_df,
            export_format="parquet"
        )

    @patch("services.map_uri_to_created_at.helper.map_uris_to_created_at")
    @patch("services.map_uri_to_created_at.helper.export_data_to_local_storage")
    def test_empty_export(self, mock_export, mock_map):
        """Test export with empty DataFrame."""
        mock_map.return_value = pd.DataFrame(columns=["uri", "created_at"])
        
        map_uris_to_created_at_for_partition_date("2024-01-01")
        
        mock_export.assert_called_once()


class TestMapUrisToCreatedAtForPartitionDates:
    """Tests for map_uris_to_created_at_for_partition_dates function."""

    @patch("services.map_uri_to_created_at.helper.get_partition_dates")
    @patch("services.map_uri_to_created_at.helper.map_uris_to_created_at_for_partition_date")
    def test_multiple_dates(self, mock_map_single, mock_get_dates):
        """Test processing multiple partition dates."""
        mock_get_dates.return_value = ["2024-01-01", "2024-01-02"]
        
        map_uris_to_created_at_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-02"
        )
        
        assert mock_map_single.call_count == 2
        mock_map_single.assert_any_call(partition_date="2024-01-01")
        mock_map_single.assert_any_call(partition_date="2024-01-02")

    @patch("services.map_uri_to_created_at.helper.get_partition_dates")
    @patch("services.map_uri_to_created_at.helper.map_uris_to_created_at_for_partition_date")
    def test_excluded_dates(self, mock_map_single, mock_get_dates):
        """Test date exclusion functionality."""
        mock_get_dates.return_value = ["2024-01-01", "2024-01-03"]
        
        map_uris_to_created_at_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-03",
            exclude_partition_dates=["2024-01-02"]
        )
        
        mock_get_dates.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-03",
            exclude_partition_dates=["2024-01-02"]
        )
        assert mock_map_single.call_count == 2

    @patch("services.map_uri_to_created_at.helper.get_partition_dates")
    @patch("services.map_uri_to_created_at.helper.map_uris_to_created_at_for_partition_date")
    def test_no_dates(self, mock_map_single, mock_get_dates):
        """Test handling of empty date range."""
        mock_get_dates.return_value = []
        
        map_uris_to_created_at_for_partition_dates(
            start_date="2024-01-01",
            end_date="2024-01-01"
        )
        
        mock_map_single.assert_not_called() 