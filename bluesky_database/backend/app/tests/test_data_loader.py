"""Tests for data loading integration (MET-18).

This module contains tests for the DataLoader class and data loading functionality.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from app.data_loader import DataLoader, data_loader


class TestDataLoader:
    """Test cases for the DataLoader class."""
    
    def test_dataloader_initialization(self):
        """Test that DataLoader initializes correctly."""
        loader = DataLoader()
        assert hasattr(loader, 'available')
        assert isinstance(loader.available, bool)
    
    def test_singleton_instance(self):
        """Test that the singleton data_loader instance exists."""
        assert data_loader is not None
        assert isinstance(data_loader, DataLoader)
    
    def test_validate_date_format_valid(self):
        """Test date format validation with valid dates."""
        loader = DataLoader()
        
        assert loader.validate_date_format("2024-01-01") is True
        assert loader.validate_date_format("2023-12-31") is True
        assert loader.validate_date_format("2024-02-29") is True  # Leap year
    
    def test_validate_date_format_invalid(self):
        """Test date format validation with invalid dates."""
        loader = DataLoader()
        
        assert loader.validate_date_format("2024-1-1") is False  # Wrong format
        assert loader.validate_date_format("01-01-2024") is False  # Wrong format
        assert loader.validate_date_format("2024/01/01") is False  # Wrong format
        assert loader.validate_date_format("invalid-date") is False
        assert loader.validate_date_format("2023-13-01") is False  # Invalid month
        assert loader.validate_date_format("2023-02-30") is False  # Invalid day
    
    def test_load_posts_missing_dates(self):
        """Test that load_posts raises ValueError for missing dates."""
        loader = DataLoader()
        
        with pytest.raises(ValueError, match="start_date and end_date must be provided"):
            loader.load_posts("", "2024-01-02")
        
        with pytest.raises(ValueError, match="start_date and end_date must be provided"):
            loader.load_posts("2024-01-01", "")
        
        with pytest.raises(ValueError, match="start_date and end_date must be provided"):
            loader.load_posts("", "")
    
    @patch('app.data_loader.load_preprocessed_posts')
    def test_load_posts_success(self, mock_load_func):
        """Test successful post loading."""
        # Mock the function to return test data
        mock_data = pd.DataFrame({
            'uri': ['at://example.com/post/1', 'at://example.com/post/2'],
            'text': ['Test post 1', 'Test post 2'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z']
        })
        mock_load_func.return_value = mock_data
        
        loader = DataLoader()
        result = loader.load_posts("2024-01-01", "2024-01-02")
        
        # Verify the function was called with correct parameters
        mock_load_func.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-02",
            sorted_by_partition_date=True,
            ascending=False,
            table_columns=None,
            output_format="df",
            convert_ts_fields=False,
        )
        
        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['uri', 'text', 'preprocessing_timestamp']
    
    @patch('app.data_loader.load_preprocessed_posts')
    def test_load_posts_failure(self, mock_load_func):
        """Test post loading failure handling."""
        # Mock the function to raise an exception
        mock_load_func.side_effect = Exception("Database connection failed")
        
        loader = DataLoader()
        
        with pytest.raises(RuntimeError, match="Failed to load preprocessed posts: Database connection failed"):
            loader.load_posts("2024-01-01", "2024-01-02")
    
    def test_filter_posts_no_filters(self):
        """Test filtering posts with no filters applied."""
        loader = DataLoader()
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'uri': ['post1', 'post2', 'post3'],
            'text': ['Hello world', 'Test post', 'Another post'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z', '2024-01-01T02:00:00Z']
        })
        
        result = loader.filter_posts(test_df)
        
        # Should return the same DataFrame
        pd.testing.assert_frame_equal(result, test_df)
    
    def test_filter_posts_with_query(self):
        """Test filtering posts with text query."""
        loader = DataLoader()
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'uri': ['post1', 'post2', 'post3'],
            'text': ['Hello world', 'Test post', 'Another hello'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z', '2024-01-01T02:00:00Z']
        })
        
        result = loader.filter_posts(test_df, query="hello")
        
        # Should return only posts containing "hello" (case insensitive)
        assert len(result) == 2
        assert 'Hello world' in result['text'].values
        assert 'Another hello' in result['text'].values
        assert 'Test post' not in result['text'].values
    
    def test_filter_posts_with_limit(self):
        """Test filtering posts with limit."""
        loader = DataLoader()
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'uri': ['post1', 'post2', 'post3'],
            'text': ['Hello world', 'Test post', 'Another post'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z', '2024-01-01T02:00:00Z']
        })
        
        result = loader.filter_posts(test_df, limit=2)
        
        # Should return only first 2 posts
        assert len(result) == 2
        assert result.iloc[0]['text'] == 'Hello world'
        assert result.iloc[1]['text'] == 'Test post'
    
    def test_filter_posts_combined_filters(self):
        """Test filtering posts with both query and limit."""
        loader = DataLoader()
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'uri': ['post1', 'post2', 'post3', 'post4'],
            'text': ['Hello world', 'Test hello', 'Another hello', 'Different post'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z', '2024-01-01T02:00:00Z', '2024-01-01T03:00:00Z']
        })
        
        result = loader.filter_posts(test_df, query="hello", limit=2)
        
        # Should return only first 2 posts containing "hello"
        assert len(result) == 2
        assert 'Hello world' in result['text'].values
        assert 'Test hello' in result['text'].values
        assert 'Different post' not in result['text'].values
    
    def test_get_posts_summary_basic(self):
        """Test getting posts summary with basic DataFrame."""
        loader = DataLoader()
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'uri': ['post1', 'post2'],
            'text': ['Hello world', 'Test post'],
            'preprocessing_timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z']
        })
        
        summary = loader.get_posts_summary(test_df)
        
        assert summary['total_count'] == 2
        assert 'uri' in summary['columns']
        assert 'text' in summary['columns']
        assert 'preprocessing_timestamp' in summary['columns']
        assert 'memory_usage' in summary
        assert 'date_range' in summary
        assert 'earliest' in summary['date_range']
        assert 'latest' in summary['date_range']
    
    def test_get_posts_summary_no_timestamp(self):
        """Test getting posts summary with DataFrame without timestamp column."""
        loader = DataLoader()
        
        # Create test DataFrame without preprocessing_timestamp
        test_df = pd.DataFrame({
            'uri': ['post1', 'post2'],
            'text': ['Hello world', 'Test post']
        })
        
        summary = loader.get_posts_summary(test_df)
        
        assert summary['total_count'] == 2
        assert 'uri' in summary['columns']
        assert 'text' in summary['columns']
        assert 'memory_usage' in summary
        assert 'date_range' not in summary  # Should not be present without timestamp column
    
    def test_check_data_availability(self):
        """Test checking data availability."""
        loader = DataLoader()
        
        # The availability check should complete without error
        availability = loader._check_data_availability()
        assert isinstance(availability, bool)
    
    @patch('app.data_loader.load_preprocessed_posts')
    def test_load_posts_with_custom_parameters(self, mock_load_func):
        """Test loading posts with custom parameters."""
        mock_load_func.return_value = pd.DataFrame()
        
        loader = DataLoader()
        loader.load_posts(
            start_date="2024-01-01",
            end_date="2024-01-02",
            sorted_by_partition_date=False,
            ascending=True,
            table_columns=["uri", "text"],
            output_format="list",
            convert_ts_fields=True
        )
        
        # Verify the function was called with custom parameters
        mock_load_func.assert_called_once_with(
            start_date="2024-01-01",
            end_date="2024-01-02",
            sorted_by_partition_date=False,
            ascending=True,
            table_columns=["uri", "text"],
            output_format="list",
            convert_ts_fields=True,
        )