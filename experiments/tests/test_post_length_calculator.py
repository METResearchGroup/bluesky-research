"""Tests for post_length_calculator module.

This module tests the functionality of creating mock social media posts
and calculating their average length.
"""

import pytest
import pandas as pd
import sys
import os

# Add the parent directory to path to import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from post_length_calculator import create_mock_posts, calculate_average_post_length


class TestCreateMockPosts:
    """Test cases for the create_mock_posts function."""
    
    def test_creates_correct_number_of_posts(self):
        """Test that exactly 10 mock posts are created."""
        posts_df = create_mock_posts()
        assert len(posts_df) == 10
    
    def test_has_required_columns(self):
        """Test that the DataFrame has the required 'text' and 'id' columns."""
        posts_df = create_mock_posts()
        assert 'text' in posts_df.columns
        assert 'id' in posts_df.columns
    
    def test_posts_have_unique_ids(self):
        """Test that all posts have unique IDs."""
        posts_df = create_mock_posts()
        assert posts_df['id'].nunique() == len(posts_df)
    
    def test_all_posts_have_text(self):
        """Test that all posts have non-empty text content."""
        posts_df = create_mock_posts()
        assert posts_df['text'].notna().all()
        assert (posts_df['text'].str.len() > 0).all()
    
    def test_posts_are_strings(self):
        """Test that all text content is string type."""
        posts_df = create_mock_posts()
        assert posts_df['text'].dtype == 'object'  # pandas string dtype
        assert all(isinstance(text, str) for text in posts_df['text'])


class TestCalculateAveragePostLength:
    """Test cases for the calculate_average_post_length function."""
    
    def test_calculates_correct_average_with_mock_data(self):
        """Test that the average length is calculated correctly with mock data."""
        posts_df = create_mock_posts()
        avg_length = calculate_average_post_length(posts_df)
        
        # Calculate expected average manually
        expected_lengths = [len(text) for text in posts_df['text']]
        expected_avg = sum(expected_lengths) / len(expected_lengths)
        
        assert abs(avg_length - expected_avg) < 0.01  # Allow for floating point precision
        assert isinstance(avg_length, float)
    
    def test_calculates_correct_average_with_simple_data(self):
        """Test average calculation with simple test data."""
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'text': ['Hi', 'Hello', 'Hey there!']
        })
        # Lengths: 2, 5, 10 -> Average: 17/3 = 5.666...
        avg_length = calculate_average_post_length(test_data)
        expected_avg = (2 + 5 + 10) / 3
        
        assert abs(avg_length - expected_avg) < 0.01
    
    def test_single_post_returns_its_length(self):
        """Test that a single post returns its exact length as average."""
        single_post_df = pd.DataFrame({
            'id': [1],
            'text': ['This is a test post with exactly 42 chars!']
        })
        avg_length = calculate_average_post_length(single_post_df)
        assert avg_length == 42.0  # Length of the text above
    
    def test_raises_error_on_empty_dataframe(self):
        """Test that ValueError is raised for empty DataFrame."""
        empty_df = pd.DataFrame(columns=['id', 'text'])
        
        with pytest.raises(ValueError, match="DataFrame cannot be empty"):
            calculate_average_post_length(empty_df)
    
    def test_raises_error_on_missing_text_column(self):
        """Test that KeyError is raised when 'text' column is missing."""
        df_without_text = pd.DataFrame({
            'id': [1, 2, 3],
            'content': ['post1', 'post2', 'post3']  # Wrong column name
        })
        
        with pytest.raises(KeyError, match="DataFrame must contain a 'text' column"):
            calculate_average_post_length(df_without_text)
    
    def test_handles_unicode_characters(self):
        """Test that function correctly handles Unicode characters."""
        unicode_posts = pd.DataFrame({
            'id': [1, 2],
            'text': ['Hello 🌍', '测试']  # Unicode emoji and Chinese characters
        })
        avg_length = calculate_average_post_length(unicode_posts)
        # 'Hello 🌍' has 7 characters, '测试' has 2 characters
        expected_avg = (7 + 2) / 2
        assert avg_length == expected_avg
    
    def test_handles_very_long_posts(self):
        """Test that function handles very long posts correctly."""
        long_text = 'A' * 10000  # 10,000 character post
        short_text = 'B'
        
        long_posts_df = pd.DataFrame({
            'id': [1, 2],
            'text': [long_text, short_text]
        })
        
        avg_length = calculate_average_post_length(long_posts_df)
        expected_avg = (10000 + 1) / 2
        assert avg_length == expected_avg


class TestIntegration:
    """Integration tests combining multiple functions."""
    
    def test_end_to_end_workflow(self):
        """Test the complete workflow from creating posts to calculating average."""
        # Create mock posts
        posts_df = create_mock_posts()
        
        # Verify the DataFrame structure
        assert len(posts_df) == 10
        assert 'text' in posts_df.columns
        
        # Calculate average length
        avg_length = calculate_average_post_length(posts_df)
        
        # Verify the result is reasonable
        assert isinstance(avg_length, float)
        assert avg_length > 0
        assert avg_length < 1000  # Reasonable upper bound for social media posts
    
    def test_mock_posts_content_quality(self):
        """Test that mock posts represent realistic social media content."""
        posts_df = create_mock_posts()
        
        # Check that posts have reasonable lengths (not too short or too long)
        lengths = posts_df['text'].str.len()
        assert lengths.min() >= 5  # Minimum reasonable post length
        assert lengths.max() <= 200  # Maximum reasonable post length for most posts
        
        # Check that we have some variety in post lengths
        assert lengths.std() > 0  # Standard deviation should be > 0 for variety