"""
Tests for the abstract DataLoader interface.

This module tests the abstract base class and custom exceptions
that define the contract for all data loaders.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

import pytest
import pandas as pd

from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.base import DataLoader, DataLoadingError, ValidationError


class MockDataLoader(DataLoader):
    """Mock implementation of DataLoader for testing."""
    
    def __init__(self):
        super().__init__(
            name="Mock Data Loader",
            description="A mock data loader for testing purposes"
        )
    
    def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Mock implementation that returns sample data."""
        return pd.DataFrame({
            'text': [
                'This is a test post about machine learning',
                'Another test post about data science',
                'Third test post about artificial intelligence'
            ],
            'created_at': [
                '2024-10-01T10:00:00Z',
                '2024-10-02T11:00:00Z',
                '2024-10-03T12:00:00Z'
            ]
        })


class TestDataLoadingError:
    """Test the DataLoadingError exception."""
    
    def test_data_loading_error_creation(self):
        """Test creating a DataLoadingError."""
        error = DataLoadingError("Test error message")
        assert str(error) == "Test error message"
    
    def test_data_loading_error_with_cause(self):
        """Test DataLoadingError with a cause exception."""
        original_error = ValueError("Original error")
        error = DataLoadingError("Wrapper error")
        # Store the original error as an attribute for testing
        error.original_error = original_error
        
        assert str(error) == "Wrapper error"
        assert hasattr(error, 'original_error')
        assert error.original_error == original_error


class TestValidationError:
    """Test the ValidationError exception."""
    
    def test_validation_error_creation(self):
        """Test creating a ValidationError."""
        error = ValidationError("Validation failed")
        assert str(error) == "Validation failed"
    
    def test_validation_error_with_cause(self):
        """Test ValidationError with a cause exception."""
        original_error = ValueError("Invalid value")
        error = ValidationError("Date validation failed")
        # Store the original error as an attribute for testing
        error.original_error = original_error
        
        assert str(error) == "Date validation failed"
        assert hasattr(error, 'original_error')
        assert error.original_error == original_error


class TestDataLoader:
    """Test the abstract DataLoader base class."""
    
    def test_data_loader_initialization(self):
        """Test DataLoader initialization."""
        loader = MockDataLoader()
        
        assert loader.name == "Mock Data Loader"
        assert loader.description == "A mock data loader for testing purposes"
    
    def test_data_loader_info(self):
        """Test getting loader information."""
        loader = MockDataLoader()
        info = loader.get_info()
        
        expected_info = {
            "name": "Mock Data Loader",
            "description": "A mock data loader for testing purposes",
            "type": "MockDataLoader"
        }
        
        assert info == expected_info
    
    def test_data_loader_string_representations(self):
        """Test string representation methods."""
        loader = MockDataLoader()
        
        str_repr = str(loader)
        repr_repr = repr(loader)
        
        assert "MockDataLoader" in str_repr
        assert "Mock Data Loader" in str_repr
        assert "MockDataLoader" in repr_repr
        assert "Mock Data Loader" in repr_repr
    
    def test_date_validation_valid_range(self):
        """Test date validation with valid date range."""
        loader = MockDataLoader()
        
        # Valid date range
        result = loader.validate_date_range("2024-01-01", "2024-01-31")
        assert result is True
    
    def test_date_validation_invalid_range_start_after_end(self):
        """Test date validation with start date after end date."""
        loader = MockDataLoader()
        
        with pytest.raises(ValidationError, match="Start date 2024-01-31 must be before or equal to end date 2024-01-01"):
            loader.validate_date_range("2024-01-31", "2024-01-01")
    
    def test_date_validation_future_dates(self):
        """Test date validation with future dates."""
        loader = MockDataLoader()
        
        # This test might need adjustment based on when it's run
        # For now, we'll test with a date that should be in the future
        future_date = "2030-01-01"
        
        with pytest.raises(ValidationError, match="Dates cannot be in the future"):
            loader.validate_date_range("2024-01-01", future_date)
    
    def test_date_validation_invalid_format(self):
        """Test date validation with invalid date format."""
        loader = MockDataLoader()
        
        with pytest.raises(ValidationError, match="Invalid date format"):
            loader.validate_date_range("invalid-date", "2024-01-31")
    
    def test_load_text_data_implementation(self):
        """Test that the mock loader properly implements load_text_data."""
        loader = MockDataLoader()
        
        df = loader.load_text_data("2024-10-01", "2024-10-03")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'text' in df.columns
        assert 'created_at' in df.columns
        
        # Check that the text content is as expected
        expected_texts = [
            'This is a test post about machine learning',
            'Another test post about data science',
            'Third test post about artificial intelligence'
        ]
        assert df['text'].tolist() == expected_texts
