"""
Tests for the TopicModelingPipeline integration.

This module tests the pipeline that integrates data loading with topic modeling,
including the new fit method for BERTopic integration.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

import pytest
import pandas as pd
from unittest.mock import patch, Mock

from src.data_loading.base import DataLoader
from src.data_loading.config import DataLoaderConfig
from src.pipeline.topic_modeling import TopicModelingPipeline


class MockDataLoader(DataLoader):
    """Mock data loader for testing the pipeline."""
    
    def __init__(self):
        super().__init__(
            name="Mock Loader",
            description="Mock loader for pipeline testing"
        )
    
    def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return mock data for testing."""
        return pd.DataFrame({
            'text': [
                'Test post about machine learning',
                'Another test post about data science',
                'Third test post about AI'
            ],
            'created_at': [
                '2024-10-01T10:00:00Z',
                '2024-10-02T11:00:00Z',
                '2024-10-03T12:00:00Z'
            ]
        })


class TestTopicModelingPipeline:
    """Test the TopicModelingPipeline class."""
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        assert pipeline.data_loader == loader
        assert pipeline.config is not None
        assert pipeline.loaded_data is None
        assert pipeline.topic_model is None
        assert pipeline.fitted is False
    
    def test_pipeline_initialization_with_config(self):
        """Test pipeline initialization with custom config."""
        loader = MockDataLoader()
        config = DataLoaderConfig()
        pipeline = TopicModelingPipeline(loader, config)
        
        assert pipeline.config == config
    
    def test_load_data_success(self):
        """Test successful data loading."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        df = pipeline.load_data("2024-10-01", "2024-10-03")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'text' in df.columns
        assert 'created_at' in df.columns
        assert pipeline.loaded_data is not None
    
    def test_load_data_no_data_loaded(self):
        """Test data loading when no data is returned."""
        # Create a loader that returns empty data
        class EmptyDataLoader(MockDataLoader):
            def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
                return pd.DataFrame()
        
        loader = EmptyDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        with pytest.raises(ValueError, match="No data loaded from data loader"):
            pipeline.load_data("2024-10-01", "2024-10-03")
    
    def test_load_data_missing_text_column(self):
        """Test data loading when text column is missing."""
        # Create a loader that returns data without text column
        class NoTextDataLoader(MockDataLoader):
            def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
                return pd.DataFrame({
                    'other_column': ['value1', 'value2'],
                    'created_at': ['2024-10-01T10:00:00Z', '2024-10-02T11:00:00Z']
                })
        
        loader = NoTextDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        with pytest.raises(ValueError, match="Loaded data must contain 'text' column"):
            pipeline.load_data("2024-10-01", "2024-10-03")
    
    def test_get_data_info_no_data(self):
        """Test getting data info when no data is loaded."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        info = pipeline.get_data_info()
        assert info["status"] == "no_data_loaded"
    
    def test_get_data_info_with_data(self):
        """Test getting data info when data is loaded."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        # Load data first
        pipeline.load_data("2024-10-01", "2024-10-03")
        
        info = pipeline.get_data_info()
        assert info["status"] == "data_loaded"
        assert info["row_count"] == 3
        assert info["text_column_exists"] is True
        assert len(info["sample_texts"]) == 3
    
    def test_prepare_for_bertopic_no_data(self):
        """Test data preparation when no data is loaded."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        with pytest.raises(ValueError, match="No data loaded. Call load_data\\(\\) first"):
            pipeline.prepare_for_bertopic()
    
    def test_prepare_for_bertopic_missing_text_column(self):
        """Test data preparation when text column is missing."""
        # Create a loader that returns data without text column
        class NoTextDataLoader(MockDataLoader):
            def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
                return pd.DataFrame({
                    'other_column': ['value1', 'value2'],
                    'created_at': ['2024-10-01T10:00:00Z', '2024-10-02T11:00:00Z']
                })
        
        loader = NoTextDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        # Load data first
        pipeline.load_data("2024-10-01", "2024-10-03")
        
        with pytest.raises(ValueError, match="Loaded data must contain 'text' column for BERTopic processing"):
            pipeline.prepare_for_bertopic()
    
    def test_prepare_for_bertopic_success(self):
        """Test successful data preparation for BERTopic."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        # Load data first
        pipeline.load_data("2024-10-01", "2024-10-03")
        
        # Prepare data
        prepared_data = pipeline.prepare_for_bertopic()
        
        assert isinstance(prepared_data, pd.DataFrame)
        assert len(prepared_data) == 3
        assert 'text' in prepared_data.columns
        assert 'created_at' in prepared_data.columns
    
    @patch('src.pipeline.topic_modeling.BERTopicWrapper')
    def test_fit_method_success(self, mock_bertopic_wrapper):
        """Test successful fitting of BERTopic model."""
        # Mock the BERTopic wrapper
        mock_wrapper = Mock()
        mock_bertopic_wrapper.return_value = mock_wrapper
        
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        # Test the fit method
        results = pipeline.fit("2024-10-01", "2024-10-03")
        
        # Verify results
        assert results["status"] == "success"
        assert results["data_loaded"] == 3
        assert results["data_prepared"] == 3
        assert results["start_date"] == "2024-10-01"
        assert results["end_date"] == "2024-10-03"
        assert results["model_fitted"] is True
        assert results["bertopic_wrapper"] == mock_wrapper
        
        # Verify pipeline state
        assert pipeline.fitted is True
        assert pipeline.topic_model == mock_wrapper
    
    def test_fit_method_no_data_remaining_after_preparation(self):
        """Test fit method when no data remains after preparation."""
        # Create a loader that returns data that will be filtered out
        class ShortTextDataLoader(MockDataLoader):
            def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
                return pd.DataFrame({
                    'text': ['Short'],  # Too short for default validation
                    'created_at': ['2024-10-01T10:00:00Z']
                })
        
        loader = ShortTextDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        with pytest.raises(ValueError, match="No valid data remaining after preparation for BERTopic"):
            pipeline.fit("2024-10-01", "2024-10-01")
    
    def test_fit_method_with_custom_bertopic_wrapper(self):
        """Test fit method with custom BERTopic wrapper."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        # Create custom wrapper
        custom_wrapper = Mock()
        
        # Test the fit method
        results = pipeline.fit("2024-10-01", "2024-10-03", bertopic_wrapper=custom_wrapper)
        
        # Verify results
        assert results["bertopic_wrapper"] == custom_wrapper
        assert pipeline.topic_model == custom_wrapper
    
    def test_get_pipeline_info(self):
        """Test getting comprehensive pipeline information."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        info = pipeline.get_pipeline_info()
        
        # Check structure
        assert "data_loader" in info
        assert "config" in info
        assert "data_status" in info
        assert "model_status" in info
        
        # Check model status
        model_status = info["model_status"]
        assert "fitted" in model_status
        assert "topic_model_available" in model_status
        assert model_status["fitted"] is False
        assert model_status["topic_model_available"] is False
    
    def test_get_pipeline_info_after_fitting(self):
        """Test getting pipeline info after fitting the model."""
        with patch('src.pipeline.topic_modeling.BERTopicWrapper') as mock_bertopic_wrapper:
            mock_wrapper = Mock()
            mock_bertopic_wrapper.return_value = mock_wrapper
            
            loader = MockDataLoader()
            pipeline = TopicModelingPipeline(loader)
            
            # Fit the model
            pipeline.fit("2024-10-01", "2024-10-03")
            
            # Get info
            info = pipeline.get_pipeline_info()
            
            # Check model status
            model_status = info["model_status"]
            assert model_status["fitted"] is True
            assert model_status["topic_model_available"] is True
    
    def test_string_representations(self):
        """Test string representation methods."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        str_repr = str(pipeline)
        repr_repr = repr(pipeline)
        
        assert "TopicModelingPipeline" in str_repr
        assert "Mock Loader" in str_repr
        assert "data_loaded=False" in str_repr
        assert "fitted=False" in str_repr
        
        assert "TopicModelingPipeline" in repr_repr
        assert "Mock Loader" in repr_repr
    
    def test_string_representations_with_data(self):
        """Test string representations when data is loaded."""
        loader = MockDataLoader()
        pipeline = TopicModelingPipeline(loader)
        
        # Load data
        pipeline.load_data("2024-10-01", "2024-10-03")
        
        str_repr = str(pipeline)
        assert "data_loaded=True" in str_repr
