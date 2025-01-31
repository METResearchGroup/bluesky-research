"""Tests for model.py.

This test suite verifies the functionality of the IME model deployment code. The tests cover:

- Label creation from DataFrame output
- Batch classification processing
- Metric collection and reporting
- Error handling and failed label processing
"""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import Mock, patch
import sys
from unittest.mock import MagicMock

# Mock comet_ml and other dependencies BEFORE any imports
mock_comet = MagicMock()

sys.modules['comet_ml'] = mock_comet
sys.modules['lib.telemetry.cometml'] = MagicMock()
# sys.modules['lib.helper'] = MagicMock()
# sys.modules['lib.log.logger'] = MagicMock()

# Now import the model module
from ml_tooling.ime.model import (
    create_labels,
    batch_classify_posts,
)

@pytest.fixture(autouse=True)
def mock_log_batch_classification():
    """Mock the log_batch_classification_to_cometml decorator."""
    def mock_decorator(func):
        def wrapper(*args, **kwargs):
            # Execute the original function and return its result
            return func(*args, **kwargs)
        return wrapper
    
    with patch(
        'ml_tooling.ime.model.log_batch_classification_to_cometml',
        return_value=mock_decorator
    ) as mock:
        yield mock


class TestCreateLabels:
    """Tests for create_labels function."""

    def test_empty_output_df(self):
        """Test handling of empty output DataFrame."""
        posts = [
            {'uri': 'uri1', 'text': 'text1'},
            {'uri': 'uri2', 'text': 'text2'}
        ]
        empty_df = pd.DataFrame()
        
        result = create_labels(posts=posts, output_df=empty_df)
        
        assert len(result) == 2
        assert all(not label['was_successfully_labeled'] for label in result)
        assert all('uri' in label for label in result)
        assert all('text' in label for label in result)

    def test_successful_label_creation(self):
        """Test converting DataFrame to label dictionaries."""
        posts = [
            {'uri': 'uri1', 'text': 'text1'},
            {'uri': 'uri2', 'text': 'text2'}
        ]
        
        input_df = pd.DataFrame({
            'uri': ['uri1', 'uri2'],
            'text': ['text1', 'text2'],
            'prob_emotion': [0.8, 0.1],
            'prob_intergroup': [0.2, 0.9],
            'prob_moral': [0.1, 0.1],
            'prob_other': [0.1, 0.1],
            'label_emotion': [1, 0],
            'label_intergroup': [0, 1],
            'label_moral': [0, 0],
            'label_other': [0, 0],
            'label_timestamp': ['2024-01-01', '2024-01-01']
        })
        
        result = create_labels(posts=posts, output_df=input_df)
        
        assert len(result) == 2
        assert all(label['was_successfully_labeled'] for label in result)
        assert all('uri' in label for label in result)
        assert all('text' in label for label in result)
        assert all('label_timestamp' in label for label in result)


class TestBatchClassifyPosts:
    """Tests for batch_classify_posts function."""

    @patch('ml_tooling.ime.model.create_batches')
    @patch('ml_tooling.ime.model.process_ime_batch')
    @patch('ml_tooling.ime.model.write_posts_to_cache')
    @patch('ml_tooling.ime.model.return_failed_labels_to_input_queue')
    def test_successful_batch_processing(
        self,
        mock_return_failed,
        mock_write_cache, 
        mock_process_batch,
        mock_create_batches
    ):
        """Test processing batches with successful labels."""
        input_posts = [
            {'uri': 'uri1', 'text': 'text1', 'batch_id': 'batch1'},
            {'uri': 'uri2', 'text': 'text2', 'batch_id': 'batch1'}
        ]
        
        mock_create_batches.return_value = [input_posts]
        
        mock_process_batch.return_value = pd.DataFrame({
            'uri': ['uri1', 'uri2'],
            'text': ['text1', 'text2'],
            'prob_emotion': [0.8, 0.1],
            'prob_intergroup': [0.2, 0.9],
            'prob_moral': [0.1, 0.1], 
            'prob_other': [0.1, 0.1],
            'label_emotion': [1, 0],
            'label_intergroup': [0, 1],
            'label_moral': [0, 0],
            'label_other': [0, 0],
            'label_timestamp': ['2024-01-01', '2024-01-01']
        })

        result = batch_classify_posts(
            posts=input_posts,
            batch_size=2,
            minibatch_size=1
        )
        
        assert isinstance(result, dict)
        assert 'metadata' in result
        assert 'experiment_metrics' in result
        assert 'classification_breakdown' in result
        mock_write_cache.assert_called_once()
        mock_return_failed.assert_not_called()

    @patch('ml_tooling.ime.model.create_batches')
    @patch('ml_tooling.ime.model.process_ime_batch') 
    @patch('ml_tooling.ime.model.write_posts_to_cache')
    @patch('ml_tooling.ime.model.return_failed_labels_to_input_queue')
    def test_failed_label_handling(
        self,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch,
        mock_create_batches
    ):
        """Test handling of failed labels."""
        input_posts = [
            {'uri': 'uri1', 'text': 'text1', 'batch_id': 'batch1'},
            {'uri': 'uri2', 'text': 'text2', 'batch_id': 'batch1'}
        ]
        
        mock_create_batches.return_value = [input_posts]
        
        # Return empty DataFrame to simulate failed processing
        mock_process_batch.return_value = pd.DataFrame()

        result = batch_classify_posts(
            posts=input_posts,
            batch_size=2,
            minibatch_size=1
        )
        
        assert isinstance(result, dict)
        assert result['metadata']['total_posts_failed_to_label'] == 2
        assert result['metadata']['total_posts_successfully_labeled'] == 0
        mock_return_failed.assert_called_once()
        mock_write_cache.assert_not_called()
