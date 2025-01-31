"""Tests for inference.py.

This test suite verifies the functionality of the IME classification model inference
code. The tests cover:

- Batch and minibatch processing
- Default class handling
- Input/output validation
- Error handling
"""

import numpy as np
import pandas as pd
import pytest
import torch
from unittest.mock import Mock, patch

from ml_tooling.ime.inference import (
    default_to_other,
    process_ime_minibatch,
    process_ime_batch
)


class TestDefaultToOther:
    """Tests for default_to_other function."""

    def test_zero_row_conversion(self):
        """Test converting zero rows to 'other' class.
        
        Expected input:
        - Array with some zero rows
        
        Expected behavior:
        - Zero rows should have last column set to 1
        - Non-zero rows should remain unchanged
        """
        input_array = np.array([
            [0, 0, 0, 0],  # Should be converted
            [1, 0, 0, 0],  # Should stay same
            [0, 0, 0, 0],  # Should be converted
            [0, 1, 1, 0]   # Should stay same
        ])
        
        expected = np.array([
            [0, 0, 0, 1],
            [1, 0, 0, 0],
            [0, 0, 0, 1],
            [0, 1, 1, 0]
        ])
        
        result = default_to_other(input_array)
        np.testing.assert_array_equal(result, expected)

    def test_no_zero_rows(self):
        """Test array with no zero rows.
        
        Expected input:
        - Array with no zero rows
        
        Expected behavior:
        - Array should remain unchanged
        """
        input_array = np.array([
            [1, 0, 0, 0],
            [0, 1, 0, 0],
            [0, 0, 1, 0]
        ])
        result = default_to_other(input_array)
        np.testing.assert_array_equal(result, input_array)


@pytest.fixture
def mock_model():
    """Mock PyTorch model."""
    model = Mock()
    model.eval = Mock()
    return model


@pytest.fixture
def mock_tokenizer():
    """Mock tokenizer."""
    return Mock()


class TestProcessIMEMinibatch:
    """Tests for process_ime_minibatch function."""
    
    @patch('ml_tooling.ime.inference.TextDataset')
    def test_minibatch_processing(self, mock_dataset_class, mock_model, mock_tokenizer):
        """Test processing a minibatch of data.
        
        Expected input:
        - DataFrame with text and URIs
        - Batch size
        - Model and tokenizer instances
        
        Expected behavior:
        - Should tokenize inputs
        - Should run model inference
        - Should process outputs into probabilities and labels
        - Should return DataFrame with predictions
        """
        # Mock data
        input_df = pd.DataFrame({
            'uri': ['uri1', 'uri2'],
            'text': ['text1', 'text2']
        })
        
        # Mock model outputs
        mock_outputs = torch.tensor([
            [0.8, 0.2, 0.1, 0.1],
            [0.1, 0.9, 0.1, 0.1]
        ])
        mock_model.return_value = mock_outputs
        
        # Mock dataset
        mock_batch = {
            'input_ids': torch.zeros(2, 512),
            'attention_mask': torch.ones(2, 512)
        }
        mock_dataset = Mock()
        mock_dataset.__iter__ = Mock(return_value=iter([mock_batch]))
        mock_dataset_class.return_value = mock_dataset
            
        result = process_ime_minibatch(
            minibatch_df=input_df,
            minibatch_size=2,
            model=mock_model,
            tokenizer=mock_tokenizer
        )
            
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert all(col in result.columns for col in [
            'uri', 'text', 
            'prob_emotion', 'prob_intergroup', 'prob_moral', 'prob_other',
            'label_emotion', 'label_intergroup', 'label_moral', 'label_other',
            'label_timestamp'
        ])

class TestProcessIMEBatch:
    """Tests for process_ime_batch function."""

    @patch('ml_tooling.ime.inference.process_ime_minibatch')
    def test_successful_batch_processing(self, mock_process_minibatch, mock_model, mock_tokenizer):
        """Test successful processing of a full batch of data.
        
        Expected input:
        - List of dictionaries with text and URIs
        - Minibatch size
        - Model and tokenizer instances
        
        Expected behavior:
        - Should split into minibatches
        - Should process each minibatch
        - Should combine results
        - Should clean up memory
        """
        input_batch = [
            {'uri': 'uri1', 'text': 'text1'},
            {'uri': 'uri2', 'text': 'text2'},
            {'uri': 'uri3', 'text': 'text3'}
        ]
        
        mock_output = pd.DataFrame({
            'uri': ['uri1'],
            'text': ['text1'],
            'prob_emotion': [0.8],
            'prob_intergroup': [0.2],
            'prob_moral': [0.1],
            'prob_other': [0.1],
            'label_emotion': [1],
            'label_intergroup': [0],
            'label_moral': [0],
            'label_other': [0],
            'label_timestamp': ['2024-01-01']
        })
        mock_process_minibatch.return_value = mock_output
        
        with patch('ml_tooling.ime.inference.gc') as mock_gc:
            result = process_ime_batch(
                batch=input_batch,
                minibatch_size=2,
                model=mock_model,
                tokenizer=mock_tokenizer
            )
            
            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert mock_process_minibatch.call_count == 2
            mock_gc.collect.assert_called_once()

    @patch('ml_tooling.ime.inference.process_ime_minibatch')
    def test_failed_batch_processing(self, mock_process_minibatch, mock_model, mock_tokenizer):
        """Test error handling during batch processing.
        
        Expected behavior:
        - Should return empty DataFrame on error
        - Should log error
        """
        input_batch = [
            {'uri': 'uri1', 'text': 'text1'},
            {'uri': 'uri2', 'text': 'text2'}
        ]
        
        mock_process_minibatch.side_effect = Exception("Test error")
        
        with patch('ml_tooling.ime.inference.gc') as mock_gc:
            result = process_ime_batch(
                batch=input_batch,
                minibatch_size=2,
                model=mock_model,
                tokenizer=mock_tokenizer
            )
            
            assert isinstance(result, pd.DataFrame)
            assert result.empty
            mock_gc.collect.assert_called_once()
