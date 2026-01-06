"""Tests for inference.py.

This test suite verifies the functionality of the IME classification model inference
code. The tests cover:

- Batch and minibatch processing
- Default class handling
- Input/output validation
- Error handling
"""

import gc
import numpy as np
import pandas as pd
import pytest
from unittest.mock import Mock, patch, MagicMock, call

torch = pytest.importorskip("torch")

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
        
        result = default_to_other(preds=input_array)
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
        result = default_to_other(preds=input_array)
        np.testing.assert_array_equal(result, input_array)

    def test_single_row(self):
        """Test handling of single row input.
        
        Expected input:
        - Single row array
        
        Expected behavior:
        - Should handle single row correctly
        """
        input_array = np.array([[0, 0, 0, 0]])
        expected = np.array([[0, 0, 0, 1]])
        result = default_to_other(preds=input_array)
        np.testing.assert_array_equal(result, expected)


@pytest.fixture
def mock_model():
    """Mock PyTorch model."""
    model = Mock()
    model.eval = Mock(return_value=None)
    model.to = Mock(return_value=model)
    return model


@pytest.fixture
def mock_tokenizer():
    """Mock tokenizer."""
    tokenizer = Mock()
    tokenizer.pad_token_id = 0
    tokenizer.return_value = {
        'input_ids': torch.zeros(2, 512),
        'attention_mask': torch.ones(2, 512)
    }
    return tokenizer


@pytest.fixture(autouse=True)
def mock_gc():
    """Mock garbage collection."""
    with patch('ml_tooling.ime.inference.gc.collect') as mock:
        yield mock


class TestProcessIMEMinibatch:
    """Tests for process_ime_minibatch function."""
    
    @patch('ml_tooling.ime.inference.TextDataset')
    @patch('ml_tooling.ime.inference.device')
    def test_minibatch_processing(self, mock_device, mock_dataset_class, mock_model, mock_tokenizer):
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
            'text': ['text1', 'text2'],
            'preprocessing_timestamp': ['2024-01-01', '2024-01-01']
        })
        
        # Mock device
        mock_device.return_value = 'cpu'
        
        # Mock tensors
        mock_input_ids = Mock(spec=torch.Tensor)
        mock_input_ids.to = Mock(return_value=mock_input_ids)
        
        mock_attention_mask = Mock(spec=torch.Tensor)
        mock_attention_mask.to = Mock(return_value=mock_attention_mask)
        
        # Mock model outputs
        mock_outputs = Mock()
        mock_outputs.cpu = Mock(return_value=Mock())
        mock_outputs.cpu.return_value.numpy = Mock(return_value=np.array([
            [0.8, 0.2, 0.1, 0.1],  # High probability for emotion
            [0.1, 0.9, 0.1, 0.1]   # High probability for intergroup
        ]))
        mock_model.return_value = mock_outputs
        
        # Mock dataset
        mock_batch = {
            'input_ids': mock_input_ids,
            'attention_mask': mock_attention_mask
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
        
        # Verify model was called correctly
        mock_model.eval.assert_called_once()
        mock_model.assert_has_calls([call(input_ids=mock_input_ids, attention_mask=mock_attention_mask)])
        
        # Verify tensor operations
        mock_input_ids.to.assert_called_once_with(mock_device)
        mock_attention_mask.to.assert_called_once_with(mock_device)
        
        # Verify probabilities are valid
        assert all(0 <= prob <= 1 for prob in result['prob_emotion'])
        assert all(0 <= prob <= 1 for prob in result['prob_intergroup'])
        assert all(0 <= prob <= 1 for prob in result['prob_moral'])
        assert all(0 <= prob <= 1 for prob in result['prob_other'])


class TestProcessIMEBatch:
    """Tests for process_ime_batch function."""

    @patch('ml_tooling.ime.inference.process_ime_minibatch')
    def test_successful_batch_processing(self, mock_process_minibatch, mock_model, mock_tokenizer, mock_gc):
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
            {'uri': 'uri1', 'text': 'text1', 'preprocessing_timestamp': '2024-01-01'},
            {'uri': 'uri2', 'text': 'text2', 'preprocessing_timestamp': '2024-01-01'},
            {'uri': 'uri3', 'text': 'text3', 'preprocessing_timestamp': '2024-01-01'}
        ]
        
        mock_output = pd.DataFrame({
            'uri': ['uri1'],
            'text': ['text1'],
            'preprocessing_timestamp': ['2024-01-01'],
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
        
        result = process_ime_batch(
            batch=input_batch,
            minibatch_size=2,
            model=mock_model,
            tokenizer=mock_tokenizer
        )
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert mock_process_minibatch.call_count == 2
        mock_gc.assert_called_once()

    @patch('ml_tooling.ime.inference.process_ime_minibatch')
    def test_failed_batch_processing(self, mock_process_minibatch, mock_model, mock_tokenizer, mock_gc):
        """Test error handling during batch processing.
        
        Expected behavior:
        - Should return empty DataFrame on error
        - Should log error
        - Should clean up memory
        """
        input_batch = [
            {'uri': 'uri1', 'text': 'text1', 'preprocessing_timestamp': '2024-01-01'},
            {'uri': 'uri2', 'text': 'text2', 'preprocessing_timestamp': '2024-01-01'}
        ]
        
        mock_process_minibatch.side_effect = Exception("Test error")
        
        result = process_ime_batch(
            batch=input_batch,
            minibatch_size=2,
            model=mock_model,
            tokenizer=mock_tokenizer
        )
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        mock_gc.assert_called_once()

    @patch('ml_tooling.ime.inference.process_ime_minibatch')
    def test_empty_batch_processing(self, mock_process_minibatch, mock_model, mock_tokenizer, mock_gc):
        """Test handling of empty batch.
        
        Expected behavior:
        - Should return empty DataFrame
        - Should not process any minibatches
        - Should clean up memory
        """
        input_batch = []
        
        result = process_ime_batch(
            batch=input_batch,
            minibatch_size=2,
            model=mock_model,
            tokenizer=mock_tokenizer
        )
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        mock_process_minibatch.assert_not_called()
        mock_gc.assert_called_once()
