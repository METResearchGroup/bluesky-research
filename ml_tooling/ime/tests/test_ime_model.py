"""Tests for model.py.

This test suite verifies the functionality of the IME model deployment code. The tests cover:

- Label creation from DataFrame output
- Batch classification processing
- Metric collection and reporting
- Error handling and failed label processing
"""

import sys
from unittest.mock import Mock, MagicMock, patch

import pandas as pd
import pytest
import numpy as np

from services.ml_inference.models import PostToLabelModel  # noqa: E402

# Mock comet_ml and other dependencies BEFORE any imports
mock_comet = MagicMock()

sys.modules['comet_ml'] = mock_comet
sys.modules['lib.telemetry.cometml'] = MagicMock()
# sys.modules['lib.helper'] = MagicMock()
# sys.modules['lib.log.logger'] = MagicMock()

from ml_tooling.ime.model import (  # noqa: E402
    create_labels,
    batch_classify_posts,
    run_batch_classification,
)

from ml_tooling.ime.constants import default_hyperparameters  # noqa: E402


def _make_post_model(
    *,
    uri: str,
    text: str,
    preprocessing_timestamp: str = "2024-01-01",
    batch_id: int = 1,
    batch_metadata: str = "{}",
) -> PostToLabelModel:
    """Create a PostToLabelModel for IME unit tests."""
    return PostToLabelModel(
        uri=uri,
        text=text,
        preprocessing_timestamp=preprocessing_timestamp,
        batch_id=batch_id,
        batch_metadata=batch_metadata,
    )

@pytest.fixture(autouse=True)
def mock_log_batch_classification():
    """Mock the log_batch_classification_to_cometml decorator."""
    def mock_decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    
    with patch(
        'ml_tooling.ime.model.log_batch_classification_to_cometml',
        return_value=mock_decorator
    ) as mock:
        yield mock


def test_model_module_does_not_import_inference_on_import():
    """Ensure importing ml_tooling.ime.model doesn't import heavy ML deps."""
    import subprocess
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import ml_tooling.ime.model; "
            "assert 'ml_tooling.ime.inference' not in sys.modules, "
            "'inference was imported eagerly'",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Lazy import test failed: {result.stderr or result.stdout}"
    )


class TestCreateLabels:
    """Tests for create_labels function."""

    def test_empty_output_df(self):
        """Test handling of empty output DataFrame."""
        posts = [
            {'uri': 'uri1', 'text': 'text1', 'preprocessing_timestamp': '2024-01-01'},
            {'uri': 'uri2', 'text': 'text2', 'preprocessing_timestamp': '2024-01-01'}
        ]
        empty_df = pd.DataFrame()

        result = create_labels(posts=posts, output_df=empty_df)

        assert len(result) == 2
        assert all(not label['was_successfully_labeled'] for label in result)
        assert all('uri' in label for label in result)
        assert all('text' in label for label in result)
        assert all('preprocessing_timestamp' in label for label in result)

    def test_successful_label_creation(self):
        """Test converting DataFrame to label dictionaries."""
        posts = [
            {'uri': 'uri1', 'text': 'text1', 'preprocessing_timestamp': '2024-01-01'},
            {'uri': 'uri2', 'text': 'text2', 'preprocessing_timestamp': '2024-01-01'}
        ]
        
        input_df = pd.DataFrame({
            'uri': ['uri1', 'uri2'],
            'text': ['text1', 'text2'],
            'preprocessing_timestamp': ['2024-01-01', '2024-01-01'],
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
        assert all('preprocessing_timestamp' in label for label in result)
        assert all('label_timestamp' in label for label in result)


class TestBatchClassifyPosts:
    """Tests for batch_classify_posts function."""

    @pytest.fixture
    def mock_dependencies(self):
        """Set up common mock dependencies."""
        with patch('ml_tooling.ime.model.create_batches') as mock_create_batches, \
             patch('ml_tooling.ime.model._process_ime_batch') as mock_process_batch, \
             patch('ml_tooling.ime.model._get_model_and_tokenizer') as mock_get_model_and_tokenizer, \
             patch('ml_tooling.ime.model.write_posts_to_cache') as mock_write_cache, \
             patch('ml_tooling.ime.model.return_failed_labels_to_input_queue') as mock_return_failed:
            mock_get_model_and_tokenizer.return_value = (Mock(), Mock())
            yield {
                'create_batches': mock_create_batches,
                'process_batch': mock_process_batch,
                'get_model_and_tokenizer': mock_get_model_and_tokenizer,
                'write_cache': mock_write_cache,
                'return_failed': mock_return_failed
            }

    def test_empty_input_posts(self, mock_dependencies):
        """Test handling of empty input posts list."""
        mock_dependencies['create_batches'].return_value = []
        
        result = batch_classify_posts(posts=[], batch_size=2, minibatch_size=1)
        
        mock_dependencies['create_batches'].assert_called_once_with(
            batch_list=[],
            batch_size=2
        )
        mock_dependencies['process_batch'].assert_not_called()
        mock_dependencies['get_model_and_tokenizer'].assert_not_called()
        
        assert result['metadata']['total_posts_successfully_labeled'] == 0
        assert result['metadata']['total_posts_failed_to_label'] == 0
        assert result['metadata']['total_batches'] == 0
        
        # For empty input, all metrics should be 0
        metrics = result['experiment_metrics']
        expected_metrics = {
            'average_text_length_per_batch': 0,
            'average_prob_emotion_per_batch': 0,
            'average_prob_intergroup_per_batch': 0,
            'average_prob_moral_per_batch': 0,
            'average_prob_other_per_batch': 0,
            'prop_emotion_per_batch': 0,
            'prop_intergroup_per_batch': 0,
            'prop_moral_per_batch': 0,
            'prop_other_per_batch': 0,
            'prop_multi_label_samples_per_batch': 0
        }
        for metric_name, expected_value in expected_metrics.items():
            assert metrics[metric_name] == expected_value, f"Expected {metric_name} to be 0, got {metrics[metric_name]}"

    def test_successful_batch_processing(self, mock_dependencies):
        """Test processing batches with successful labels."""
        input_posts = [
            _make_post_model(uri="uri1", text="text1", batch_id=1),
            _make_post_model(uri="uri2", text="text2", batch_id=1),
        ]
        input_post_dicts = [p.model_dump() for p in input_posts]
        
        mock_dependencies['create_batches'].return_value = [input_post_dicts]
        mock_dependencies['process_batch'].return_value = pd.DataFrame({
            'uri': ['uri1', 'uri2'],
            'text': ['text1', 'text2'],
            'preprocessing_timestamp': ['2024-01-01', '2024-01-01'],
            'prob_emotion': [0.8, 0.2],
            'prob_intergroup': [0.2, 0.9],
            'prob_moral': [0.1, 0.1],
            'prob_other': [0.1, 0.1],
            'label_emotion': [1, 0],
            'label_intergroup': [0, 1],
            'label_moral': [0, 0],
            'label_other': [0, 0],
            'label_timestamp': ['2024-01-01-12:00:00', '2024-01-01-12:00:00']
        })

        result = batch_classify_posts(
            posts=input_posts,
            batch_size=2,
            minibatch_size=1
        )

        assert result['metadata']['total_posts_successfully_labeled'] == 2
        assert result['metadata']['total_posts_failed_to_label'] == 0
        mock_dependencies['write_cache'].assert_called_once()
        mock_dependencies['return_failed'].assert_not_called()

    def test_failed_batch_processing(self, mock_dependencies):
        """Test handling of completely failed batch."""
        input_posts = [
            _make_post_model(uri="uri1", text="text1", batch_id=1),
            _make_post_model(uri="uri2", text="text2", batch_id=1),
        ]
        input_post_dicts = [p.model_dump() for p in input_posts]
        
        mock_dependencies['create_batches'].return_value = [input_post_dicts]
        mock_dependencies['process_batch'].return_value = pd.DataFrame()  # Empty DataFrame to simulate failure

        result = batch_classify_posts(
            posts=input_posts,
            batch_size=2,
            minibatch_size=1
        )

        assert result['metadata']['total_posts_successfully_labeled'] == 0
        assert result['metadata']['total_posts_failed_to_label'] == 2
        mock_dependencies['write_cache'].assert_not_called()
        mock_dependencies['return_failed'].assert_called_once()

    def test_partial_success_batch_processing(self, mock_dependencies):
        """Test handling of partially successful batch."""
        input_posts = [
            _make_post_model(uri="uri1", text="text1", batch_id=1),
            _make_post_model(uri="uri2", text="text2", batch_id=1),
        ]
        input_post_dicts = [p.model_dump() for p in input_posts]
        
        mock_dependencies['create_batches'].return_value = [input_post_dicts]
        mock_dependencies['process_batch'].return_value = pd.DataFrame({
            'uri': ['uri1'],  # Only first post processed
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
            'label_timestamp': ['2024-01-01-12:00:00']
        })

        result = batch_classify_posts(
            posts=input_posts,
            batch_size=2,
            minibatch_size=1
        )

        assert result['metadata']['total_posts_successfully_labeled'] == 1
        assert result['metadata']['total_posts_failed_to_label'] == 1
        mock_dependencies['write_cache'].assert_called_once()
        mock_dependencies['return_failed'].assert_called_once()

    def test_metric_collection(self, mock_dependencies):
        """Test that metrics are correctly calculated for a batch."""
        input_posts = [
            _make_post_model(uri="uri1", text="short", batch_id=1),
            _make_post_model(uri="uri2", text="very long text", batch_id=1),
        ]
        input_post_dicts = [p.model_dump() for p in input_posts]
        
        mock_dependencies['create_batches'].return_value = [input_post_dicts]
        
        # Create DataFrame with known values to verify metrics
        mock_dependencies['process_batch'].return_value = pd.DataFrame({
            'uri': ['uri1', 'uri2'],
            'text': ['short', 'very long text'],
            'preprocessing_timestamp': ['2024-01-01', '2024-01-01'],
            'prob_emotion': [0.8, 0.2],
            'prob_intergroup': [0.7, 0.3],
            'prob_moral': [0.6, 0.4],
            'prob_other': [0.5, 0.5],
            'label_emotion': [1, 0],
            'label_intergroup': [1, 0],
            'label_moral': [1, 0],
            'label_other': [0, 0],
            'label_timestamp': ['2024-01-01-12:00:00', '2024-01-01-12:00:00']
        })

        result = batch_classify_posts(
            posts=input_posts,
            batch_size=2,
            minibatch_size=1
        )
        
        metrics = result['experiment_metrics']
        # Verify average probabilities (now scalar values)
        assert metrics['average_prob_emotion_per_batch'] == 0.5
        assert metrics['prop_emotion_per_batch'] == 50.0
        # Verify multi-label detection
        assert metrics['prop_multi_label_samples_per_batch'] == 0.5
