import pytest
from unittest.mock import patch
from ml_tooling.valence_classifier import model
import pandas as pd

@pytest.fixture
def sample_posts():
    return [
        {"uri": "1", "text": "I love this!", "batch_id": "batch1", "preprocessing_timestamp": "2023-01-01T00:00:00Z"},
        {"uri": "2", "text": "This is okay.", "batch_id": "batch1", "preprocessing_timestamp": "2023-01-01T00:00:00Z"},
        {"uri": "3", "text": "I hate this.", "batch_id": "batch1", "preprocessing_timestamp": "2023-01-01T00:00:00Z"},
    ]

def test_batch_classify_posts_empty():
    # Expected results for empty input
    expected_result = {
        "metadata": {
            "total_batches": 0,
            "total_posts_successfully_labeled": 0,
            "total_posts_failed_to_label": 0,
        },
        "experiment_metrics": {},
    }
    result = model.batch_classify_posts([])
    assert result == expected_result

def test_run_batch_classification_delegates(sample_posts):
    # Expected results to be returned by the mock
    expected_result = {
        "metadata": {"test": "value"},
        "experiment_metrics": {"test_metric": "value"}
    }
    
    with patch("ml_tooling.valence_classifier.model.batch_classify_posts") as mock_batch:
        mock_batch.return_value = expected_result
        out = model.run_batch_classification(sample_posts)
        
        assert out == expected_result
        mock_batch.assert_called_once_with(posts=sample_posts, batch_size=100)

def test_create_labels_success(sample_posts):
    # Test data
    output_df = pd.DataFrame([
        {"uri": "1", "valence_label": "positive", "compound": 0.8},
        {"uri": "2", "valence_label": "neutral", "compound": 0.0},
        {"uri": "3", "valence_label": "negative", "compound": -0.7},
    ])
    
    # Expected results
    expected_labels = [
        {
            "uri": "1",
            "text": "I love this!",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "was_successfully_labeled": True,
            "label_timestamp": "2023-01-01T12:00:00Z",
            "valence_label": "positive",
            "compound": 0.8,
        },
        {
            "uri": "2",
            "text": "This is okay.",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "was_successfully_labeled": True,
            "label_timestamp": "2023-01-01T12:00:00Z",
            "valence_label": "neutral",
            "compound": 0.0,
        },
        {
            "uri": "3",
            "text": "I hate this.",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "was_successfully_labeled": True,
            "label_timestamp": "2023-01-01T12:00:00Z",
            "valence_label": "negative",
            "compound": -0.7,
        },
    ]
    
    with patch("ml_tooling.valence_classifier.model.generate_current_datetime_str") as mock_time:
        mock_time.return_value = "2023-01-01T12:00:00Z"
        labels = model.create_labels(sample_posts, output_df)
    
    # Compare each label with expected result
    assert len(labels) == len(expected_labels)
    for i, (label, expected) in enumerate(zip(labels, expected_labels)):
        for key, value in expected.items():
            assert label[key] == value, f"Mismatch in label {i}, key {key}"

def test_create_labels_partial_success(sample_posts):
    # Test data
    output_df = pd.DataFrame([
        {"uri": "1", "valence_label": "positive", "compound": 0.8},
    ])
    
    # Expected results
    expected_labels = [
        {
            "uri": "1",
            "text": "I love this!",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "was_successfully_labeled": True,
            "label_timestamp": "2023-01-01T12:00:00Z",
            "valence_label": "positive",
            "compound": 0.8,
        },
        {
            "uri": "2",
            "text": "This is okay.",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "was_successfully_labeled": False,
            "label_timestamp": "2023-01-01T12:00:00Z",
            "valence_label": None,
            "compound": None,
        },
        {
            "uri": "3",
            "text": "I hate this.",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z",
            "was_successfully_labeled": False,
            "label_timestamp": "2023-01-01T12:00:00Z",
            "valence_label": None,
            "compound": None,
        },
    ]
    
    with patch("ml_tooling.valence_classifier.model.generate_current_datetime_str") as mock_time:
        mock_time.return_value = "2023-01-01T12:00:00Z"
        labels = model.create_labels(sample_posts, output_df)
    
    # Compare each label with expected result
    assert len(labels) == len(expected_labels)
    for i, (label, expected) in enumerate(zip(labels, expected_labels)):
        for key, value in expected.items():
            assert label[key] == value, f"Mismatch in label {i}, key {key}"

def test_batch_classify_posts_success(sample_posts):
    # Expected results
    expected_result = {
        "metadata": {
            "total_batches": 2,  # With batch_size=2, we should have 2 batches
            "total_posts_successfully_labeled": 3,
            "total_posts_failed_to_label": 0,
        },
        "experiment_metrics": {
            "label_distribution": {
                "positive": 1,
                "neutral": 1,
                "negative": 1,
            }
        }
    }
    
    with patch("ml_tooling.valence_classifier.model.run_vader_on_posts") as mock_vader, \
         patch("ml_tooling.valence_classifier.model.write_posts_to_cache") as mock_write, \
         patch("ml_tooling.valence_classifier.model.return_failed_labels_to_input_queue") as mock_return:
        
        mock_vader.return_value = pd.DataFrame([
            {"uri": "1", "valence_label": "positive", "compound": 0.8},
            {"uri": "2", "valence_label": "neutral", "compound": 0.0},
            {"uri": "3", "valence_label": "negative", "compound": -0.7},
        ])
        
        result = model.batch_classify_posts(sample_posts, batch_size=2)
        
        # Compare result with expected result
        assert result["metadata"] == expected_result["metadata"]
        assert result["experiment_metrics"]["label_distribution"] == expected_result["experiment_metrics"]["label_distribution"]
        
        # Verify write_posts_to_cache was called with the right parameters
        assert mock_write.call_count > 0
        # Verify return_failed_labels_to_input_queue was not called since all labels succeeded
        assert mock_return.call_count == 0

def test_batch_classify_posts_with_failures(sample_posts):
    # Expected results
    expected_result = {
        "metadata": {
            "total_batches": 2,  # With batch_size=2, we should have 2 batches
            "total_posts_successfully_labeled": 1,
            "total_posts_failed_to_label": 2,
        },
        "experiment_metrics": {
            "label_distribution": {
                "positive": 1,
                "neutral": 0,
                "negative": 0,
            }
        }
    }
    
    with patch("ml_tooling.valence_classifier.model.run_vader_on_posts") as mock_vader, \
         patch("ml_tooling.valence_classifier.model.write_posts_to_cache") as mock_write, \
         patch("ml_tooling.valence_classifier.model.return_failed_labels_to_input_queue") as mock_return:
        
        # Only return data for the first URI
        mock_vader.return_value = pd.DataFrame([
            {"uri": "1", "valence_label": "positive", "compound": 0.8},
        ])
        
        result = model.batch_classify_posts(sample_posts, batch_size=2)
        
        # Compare result with expected result
        assert result["metadata"] == expected_result["metadata"]
        assert result["experiment_metrics"]["label_distribution"] == expected_result["experiment_metrics"]["label_distribution"]
        
        # Verify both success and failure handlers were called
        assert mock_write.call_count > 0
        assert mock_return.call_count > 0