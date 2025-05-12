import pytest
from unittest.mock import patch
from ml_tooling.valence_classifier import model
import pandas as pd

@pytest.fixture
def sample_posts():
    return [
        {"uri": "1", "text": "I love this!"},
        {"uri": "2", "text": "This is okay."},
        {"uri": "3", "text": "I hate this."},
    ]

def test_batch_classify_posts_empty():
    result = model.batch_classify_posts([])
    assert result["metadata"]["total_batches"] == 0
    assert result["labels"] == []

def test_run_batch_classification_delegates(sample_posts):
    with patch("ml_tooling.valence_classifier.model.batch_classify_posts") as mock_batch:
        mock_batch.return_value = {"labels": [1, 2, 3]}
        out = model.run_batch_classification(sample_posts)
        assert out["labels"] == [1, 2, 3]

def test_create_labels_success(sample_posts):
    output_df = pd.DataFrame([
        {"uri": "1", "valence_label": "positive", "compound": 0.8},
        {"uri": "2", "valence_label": "neutral", "compound": 0.0},
        {"uri": "3", "valence_label": "negative", "compound": -0.7},
    ])
    labels = model.create_labels(sample_posts, output_df)
    assert len(labels) == 3
    assert labels[0]["valence_label"] == "positive"
    assert labels[1]["valence_label"] == "neutral"
    assert labels[2]["valence_label"] == "negative"
    assert all(l["was_successfully_labeled"] for l in labels)

def test_create_labels_partial_success(sample_posts):
    output_df = pd.DataFrame([
        {"uri": "1", "valence_label": "positive", "compound": 0.8},
    ])
    labels = model.create_labels(sample_posts, output_df)
    assert len(labels) == 3
    assert labels[0]["was_successfully_labeled"]
    assert not labels[1]["was_successfully_labeled"]
    assert not labels[2]["was_successfully_labeled"]

def test_batch_classify_posts_success(sample_posts):
    with patch("ml_tooling.valence_classifier.model.run_vader_on_posts") as mock_vader:
        mock_vader.return_value = pd.DataFrame([
            {"uri": "1", "valence_label": "positive", "compound": 0.8},
            {"uri": "2", "valence_label": "neutral", "compound": 0.0},
            {"uri": "3", "valence_label": "negative", "compound": -0.7},
        ])
        result = model.batch_classify_posts(sample_posts, batch_size=2)
        assert result["metadata"]["total_posts_successfully_labeled"] == 3
        assert result["experiment_metrics"]["label_distribution"]["positive"] == 1
        assert result["experiment_metrics"]["label_distribution"]["neutral"] == 1
        assert result["experiment_metrics"]["label_distribution"]["negative"] == 1 