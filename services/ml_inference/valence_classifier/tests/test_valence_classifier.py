import pytest
from unittest.mock import patch
from services.ml_inference.valence_classifier import valence_classifier

def test_classify_latest_posts_no_posts():
    with patch("services.ml_inference.valence_classifier.valence_classifier.get_posts_to_classify", return_value=[]):
        with patch("services.ml_inference.valence_classifier.valence_classifier.determine_backfill_latest_timestamp", return_value="2024-01-01T00:00:00"):
            result = valence_classifier.classify_latest_posts()
            assert result["total_classified_posts"] == 0
            assert result["inference_metadata"] == {}
            assert result["inference_type"] == "valence_classifier"

def test_classify_latest_posts_with_posts():
    posts = [{"uri": "1", "text": "I love this!"}]
    expected_label = {
        "uri": "1",
        "text": "I love this!",
        "valence_label": "positive",
        "compound": pytest.approx(0.6696, abs=0.05),  # VADER's value may vary slightly
        "was_successfully_labeled": True
    }
    with patch("services.ml_inference.valence_classifier.valence_classifier.get_posts_to_classify", return_value=posts):
        with patch("services.ml_inference.valence_classifier.valence_classifier.determine_backfill_latest_timestamp", return_value="2024-01-01T00:00:00"):
            # Patch to use the real model, not a mock, so we get the real label dict
            result = valence_classifier.classify_latest_posts()
            assert result["total_classified_posts"] == 1
            assert isinstance(result["inference_metadata"], dict)
            assert "labels" in result["inference_metadata"]
            labels = result["inference_metadata"]["labels"]
            assert isinstance(labels, list)
            assert len(labels) == 1
            label = labels[0]
            assert label["uri"] == expected_label["uri"]
            assert label["text"] == expected_label["text"]
            assert label["valence_label"] == expected_label["valence_label"]
            assert label["was_successfully_labeled"] is True
            assert isinstance(label["compound"], float)

def test_classify_latest_posts_run_classification_false():
    result = valence_classifier.classify_latest_posts(run_classification=False)
    assert result["total_classified_posts"] == 0
    assert result["inference_metadata"] == {}

def test_classify_latest_posts_event_passthrough():
    event = {"foo": "bar"}
    with patch("services.ml_inference.valence_classifier.valence_classifier.get_posts_to_classify", return_value=[]):
        with patch("services.ml_inference.valence_classifier.valence_classifier.determine_backfill_latest_timestamp", return_value="2024-01-01T00:00:00"):
            result = valence_classifier.classify_latest_posts(event=event)
            assert result["event"] == event 