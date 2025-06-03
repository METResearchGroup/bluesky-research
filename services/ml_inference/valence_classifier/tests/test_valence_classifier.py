import pytest
from unittest.mock import patch
from services.ml_inference.valence_classifier import valence_classifier

@pytest.fixture
def sample_posts():
    return [
        {"uri": "1", "text": "I love this!", "batch_id": "batch1", "preprocessing_timestamp": "2023-01-01T00:00:00Z"},
        {"uri": "2", "text": "It's okay, it's fine.", "batch_id": "batch1", "preprocessing_timestamp": "2023-01-01T00:00:00Z"},
        {"uri": "3", "text": "I hate this.", "batch_id": "batch1", "preprocessing_timestamp": "2023-01-01T00:00:00Z"},
    ]

def test_classify_latest_posts_no_posts():
    # Expected result when no posts are found
    expected_result = {
        "inference_type": "valence_classifier",
        "inference_timestamp": "2024-01-01T00:00:00Z",
        "total_classified_posts": 0,
        "event": None,
        "inference_metadata": {},
    }
    
    with patch("services.ml_inference.valence_classifier.valence_classifier.get_posts_to_classify", return_value=[]):
        with patch("services.ml_inference.valence_classifier.valence_classifier.determine_backfill_latest_timestamp", return_value="2024-01-01T00:00:00"):
            with patch("services.ml_inference.valence_classifier.valence_classifier.generate_current_datetime_str", return_value="2024-01-01T00:00:00Z"):
                result = valence_classifier.classify_latest_posts()
                
                assert result["total_classified_posts"] == expected_result["total_classified_posts"]
                assert result["inference_metadata"] == expected_result["inference_metadata"]
                assert result["inference_type"] == expected_result["inference_type"]
                assert result["inference_timestamp"] == expected_result["inference_timestamp"]
                assert result["event"] == expected_result["event"]

def test_classify_latest_posts_with_posts(sample_posts):
    # Expected metadata from classification
    expected_metadata = {
        "metadata": {
            "total_batches": 1,
            "total_posts_successfully_labeled": 3,
            "total_posts_failed_to_label": 0,
        },
        "experiment_metrics": {
            "label_distribution": {
                "positive": 2,
                "neutral": 0,
                "negative": 1,
            }
        }
    }
    
    # Expected result structure
    expected_result = {
        "inference_type": "valence_classifier",
        "inference_timestamp": "2024-01-01T00:00:00Z",
        "total_classified_posts": 3,
        "event": None,
        "inference_metadata": expected_metadata,
    }
    
    with patch("services.ml_inference.valence_classifier.valence_classifier.get_posts_to_classify", return_value=sample_posts):
        with patch("services.ml_inference.valence_classifier.valence_classifier.determine_backfill_latest_timestamp", return_value="2024-01-01T00:00:00"):
            with patch("services.ml_inference.valence_classifier.valence_classifier.generate_current_datetime_str", return_value="2024-01-01T00:00:00Z"):
                with patch("ml_tooling.valence_classifier.model.run_batch_classification", return_value=expected_metadata):
                    # Patch the queue operations to prevent the batch_delete_items_by_ids error
                    with patch("services.ml_inference.export_data.write_posts_to_cache"):
                        with patch("lib.db.queue.Queue.batch_delete_items_by_ids"):
                            result = valence_classifier.classify_latest_posts()
                    
                    assert result["total_classified_posts"] == expected_result["total_classified_posts"]
                    assert result["inference_metadata"] == expected_result["inference_metadata"]
                    assert result["inference_type"] == expected_result["inference_type"]
                    assert result["inference_timestamp"] == expected_result["inference_timestamp"]
                    assert result["event"] == expected_result["event"]

def test_classify_latest_posts_run_classification_false():
    # Expected result when run_classification is False
    expected_result = {
        "inference_type": "valence_classifier",
        "inference_timestamp": "2024-01-01T00:00:00Z",
        "total_classified_posts": 0,
        "event": None,
        "inference_metadata": {},
    }
    
    with patch("services.ml_inference.valence_classifier.valence_classifier.generate_current_datetime_str", return_value="2024-01-01T00:00:00Z"):
        result = valence_classifier.classify_latest_posts(run_classification=False)
        
        assert result["total_classified_posts"] == expected_result["total_classified_posts"]
        assert result["inference_metadata"] == expected_result["inference_metadata"]
        assert result["inference_type"] == expected_result["inference_type"]
        assert result["inference_timestamp"] == expected_result["inference_timestamp"]
        assert result["event"] == expected_result["event"]

def test_classify_latest_posts_event_passthrough():
    # Test event
    event = {"foo": "bar"}
    
    # Expected result with event passed through
    expected_result = {
        "inference_type": "valence_classifier",
        "inference_timestamp": "2024-01-01T00:00:00Z",
        "total_classified_posts": 0,
        "event": event,
        "inference_metadata": {},
    }
    
    with patch("services.ml_inference.valence_classifier.valence_classifier.get_posts_to_classify", return_value=[]):
        with patch("services.ml_inference.valence_classifier.valence_classifier.determine_backfill_latest_timestamp", return_value="2024-01-01T00:00:00"):
            with patch("services.ml_inference.valence_classifier.valence_classifier.generate_current_datetime_str", return_value="2024-01-01T00:00:00Z"):
                result = valence_classifier.classify_latest_posts(event=event)
                
                assert result["event"] == expected_result["event"]
                assert result["total_classified_posts"] == expected_result["total_classified_posts"]
                assert result["inference_metadata"] == expected_result["inference_metadata"]
                assert result["inference_type"] == expected_result["inference_type"]
                assert result["inference_timestamp"] == expected_result["inference_timestamp"]
