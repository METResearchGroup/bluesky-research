import pytest
from unittest.mock import patch
from pipelines.classify_records.valence_classifier import handler

class TestLambdaHandler:
    def test_lambda_handler_default_event(self):
        with patch("pipelines.classify_records.valence_classifier.handler.classify_latest_posts", return_value={
            "inference_timestamp": "2024-01-01T00:00:00",
            "inference_type": "valence_classifier",
            "total_classified_posts": 0,
            "event": {},
            "inference_metadata": {},
        }):
            result = handler.lambda_handler(None, None)
            assert result["status_code"] == 200
            assert result["service"] == "ml_inference_valence_classifier"
            assert "timestamp" in result
            assert "metadata" in result

    def test_lambda_handler_valid_event(self):
        event = {"backfill_period": "days", "backfill_duration": 1}
        with patch("pipelines.classify_records.valence_classifier.handler.classify_latest_posts", return_value={
            "inference_timestamp": "2024-01-01T00:00:00",
            "inference_type": "valence_classifier",
            "total_classified_posts": 10,
            "event": event,
            "inference_metadata": {"labels": [1, 2]},
        }):
            result = handler.lambda_handler(event, None)
            assert result["status_code"] == 200
            assert result["service"] == "ml_inference_valence_classifier"
            assert "timestamp" in result
            assert "metadata" in result

    def test_lambda_handler_error(self):
        with patch("pipelines.classify_records.valence_classifier.handler.classify_latest_posts", side_effect=Exception("fail")):
            result = handler.lambda_handler({}, None)
            assert result["status_code"] == 500
            assert result["service"] == "ml_inference_valence_classifier"
            assert "timestamp" in result
            assert "metadata" in result
            assert "fail" in result["body"] 