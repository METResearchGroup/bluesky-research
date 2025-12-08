"""Tests for perspective_api handler."""

import json
import pytest
from unittest.mock import patch
from pipelines.classify_records.perspective_api import handler
from services.ml_inference.models import ClassificationSessionModel


class TestLambdaHandler:
    def test_lambda_handler_default_event(self):
        """Test handler with default event and ClassificationSessionModel return."""
        mock_model = ClassificationSessionModel(
            inference_type="perspective_api",
            inference_timestamp="2024-01-01T00:00:00",
            total_classified_posts=0,
            event={},
            inference_metadata={},
        )
        with patch(
            "pipelines.classify_records.perspective_api.handler.classify_latest_posts",
            return_value=mock_model,
        ):
            result = handler.lambda_handler(None, None)
            assert result["status_code"] == 200
            assert result["service"] == "ml_inference_perspective_api"
            assert "timestamp" in result
            assert "metadata" in result
            # Verify metadata is valid JSON
            metadata = json.loads(result["metadata"])
            assert metadata["inference_timestamp"] == "2024-01-01T00:00:00"

    def test_lambda_handler_valid_event(self):
        """Test handler with valid event and ClassificationSessionModel return."""
        event = {"backfill_period": "days", "backfill_duration": 1}
        mock_model = ClassificationSessionModel(
            inference_type="perspective_api",
            inference_timestamp="2024-01-01T00:00:00",
            total_classified_posts=10,
            event=event,
            inference_metadata={"total_batches": 2},
        )
        with patch(
            "pipelines.classify_records.perspective_api.handler.classify_latest_posts",
            return_value=mock_model,
        ):
            result = handler.lambda_handler(event, None)
            assert result["status_code"] == 200
            assert result["service"] == "ml_inference_perspective_api"
            assert "timestamp" in result
            assert "metadata" in result
            # Verify metadata can be parsed and contains expected data
            metadata = json.loads(result["metadata"])
            assert metadata["total_classified_posts"] == 10
            assert metadata["inference_metadata"]["total_batches"] == 2

    def test_lambda_handler_error(self):
        """Test handler error handling."""
        with patch(
            "pipelines.classify_records.perspective_api.handler.classify_latest_posts",
            side_effect=Exception("fail"),
        ):
            result = handler.lambda_handler({}, None)
            assert result["status_code"] == 500
            assert result["service"] == "ml_inference_perspective_api"
            assert "timestamp" in result
            assert "metadata" in result
            assert "fail" in result["body"]

    def test_handler_bracket_notation_access(self):
        """Test that handler can access fields using bracket notation after model_dump()."""
        mock_model = ClassificationSessionModel(
            inference_type="perspective_api",
            inference_timestamp="2024-01-15-14:30:00",
            total_classified_posts=100,
            event=None,
            inference_metadata={},
        )
        with patch(
            "pipelines.classify_records.perspective_api.handler.classify_latest_posts",
            return_value=mock_model,
        ):
            result = handler.lambda_handler({}, None)
            # Verify timestamp was accessed using bracket notation
            assert result["timestamp"] == "2024-01-15-14:30:00"
            # Verify metadata is valid JSON string
            metadata = json.loads(result["metadata"])
            assert metadata["inference_timestamp"] == "2024-01-15-14:30:00"
            assert metadata["inference_type"] == "perspective_api"
            assert metadata["total_classified_posts"] == 100

