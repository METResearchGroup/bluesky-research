"""Tests for handler.py."""

import json
import traceback
from unittest.mock import patch, Mock

import pytest

from pipelines.classify_records.intergroup.handler import lambda_handler


class TestLambdaHandler:
    """Tests for lambda_handler() function."""

    @pytest.fixture
    def mock_classify_latest_posts(self):
        """Mock classify_latest_posts function."""
        with patch(
            "pipelines.classify_records.intergroup.handler.classify_latest_posts"
        ) as mock:
            mock.return_value = Mock(
                model_dump=lambda: {
                    "inference_type": "intergroup",
                    "inference_timestamp": "2024-01-01-12:00:00",
                    "total_classified_posts": 2,
                    "event": None,
                    "inference_metadata": {
                        "total_batches": 1,
                        "total_posts_successfully_labeled": 2,
                        "total_posts_failed_to_label": 0
                    }
                }
            )
            yield mock

    @pytest.fixture
    def mock_generate_datetime(self):
        """Mock generate_current_datetime_str function."""
        with patch(
            "pipelines.classify_records.intergroup.handler.generate_current_datetime_str"
        ) as mock:
            mock.return_value = "2024-01-01-12:00:00"
            yield mock

    @pytest.fixture
    def mock_logger(self):
        """Mock logger."""
        with patch(
            "pipelines.classify_records.intergroup.handler.logger"
        ) as mock:
            yield mock

    def test_successful_execution_with_default_event(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test successful execution with default event.

        Should properly handle None event and return success metadata.
        """
        # Arrange
        event = None
        context = None
        expected_service = "ml_inference_intergroup"
        expected_status_code = 200

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["service"] == expected_service
        assert result["status_code"] == expected_status_code
        assert result["timestamp"] == "2024-01-01-12:00:00"
        assert "metadata" in result
        mock_classify_latest_posts.assert_called_once()
        call_kwargs = mock_classify_latest_posts.call_args[1]
        assert call_kwargs["backfill_period"] is None
        assert call_kwargs["backfill_duration"] is None
        assert call_kwargs["run_classification"] is True

    def test_successful_execution_with_backfill_parameters(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test successful execution with backfill parameters.

        Should properly parse and pass backfill parameters.
        """
        # Arrange
        event = {
            "backfill_period": "days",
            "backfill_duration": "7"
        }
        context = None

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["status_code"] == 200
        mock_classify_latest_posts.assert_called_once()
        call_kwargs = mock_classify_latest_posts.call_args[1]
        assert call_kwargs["backfill_period"] == "days"
        assert call_kwargs["backfill_duration"] == 7  # Should be converted to int
        assert call_kwargs["run_classification"] is True

    def test_error_handling_and_logging(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test error handling and logging.

        Should properly log errors and return error metadata.
        """
        # Arrange
        event = None
        context = None
        test_error = ValueError("Test error")
        mock_classify_latest_posts.side_effect = test_error

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["status_code"] == 500
        assert result["service"] == "ml_inference_intergroup"
        assert "Error in intergroup classification" in result["body"]
        mock_logger.error.assert_called()
        assert any("Test error" in str(call) for call in mock_logger.error.call_args_list)

    def test_event_parsing_backfill_period_conversion(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test event parsing (backfill_period, backfill_duration conversion).

        Should properly convert backfill_duration string to int.
        """
        # Arrange
        event = {
            "backfill_period": "hours",
            "backfill_duration": "24"
        }
        context = None

        # Act
        result = lambda_handler(event, context)

        # Assert
        mock_classify_latest_posts.assert_called_once()
        call_kwargs = mock_classify_latest_posts.call_args[1]
        assert call_kwargs["backfill_period"] == "hours"
        assert isinstance(call_kwargs["backfill_duration"], int)
        assert call_kwargs["backfill_duration"] == 24

    def test_return_value_structure(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test return value structure (session_status_metadata).

        Should return properly structured session status metadata.
        """
        # Arrange
        event = None
        context = None
        expected_fields = [
            "service", "timestamp", "status_code", "body",
            "metadata_table_name", "metadata"
        ]

        # Act
        result = lambda_handler(event, context)

        # Assert
        for field in expected_fields:
            assert field in result
        assert result["service"] == "ml_inference_intergroup"
        assert result["status_code"] == 200
        assert isinstance(result["metadata"], str)  # Should be JSON string

    def test_exception_handling_with_proper_status_codes(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test exception handling with proper status codes (500).

        Should return 500 status code on exceptions.
        """
        # Arrange
        event = None
        context = None
        mock_classify_latest_posts.side_effect = Exception("Unexpected error")

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["status_code"] == 500
        assert "Error in intergroup classification" in result["body"]
        assert result["timestamp"] == "2024-01-01-12:00:00"
        mock_logger.error.assert_called()

    def test_metadata_serialization(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test metadata serialization.

        Should properly serialize metadata to JSON string.
        """
        # Arrange
        event = None
        context = None

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert isinstance(result["metadata"], str)
        metadata_dict = json.loads(result["metadata"])
        assert metadata_dict["inference_type"] == "intergroup"
        assert metadata_dict["total_classified_posts"] == 2

    def test_backfill_duration_none_handling(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test handling when backfill_duration is None.

        Should handle None backfill_duration without conversion error.
        """
        # Arrange
        event = {
            "backfill_period": "days",
            "backfill_duration": None
        }
        context = None

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["status_code"] == 200
        mock_classify_latest_posts.assert_called_once()
        call_kwargs = mock_classify_latest_posts.call_args[1]
        assert call_kwargs["backfill_duration"] is None

    def test_traceback_in_error_metadata(
        self,
        mock_classify_latest_posts,
        mock_generate_datetime,
        mock_logger
    ):
        """Test that traceback is included in error metadata.

        Should include full traceback in error response metadata.
        """
        # Arrange
        event = None
        context = None
        mock_classify_latest_posts.side_effect = RuntimeError("Runtime error")

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["status_code"] == 500
        metadata = json.loads(result["metadata"])  # traceback is JSON-encoded string
        assert isinstance(metadata, str)
        assert "RuntimeError" in metadata or "Runtime error" in metadata

