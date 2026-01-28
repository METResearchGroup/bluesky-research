"""Tests for intergroup.py.

This test suite verifies the functionality of intergroup orchestration:
- classify_latest_posts: Main orchestration function
"""

import pytest
from unittest.mock import Mock, patch

from services.ml_inference.intergroup.intergroup import classify_latest_posts
from services.ml_inference.models import ClassificationSessionModel


class TestClassifyLatestPosts:
    """Tests for classify_latest_posts function."""

    @pytest.fixture
    def mock_orchestrate(self):
        """Mock orchestrate_classification function."""
        with patch(
            "services.ml_inference.intergroup.intergroup.orchestrate_classification"
        ) as mock:
            yield mock

    def test_successful_classification_with_backfill_parameters(self, mock_orchestrate):
        """Test successful classification with backfill parameters.

        Expected behavior:
            - Should call orchestrate_classification with correct parameters
            - Should return ClassificationSessionModel
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=10,
            inference_metadata={"total_batches": 1},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts(
            backfill_period="days", backfill_duration=7
        )

        # Assert
        assert isinstance(result, ClassificationSessionModel)
        mock_orchestrate.assert_called_once()
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["backfill_period"] == "days"
        assert call_kwargs["backfill_duration"] == 7

    def test_passes_max_records_per_run_to_orchestrate_classification(
        self, mock_orchestrate
    ):
        """Test that max_records_per_run is passed through to orchestrate_classification."""
        # Arrange
        expected_max = 10
        mock_orchestrate.return_value = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=0,
            inference_metadata={},
        )

        # Act
        classify_latest_posts(max_records_per_run=expected_max)

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["max_records_per_run"] == expected_max

    def test_with_backfill_period_days_and_duration(self, mock_orchestrate):
        """Test with backfill_period="days" and backfill_duration=7.

        Expected behavior:
            - Should pass backfill parameters correctly
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=5,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts(
            backfill_period="days", backfill_duration=7
        )

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["backfill_period"] == "days"
        assert call_kwargs["backfill_duration"] == 7
        assert result.total_classified_posts == 5

    def test_with_backfill_period_hours_and_duration(self, mock_orchestrate):
        """Test with backfill_period="hours" and backfill_duration=24.

        Expected behavior:
            - Should pass backfill parameters correctly
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=3,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts(
            backfill_period="hours", backfill_duration=24
        )

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["backfill_period"] == "hours"
        assert call_kwargs["backfill_duration"] == 24

    def test_with_no_backfill_parameters(self, mock_orchestrate):
        """Test with no backfill parameters (None).

        Expected behavior:
            - Should pass None for backfill parameters
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=0,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts()

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["backfill_period"] is None
        assert call_kwargs["backfill_duration"] is None

    def test_with_run_classification_true(self, mock_orchestrate):
        """Test with run_classification=True (default).

        Expected behavior:
            - Should pass run_classification=True to orchestrate_classification
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=10,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        classify_latest_posts(run_classification=True)

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["run_classification"] is True

    def test_with_run_classification_false(self, mock_orchestrate):
        """Test with run_classification=False (skip classification).

        Expected behavior:
            - Should pass run_classification=False to orchestrate_classification
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=0,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts(run_classification=False)

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["run_classification"] is False
        assert result.total_classified_posts == 0

    def test_with_previous_run_metadata(self, mock_orchestrate):
        """Test with previous_run_metadata.

        Expected behavior:
            - Should pass previous_run_metadata to orchestrate_classification
        """
        # Arrange
        previous_metadata = {
            "latest_id": 123,
            "timestamp": "2024-01-01",
        }
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=5,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        classify_latest_posts(previous_run_metadata=previous_metadata)

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["previous_run_metadata"] == previous_metadata

    def test_with_event_parameter(self, mock_orchestrate):
        """Test with event parameter.

        Expected behavior:
            - Should pass event to orchestrate_classification
        """
        # Arrange
        event = {"test_key": "test_value"}
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=2,
            inference_metadata={},
            event=event,
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts(event=event)

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["event"] == event
        assert result.event == event

    def test_orchestrate_called_with_correct_config(self, mock_orchestrate):
        """Test that orchestrate_classification is called with correct INTERGROUP_CONFIG.

        Expected behavior:
            - Should pass INTERGROUP_CONFIG to orchestrate_classification
        """
        # Arrange
        from services.ml_inference.intergroup.intergroup import INTERGROUP_CONFIG

        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=1,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        classify_latest_posts()

        # Assert
        # orchestrate_classification is called with config as keyword argument
        call_args = mock_orchestrate.call_args
        assert call_args.kwargs["config"] == INTERGROUP_CONFIG

    def test_classification_session_model_returned(self, mock_orchestrate):
        """Test that ClassificationSessionModel is returned.

        Expected behavior:
            - Should return ClassificationSessionModel instance
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=10,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts()

        # Assert
        assert isinstance(result, ClassificationSessionModel)
        assert result == expected_result

    def test_result_has_correct_inference_type(self, mock_orchestrate):
        """Test that result has correct inference_type="intergroup".

        Expected behavior:
            - Returned model should have inference_type="intergroup"
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=5,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts()

        # Assert
        assert result.inference_type == "intergroup"

    def test_behavior_when_no_posts_found(self, mock_orchestrate):
        """Test behavior when no posts found (empty result).

        Expected behavior:
            - Should return ClassificationSessionModel with zero counts
        """
        # Arrange
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=0,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        result = classify_latest_posts()

        # Assert
        assert result.total_classified_posts == 0
        assert result.inference_metadata == {}

    def test_all_parameters_passed_through(self, mock_orchestrate):
        """Test that all parameters are passed through to orchestrate_classification.

        Expected behavior:
            - All parameters should be forwarded correctly
        """
        # Arrange
        previous_metadata = {"latest_id": 100}
        event = {"key": "value"}
        expected_result = ClassificationSessionModel(
            inference_type="intergroup",
            inference_timestamp="2024-01-01-12:00:00",
            total_classified_posts=1,
            inference_metadata={},
        )
        mock_orchestrate.return_value = expected_result

        # Act
        classify_latest_posts(
            backfill_period="days",
            backfill_duration=5,
            run_classification=True,
            previous_run_metadata=previous_metadata,
            event=event,
        )

        # Assert
        call_kwargs = mock_orchestrate.call_args[1]
        assert call_kwargs["backfill_period"] == "days"
        assert call_kwargs["backfill_duration"] == 5
        assert call_kwargs["run_classification"] is True
        assert call_kwargs["previous_run_metadata"] == previous_metadata
        assert call_kwargs["event"] == event
