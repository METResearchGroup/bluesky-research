"""Tests for ime.py.

This test suite verifies the functionality of the IME classification service:
- Proper handling of backfill parameters
- Correct classification of posts
- Appropriate logging
- Error handling and edge cases
- Integration with the IME model
"""

from unittest.mock import patch, MagicMock
import pytest

from ml_tooling.ime.constants import default_hyperparameters
from services.ml_inference.ime.ime import classify_latest_posts


class TestClassifyLatestPosts:
    """Tests for classify_latest_posts function."""

    @pytest.fixture
    def mock_determine_backfill(self):
        """Mock the determine_backfill_latest_timestamp function."""
        with patch("services.ml_inference.ime.ime.determine_backfill_latest_timestamp") as mock:
            mock.return_value = "2024-01-01T00:00:00"
            yield mock

    @pytest.fixture
    def mock_get_posts(self):
        """Mock the get_posts_to_classify function."""
        with patch("services.ml_inference.ime.ime.get_posts_to_classify") as mock:
            mock.return_value = [
                {"uri": "post1", "text": "text1"},
                {"uri": "post2", "text": "text2"}
            ]
            yield mock

    @pytest.fixture
    def mock_run_classification(self):
        """Mock the run_batch_classification function."""
        with patch("services.ml_inference.ime.ime.run_batch_classification") as mock:
            mock.return_value = {
                "model_version": "v1.0",
                "batch_size": 2,
                "processing_time": 1.5
            }
            yield mock

    @pytest.fixture
    def mock_logger(self):
        """Mock the logger."""
        with patch("services.ml_inference.ime.ime.logger") as mock:
            yield mock

    def test_classify_latest_posts_success(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test successful classification of latest posts.
        
        This test verifies that:
        1. Posts are retrieved correctly
        2. Classification is run with correct parameters
        3. Results are properly formatted
        4. Appropriate logging occurs
        """
        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7,
            run_classification=True
        )

        mock_determine_backfill.assert_called_once_with(
            backfill_duration=7,
            backfill_period="days"
        )
        mock_get_posts.assert_called_once_with(
            inference_type="ime",
            timestamp="2024-01-01T00:00:00",
            previous_run_metadata=None
        )
        mock_run_classification.assert_called_once_with(
            posts=mock_get_posts.return_value,
            hyperparameters=default_hyperparameters
        )

        assert result["inference_type"] == "ime"
        assert result["total_classified_posts"] == 2
        assert "inference_timestamp" in result
        assert result["inference_metadata"] == {
            "model_version": "v1.0",
            "batch_size": 2,
            "processing_time": 1.5
        }

    def test_classify_latest_posts_no_posts(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test behavior when no posts are found to classify.
        
        This test verifies that:
        1. Empty result set is handled gracefully
        2. No classification is attempted
        3. Appropriate warning is logged
        4. Result contains expected zero counts
        """
        mock_get_posts.return_value = []

        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7
        )

        mock_run_classification.assert_not_called()
        mock_logger.warning.assert_called_with(
            "No posts to classify with IME classifier. Exiting..."
        )

        assert result["total_classified_posts"] == 0
        assert result["inference_metadata"] == {}

    def test_classify_latest_posts_with_event(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test classification with event metadata.
        
        This test verifies that:
        1. Event metadata is properly passed through
        2. Custom hyperparameters are used if provided
        3. Event data is included in result
        """
        event = {
            "hyperparameters": {"custom_param": "value"},
            "metadata": {"run_id": "test123"}
        }

        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7,
            event=event
        )

        mock_run_classification.assert_called_once_with(
            posts=mock_get_posts.return_value,
            hyperparameters={"custom_param": "value"}
        )

        assert result["event"] == event

    def test_classify_latest_posts_skip_classification(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test skipping classification and exporting cached results.
        
        This test verifies that:
        1. No classification is performed when run_classification=False
        2. No posts are retrieved
        3. Appropriate logging occurs
        4. Result contains expected zero counts
        """
        result = classify_latest_posts(run_classification=False)

        mock_determine_backfill.assert_not_called()
        mock_get_posts.assert_not_called()
        mock_run_classification.assert_not_called()
        mock_logger.info.assert_called_with(
            "Skipping classification and exporting cached results..."
        )

        assert result["total_classified_posts"] == 0
        assert result["inference_metadata"] == {}

    def test_classify_latest_posts_with_previous_metadata(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test classification with previous run metadata.
        
        This test verifies that:
        1. Previous metadata is passed to get_posts_to_classify
        2. Classification proceeds normally
        3. Results include new classification metadata
        """
        previous_metadata = {
            "last_processed_id": "123",
            "timestamp": "2024-01-01T00:00:00"
        }

        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7,
            previous_run_metadata=previous_metadata
        )

        mock_get_posts.assert_called_once_with(
            inference_type="ime",
            timestamp="2024-01-01T00:00:00",
            previous_run_metadata=previous_metadata
        )
        mock_run_classification.assert_called_once_with(
            posts=mock_get_posts.return_value,
            hyperparameters=default_hyperparameters
        )

        assert result["total_classified_posts"] == 2
        assert result["inference_metadata"] == {
            "model_version": "v1.0",
            "batch_size": 2,
            "processing_time": 1.5
        }

    @pytest.mark.parametrize("backfill_period,backfill_duration", [
        (None, None),
        ("days", None),
        (None, 7),
        ("invalid", 7)
    ])
    def test_classify_latest_posts_invalid_backfill(
        self,
        backfill_period,
        backfill_duration,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test handling of invalid backfill parameters.
        
        This test verifies that:
        1. Invalid parameter combinations are handled gracefully
        2. Classification proceeds with default timestamp
        3. Appropriate logging occurs
        
        Args:
            backfill_period: The unit for backfilling (days/hours/invalid)
            backfill_duration: The number of units to backfill
        """
        mock_determine_backfill.return_value = None

        result = classify_latest_posts(
            backfill_period=backfill_period,
            backfill_duration=backfill_duration
        )

        mock_determine_backfill.assert_called_once_with(
            backfill_duration=backfill_duration,
            backfill_period=backfill_period
        )
        mock_get_posts.assert_called_once_with(
            inference_type="ime",
            timestamp=None,
            previous_run_metadata=None
        )
        mock_run_classification.assert_called_once_with(
            posts=mock_get_posts.return_value,
            hyperparameters=default_hyperparameters
        )

        assert result["total_classified_posts"] == 2
        assert "inference_timestamp" in result 