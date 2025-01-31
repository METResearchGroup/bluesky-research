"""Tests for perspective_api.py."""

import pytest
from unittest.mock import Mock, patch

from services.ml_inference.perspective_api.perspective_api import classify_latest_posts


class TestClassifyLatestPosts:
    """Tests for classify_latest_posts function.
    
    This class tests the main function that coordinates post classification using
    the Perspective API. It verifies:
    - Proper handling of backfill parameters
    - Post retrieval and filtering
    - Classification execution
    - Metadata generation and return
    """

    @pytest.fixture
    def mock_determine_backfill(self):
        """Mock determine_backfill_latest_timestamp function."""
        with patch(
            "services.ml_inference.perspective_api.perspective_api.determine_backfill_latest_timestamp"
        ) as mock:
            mock.return_value = "2024-01-01-12:00:00"
            yield mock

    @pytest.fixture
    def mock_get_posts(self):
        """Mock get_posts_to_classify function."""
        with patch(
            "services.ml_inference.perspective_api.perspective_api.get_posts_to_classify"
        ) as mock:
            mock.return_value = [
                {"uri": "test1", "text": "test post 1"},
                {"uri": "test2", "text": "test post 2"}
            ]
            yield mock

    @pytest.fixture
    def mock_run_classification(self):
        """Mock run_batch_classification function."""
        with patch(
            "services.ml_inference.perspective_api.perspective_api.run_batch_classification"
        ) as mock:
            mock.return_value = {
                "total_batches": 1,
                "total_posts_successfully_labeled": 2,
                "total_posts_failed_to_label": 0
            }
            yield mock

    @pytest.fixture
    def mock_logger(self):
        """Mock logger."""
        with patch(
            "services.ml_inference.perspective_api.perspective_api.logger"
        ) as mock:
            yield mock

    def test_classify_latest_posts_success(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test successful classification of latest posts.
        
        Should properly coordinate all components and return complete metadata.
        """
        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7
        )
        
        # Verify function calls
        mock_determine_backfill.assert_called_once_with(
            backfill_duration=7,
            backfill_period="days"
        )
        mock_get_posts.assert_called_once_with(
            inference_type="perspective_api",
            timestamp="2024-01-01-12:00:00",
            previous_run_metadata=None
        )
        mock_run_classification.assert_called_once_with(posts=mock_get_posts.return_value)
        
        # Verify result structure
        assert isinstance(result, dict)
        assert result["inference_type"] == "perspective_api"
        assert "inference_timestamp" in result
        assert result["total_classified_posts"] == 2
        assert "inference_metadata" in result
        assert result["event"] is None

    def test_classify_latest_posts_no_posts(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test behavior when no posts are found to classify.
        
        Should return metadata with zero counts.
        """
        mock_get_posts.return_value = []
        
        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7
        )
        
        mock_run_classification.assert_not_called()
        assert result["total_classified_posts"] == 0
        assert "inference_metadata" in result
        mock_logger.warning.assert_called_once()

    def test_classify_latest_posts_with_event(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test classification with event metadata.
        
        Should include event data in returned metadata.
        """
        test_event = {"test_key": "test_value"}
        
        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7,
            event=test_event
        )
        
        assert result["event"] == test_event

    def test_classify_latest_posts_skip_classification(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test skipping classification and exporting cached results.
        
        Should not perform classification but return metadata.
        """
        result = classify_latest_posts(
            run_classification=False
        )
        
        # Verify no classification was attempted
        mock_determine_backfill.assert_not_called()
        mock_get_posts.assert_not_called()
        mock_run_classification.assert_not_called()
        
        # Verify result structure
        assert isinstance(result, dict)
        assert result["inference_type"] == "perspective_api"
        assert "inference_timestamp" in result
        assert result["total_classified_posts"] == 0  # Should be 0 when skipping
        assert "inference_metadata" in result
        assert result["inference_metadata"] == {}  # Should be empty when skipping
        assert result["event"] is None
        
        # Verify correct logging
        mock_logger.info.assert_called_with(
            "Skipping classification and exporting cached results..."
        )

    def test_classify_latest_posts_with_previous_metadata(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_run_classification,
        mock_logger
    ):
        """Test classification using previous run metadata.
        
        Should pass previous metadata to get_posts_to_classify.
        """
        previous_metadata = {
            "latest_id": 123,
            "timestamp": "2024-01-01"
        }
        
        result = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7,
            previous_run_metadata=previous_metadata
        )
        
        mock_get_posts.assert_called_once_with(
            inference_type="perspective_api",
            timestamp="2024-01-01-12:00:00",
            previous_run_metadata=previous_metadata
        )
        assert result["total_classified_posts"] == 2

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
        
        Should still function with default/None timestamp.
        """
        mock_determine_backfill.return_value = None
        
        result = classify_latest_posts(
            backfill_period=backfill_period,
            backfill_duration=backfill_duration
        )
        
        mock_get_posts.assert_called_once_with(
            inference_type="perspective_api",
            timestamp=None,
            previous_run_metadata=None
        )
        assert isinstance(result, dict)
        assert "inference_timestamp" in result 