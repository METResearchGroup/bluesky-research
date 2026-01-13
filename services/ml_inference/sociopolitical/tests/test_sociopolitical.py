"""Tests for sociopolitical.py."""

import pytest
from unittest.mock import Mock, patch

from services.ml_inference.sociopolitical import sociopolitical
from services.ml_inference.sociopolitical.sociopolitical import classify_latest_posts
from services.ml_inference.models import PostToLabelModel


class TestClassifyLatestPosts:
    """Tests for classify_latest_posts function.
    
    This class tests the main function that coordinates post classification using
    LLM inference. It verifies:
    - Proper handling of backfill parameters
    - Post retrieval and filtering
    - Classification execution 
    - Metadata generation and return
    """

    @pytest.fixture
    def mock_determine_backfill(self):
        """Mock determine_backfill_latest_timestamp function."""
        with patch(
            "services.ml_inference.helper.determine_backfill_latest_timestamp"
        ) as mock:
            mock.return_value = "2024-01-01-12:00:00"
            yield mock

    @pytest.fixture
    def mock_get_posts(self):
        """Mock get_posts_to_classify function."""
        with patch(
            "services.ml_inference.helper.get_posts_to_classify"
        ) as mock:
            mock.return_value = [
                PostToLabelModel(
                    uri="test1",
                    text="test post 1",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=1,
                    batch_metadata="{}",
                ),
                PostToLabelModel(
                    uri="test2",
                    text="test post 2",
                    preprocessing_timestamp="2024-01-01-12:00:00",
                    batch_id=2,
                    batch_metadata="{}",
                ),
            ]
            yield mock

    @pytest.fixture
    def mock_run_classification(self):
        """Mock run_batch_classification function."""
        # Patch the function stored in the config object
        original_func = sociopolitical.SOCIOPOLITICAL_CONFIG.classification_func
        mock_func = Mock(return_value={
            "total_batches": 1,
            "total_posts_successfully_labeled": 2,
            "total_posts_failed_to_label": 0
        })
        sociopolitical.SOCIOPOLITICAL_CONFIG.classification_func = mock_func
        yield mock_func
        # Restore original function
        sociopolitical.SOCIOPOLITICAL_CONFIG.classification_func = original_func

    @pytest.fixture
    def mock_logger(self):
        """Mock logger."""
        with patch(
            "services.ml_inference.helper.logger"
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
            inference_type="sociopolitical",
            timestamp="2024-01-01-12:00:00",
            previous_run_metadata=None
        )
        mock_run_classification.assert_called_once_with(posts=mock_get_posts.return_value)
        
        # Verify result structure
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.inference_type == "sociopolitical"
        assert result.inference_timestamp is not None
        assert result.total_classified_posts == 2
        assert result.inference_metadata is not None
        assert result.event is None

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
        assert result.total_classified_posts == 0
        assert result.inference_metadata is not None
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
        
        assert result.event == test_event

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
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.inference_type == "sociopolitical"
        assert result.inference_timestamp is not None
        assert result.total_classified_posts == 0  # Should be 0 when skipping
        assert result.inference_metadata == {}  # Should be empty when skipping
        assert result.event is None
        
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
            inference_type="sociopolitical",
            timestamp="2024-01-01-12:00:00",
            previous_run_metadata=previous_metadata
        )
        assert result.total_classified_posts == 2

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
            inference_type="sociopolitical",
            timestamp=None,
            previous_run_metadata=None
        )
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.inference_timestamp is not None
