"""Tests for perspective_api.py."""

import pytest
from unittest.mock import Mock, patch, call

from services.ml_inference.perspective_api.perspective_api import classify_latest_posts
from services.ml_inference.models import PostToLabelModel


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
    def mock_process_batch(self):
        """Mock process_perspective_batch_with_retries function."""
        with patch(
            "ml_tooling.perspective_api.model.process_perspective_batch_with_retries"
        ) as mock:
            # Create an async mock that returns different success/failure patterns
            async def mock_process(*args, **kwargs):
                test_response = {
                    "prob_toxic": 0.8,
                    "label_toxic": 1,
                    "prob_reasoning": 0.6,
                    "label_reasoning": 1,
                    "prob_constructive": 0.6,
                    "label_constructive": 1
                }
                
                # Get the posts from kwargs
                posts = kwargs.get("requests", [])
                total_posts = len(posts)
                
                # First run: 20 succeed, 30 fail
                if total_posts == 50:
                    return ([test_response] * 20) + ([None] * 30)
                # Second run: 18 succeed, 12 fail
                elif total_posts == 30:
                    return ([test_response] * 18) + ([None] * 12)
                # Final run: all 12 succeed
                elif total_posts == 12:
                    return [test_response] * 12
                else:
                    return []
                
            mock.side_effect = mock_process
            yield mock

    @pytest.fixture
    def mock_logger(self):
        """Mock logger."""
        with patch(
            "services.ml_inference.helper.logger"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_write_posts_to_cache(self):
        """Mock write_posts_to_cache function."""
        with patch(
            "ml_tooling.perspective_api.model.write_posts_to_cache"
        ) as mock:
            yield mock

    @pytest.fixture
    def mock_return_failed_labels(self):
        """Mock return_failed_labels_to_input_queue function."""
        with patch(
            "ml_tooling.perspective_api.model.return_failed_labels_to_input_queue"
        ) as mock:
            yield mock

    def test_classify_latest_posts_success(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_logger,
        mock_write_posts_to_cache,
        mock_return_failed_labels,
        mock_process_batch
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
        
        # Verify result structure
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.inference_type == "perspective_api"
        assert result.inference_timestamp is not None
        assert result.total_classified_posts == 2
        assert result.inference_metadata is not None
        assert result.event is None

    def test_classify_latest_posts_no_posts(
        self,
        mock_determine_backfill,
        mock_get_posts,
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
        
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.total_classified_posts == 0
        assert result.inference_metadata is not None
        mock_logger.warning.assert_called_once()

    def test_classify_latest_posts_with_event(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_logger,
        mock_write_posts_to_cache,
        mock_return_failed_labels,
        mock_process_batch
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
        
        # Verify result structure
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.inference_type == "perspective_api"
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
        mock_logger,
        mock_write_posts_to_cache,
        mock_return_failed_labels,
        mock_process_batch
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
        mock_logger,
        mock_write_posts_to_cache,
        mock_return_failed_labels,
        mock_process_batch
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
        from services.ml_inference.models import ClassificationSessionModel
        assert isinstance(result, ClassificationSessionModel)
        assert result.inference_timestamp is not None 

    def test_multi_step_classification_with_partial_success(
        self,
        mock_determine_backfill,
        mock_get_posts,
        mock_logger,
        mock_write_posts_to_cache,
        mock_return_failed_labels,
        mock_process_batch
    ):
        """Test multi-step classification process with partial successes.
        
        This test verifies the behavior of classify_latest_posts when processing
        a large batch of posts across multiple runs, with different success rates
        in each run. The test follows this sequence:
        
        Initial State:
        - 50 total posts to process
        
        Step 1 (First Run):
        - 20 posts successfully classified
        - 30 posts fail classification
        - Successfully classified posts written to cache
        - Failed posts returned to input queue
        
        Step 2 (Second Run):
        - 18 posts successfully classified
        - 12 posts fail classification
        - Same operations as Step 1
        - Running total: 38 successful, 12 remaining
        
        Step 3 (Final Run):
        - All remaining 12 posts successfully classified
        - Final total: 50 posts successfully classified
        
        For each step, verifies:
        - classify_latest_posts input/output behavior
        - Cache writing operations
        - Failed label handling
        - Metadata accuracy in classify_latest_posts output
        """
        # Setup initial test data
        initial_posts = [
            PostToLabelModel(
                uri=f"test{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i,
                batch_metadata="{}",
            )
            for i in range(50)
        ]
        
        # Step 1: First run (20 success, 30 fail)
        mock_get_posts.return_value = initial_posts
        
        result1 = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7
        )
        
        # Verify first run results
        assert result1.total_classified_posts == 50
        assert result1.inference_metadata["total_posts_successfully_labeled"] == 20
        assert result1.inference_metadata["total_posts_failed_to_label"] == 30
        
        # Verify write_posts_to_cache and return_failed_labels were called appropriately
        mock_write_posts_to_cache.assert_called_once()
        mock_return_failed_labels.assert_called_once()
        
        # Step 2: Second run (18 success, 12 fail)
        remaining_posts = [
            PostToLabelModel(
                uri=f"test{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i + 50,
                batch_metadata="{}",
            )
            for i in range(30)
        ]
        mock_get_posts.return_value = remaining_posts
        
        result2 = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7
        )
        
        # Verify second run results
        assert result2.total_classified_posts == 30
        assert result2.inference_metadata["total_posts_successfully_labeled"] == 18
        assert result2.inference_metadata["total_posts_failed_to_label"] == 12
        
        # Verify write_posts_to_cache and return_failed_labels were called appropriately
        assert mock_write_posts_to_cache.call_count == 2
        assert mock_return_failed_labels.call_count == 2
        
        # Step 3: Final run (12 success, 0 fail)
        final_posts = [
            PostToLabelModel(
                uri=f"test{i}",
                text=f"test post {i}",
                preprocessing_timestamp="2024-01-01-12:00:00",
                batch_id=i + 80,
                batch_metadata="{}",
            )
            for i in range(12)
        ]
        mock_get_posts.return_value = final_posts
        
        result3 = classify_latest_posts(
            backfill_period="days",
            backfill_duration=7
        )
        
        # Verify final run results
        assert result3.total_classified_posts == 12
        assert result3.inference_metadata["total_posts_successfully_labeled"] == 12
        assert result3.inference_metadata["total_posts_failed_to_label"] == 0
        
        # Verify write_posts_to_cache and return_failed_labels were called appropriately
        assert mock_write_posts_to_cache.call_count == 3  # Called for all successful posts
        assert mock_return_failed_labels.call_count == 2  # Not called in final run (no failures)
