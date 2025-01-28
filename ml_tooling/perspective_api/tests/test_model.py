"""Tests for the Perspective API model functions."""

import pytest
from unittest.mock import Mock, patch

from ml_tooling.perspective_api.model import (
    process_perspective_batch,
    create_label_models,
    batch_classify_posts,
    run_batch_classification,
)
from services.ml_inference.models import PerspectiveApiLabelsModel


class TestProcessPerspectiveBatch:
    """Tests for process_perspective_batch function.
    
    This class tests the async function that processes batches of requests to the 
    Perspective API. It verifies:
    - Successful processing of valid requests
    - Proper handling of API errors
    - Correct formatting of response data
    - Proper callback behavior
    """

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.get_google_client')
    async def test_process_batch_success(self, mock_get_client):
        """Test successful processing of a batch of requests.
        
        Should return list of classification results with proper prob/label fields.
        """
        # Mock the Google API client and batch request
        mock_client = Mock()
        mock_batch = Mock()
        mock_client.new_batch_http_request.return_value = mock_batch
        mock_get_client.return_value = mock_client

        # Store the callback function when it's passed to add()
        stored_callback = None
        def mock_add(request, callback=None):
            nonlocal stored_callback
            stored_callback = callback
        mock_batch.add.side_effect = mock_add

        # Mock execute to call the stored callback with test responses
        def mock_execute():
            test_response = {
                "attributeScores": {
                    "TOXICITY": {"summaryScore": {"value": 0.8}},
                    "REASONING_EXPERIMENTAL": {"summaryScore": {"value": 0.6}}
                }
            }
            # Call the callback that was stored during add()
            stored_callback("1", test_response, None)
        mock_batch.execute.side_effect = mock_execute

        test_requests = [
            {
                "comment": {"text": "test text"},
                "languages": ["en"],
                "requestedAttributes": {
                    "TOXICITY": {},
                    "REASONING_EXPERIMENTAL": {}  # Need to request REASONING to get constructiveness
                }
            }
        ]
        
        results = await process_perspective_batch(test_requests)
        
        assert isinstance(results, list)
        assert len(results) == 1
        assert "prob_toxic" in results[0]
        assert "label_toxic" in results[0]
        assert "prob_reasoning" in results[0]
        assert "label_reasoning" in results[0]
        # Check that constructiveness equals reasoning
        assert "prob_constructive" in results[0]
        assert "label_constructive" in results[0]
        assert results[0]["prob_constructive"] == results[0]["prob_reasoning"]
        assert results[0]["label_constructive"] == results[0]["label_reasoning"]

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.get_google_client') 
    async def test_process_batch_error(self, mock_get_client):
        """Test handling of API errors in batch processing.
        
        Should return None for failed requests.
        """
        # Mock the Google API client and batch request
        mock_client = Mock()
        mock_batch = Mock()
        mock_client.new_batch_http_request.return_value = mock_batch
        mock_get_client.return_value = mock_client

        # Store the callback function when it's passed to add()
        stored_callback = None
        def mock_add(request, callback=None):
            nonlocal stored_callback
            stored_callback = callback
        mock_batch.add.side_effect = mock_add

        # Mock execute to call the stored callback with error
        def mock_execute():
            stored_callback("1", None, Exception("API Error"))
        mock_batch.execute.side_effect = mock_execute

        test_requests = [
            {
                "comment": {"text": ""}, # Invalid empty text
                "languages": ["en"],
                "requestedAttributes": {"TOXICITY": {}}
            }
        ]
        
        results = await process_perspective_batch(test_requests)
        
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0] is None


class TestCreateLabelModels:
    """Tests for create_label_models function.
    
    This class tests the function that creates PerspectiveApiLabelsModel objects from
    posts and API responses. It verifies:
    - Proper model creation for successful responses
    - Error handling for failed responses
    - Correct timestamp assignment
    - Proper field mapping
    """

    def test_create_models_success(self):
        """Test creation of label models from successful API responses.
        
        Should create properly formatted PerspectiveApiLabelsModel objects.
        """
        posts = [{"uri": "test_uri", "text": "test text", "batch_id": 1}]
        responses = [{
            "prob_toxic": 0.8,
            "label_toxic": 1,
            "prob_reasoning": 0.6,
            "label_reasoning": 1
        }]
        
        models = create_label_models(posts, responses)
        
        assert len(models) == 1
        assert isinstance(models[0], PerspectiveApiLabelsModel)
        assert models[0].uri == "test_uri"
        assert models[0].was_successfully_labeled is True
        assert models[0].prob_toxic == 0.8

    def test_create_models_error(self):
        """Test handling of failed API responses.
        
        Should create models with was_successfully_labeled=False.
        """
        posts = [{"uri": "test_uri", "text": "test text", "batch_id": 1}]
        responses = [{"error": "API Error"}]
        
        models = create_label_models(posts, responses)
        
        assert len(models) == 1
        assert models[0].was_successfully_labeled is False
        assert models[0].reason == "API Error"


class TestBatchClassifyPosts:
    """Tests for batch_classify_posts function.
    
    This class tests the async function that coordinates batch classification of posts.
    It verifies:
    - Proper batching of requests
    - Success/failure counting
    - Delay timing between batches
    - Integration with other components
    """

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.process_perspective_batch')
    @patch('ml_tooling.perspective_api.model.return_failed_labels_to_input_queue')
    @patch('ml_tooling.perspective_api.model.write_posts_to_cache')
    async def test_batch_classify_success(self, mock_write, mock_return, mock_process_batch):
        """Test successful batch classification of posts.
        
        Should return metadata with success counts.
        """
        mock_process_batch.return_value = [{
            "prob_toxic": 0.8,
            "label_toxic": 1,
            "prob_reasoning": 0.6,
            "label_reasoning": 1
        }]

        posts = [
            {"uri": "test1", "text": "text 1", "batch_id": 1},
            {"uri": "test2", "text": "text 2", "batch_id": 2}
        ]
        
        metadata = await batch_classify_posts(
            posts=posts,
            batch_size=1,
            seconds_delay_per_batch=0.1
        )
        
        assert "total_batches" in metadata
        assert metadata["total_batches"] == 2
        assert "total_posts_successfully_labeled" in metadata
        assert "total_posts_failed_to_label" in metadata
        
        # Verify write_posts_to_cache was called with dicts
        mock_write.assert_called()
        called_posts = mock_write.call_args[1]["posts"]
        assert isinstance(called_posts, list)
        assert all(isinstance(post, dict) for post in called_posts)


class TestRunBatchClassification:
    """Tests for run_batch_classification function.
    
    This class tests the synchronous wrapper function for batch classification.
    It verifies:
    - Proper async event loop handling
    - Correct metadata passing
    - Default parameter handling
    """

    @patch('ml_tooling.perspective_api.model.batch_classify_posts')
    def test_run_classification(self, mock_batch_classify):
        """Test synchronous batch classification execution.
        
        Should properly wrap async function and return metadata.
        """
        # Create an async mock that returns the expected data
        async def async_return():
            return {
                "total_batches": 1,
                "total_posts_successfully_labeled": 1,
                "total_posts_failed_to_label": 0
            }
        mock_batch_classify.return_value = async_return()

        posts = [{"uri": "test", "text": "test text", "batch_id": 1}]
        
        metadata = run_batch_classification(
            posts=posts,
            batch_size=1,
            seconds_delay_per_batch=0.1
        )
        
        assert isinstance(metadata, dict)
        assert "total_batches" in metadata
        assert "total_posts_successfully_labeled" in metadata
        assert "total_posts_failed_to_label" in metadata

    @patch('ml_tooling.perspective_api.model.batch_classify_posts')
    def test_run_classification_defaults(self, mock_batch_classify):
        """Test batch classification with default parameters.
        
        Should use default batch size and delay values.
        """
        # Create an async mock that returns the expected data
        async def async_return():
            return {
                "total_batches": 1,
                "total_posts_successfully_labeled": 1,
                "total_posts_failed_to_label": 0
            }
        mock_batch_classify.return_value = async_return()

        posts = [{"uri": "test", "text": "test text", "batch_id": 1}]
        
        metadata = run_batch_classification(posts=posts)
        
        assert isinstance(metadata, dict)
        assert "total_batches" in metadata
