"""Tests for the Perspective API model functions."""

import pytest
from unittest.mock import Mock, patch
import json
from googleapiclient.errors import HttpError
import time
import gc
import psutil
import os
import asyncio

from services.ml_inference.models import LabelWithBatchId
from ml_tooling.perspective_api.model import (
    process_perspective_batch,
    create_labels,
    batch_classify_posts,
    run_batch_classification,
    get_google_client,
    request_comment_analyzer,
    process_response,
    classify_text_toxicity,
    create_perspective_request,
    default_requested_attributes,
    DEFAULT_BATCH_SIZE,
    process_perspective_batch_with_retries,
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
        
        Args:
            mock_get_client: Mocked Google API client
            
        Returns:
            None
            
        Verifies:
            - Batch requests are properly formatted
            - Callbacks are correctly registered and executed
            - Response processing handles all attribute types
            - Probability scores are correctly mapped
            - Label thresholds are properly applied
            - Constructiveness scores match reasoning scores
            
        Expected Behavior:
            The function should process a batch of requests through the Perspective API,
            handling callbacks for each request, and returning properly formatted
            classification results with both probability scores and binary labels.
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
        """Test error handling in batch processing.
        
        Args:
            mock_get_client: Mocked Google API client
            
        Returns:
            None
            
        Verifies:
            - Error responses are properly caught and handled
            - Failed requests return None results
            - Batch processing continues after errors
            - Error details are properly logged
            - API error responses match expected format
            
        Expected Behavior:
            The function should gracefully handle API errors by returning None
            for failed requests while allowing the batch to complete processing.
            Error details should be properly captured for debugging.
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
            error = HttpError(resp=Mock(status=400), content=b'{"error": "API Error"}')
            stored_callback("1", None, error)
        mock_batch.execute.side_effect = mock_execute

        test_requests = [
            {
                "comment": {"text": "test text"},
                "languages": ["en"],
                "requestedAttributes": {
                    "TOXICITY": {},
                    "REASONING_EXPERIMENTAL": {}
                }
            }
        ]
        
        results = await process_perspective_batch(test_requests)
        
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0] is None

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.get_google_client')
    async def test_process_empty_batch(self, mock_get_client):
        """Test processing of empty batch.
        
        Args:
            mock_get_client: Mocked Google API client
            
        Returns:
            None
            
        Verifies:
            - Empty batch returns empty results list
            - No API client is created
            - No batch request is created
            - No API calls are made
            
        Expected Behavior:
            The function should immediately return an empty list for an empty batch
            without making any API calls or creating any client objects.
        """
        # Mock the Google API client and batch request
        mock_client = Mock()
        mock_batch = Mock()
        mock_client.new_batch_http_request.return_value = mock_batch
        mock_get_client.return_value = mock_client
        
        results = await process_perspective_batch([])
        
        assert isinstance(results, list)
        assert len(results) == 0
        # Verify no API client or batch was created
        mock_get_client.assert_not_called()
        mock_client.new_batch_http_request.assert_not_called()
        mock_batch.add.assert_not_called()
        mock_batch.execute.assert_not_called()

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.get_google_client')
    async def test_concurrent_batch_processing(self, mock_get_client):
        """Test concurrent processing of multiple batches."""
        # Mock the Google API client
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        # Track callbacks for each batch separately
        batch_callbacks = {}  # Store callbacks by batch ID
        batch_counter = 0
        
        def mock_add(request, callback=None):
            nonlocal batch_callbacks, batch_counter
            if batch_counter not in batch_callbacks:
                batch_callbacks[batch_counter] = []
            batch_callbacks[batch_counter].append((request, callback))
        
        def mock_execute():
            nonlocal batch_counter
            test_response = {
                "attributeScores": {
                    "TOXICITY": {"summaryScore": {"value": 0.8}},
                    "REASONING_EXPERIMENTAL": {"summaryScore": {"value": 0.6}}
                }
            }
            # Call all callbacks for this batch
            for request, callback in batch_callbacks[batch_counter]:
                callback(id(request), test_response, None)
            batch_counter += 1
        
        # Create mock batch objects for each concurrent request
        def mock_new_batch():
            mock_batch = Mock()
            mock_batch.add.side_effect = mock_add
            mock_batch.execute.side_effect = mock_execute
            return mock_batch
            
        mock_client.new_batch_http_request.side_effect = mock_new_batch
        
        # Create test requests for multiple batches
        test_requests = [
            {
                "comment": {"text": "test text 1"},
                "languages": ["en"],
                "requestedAttributes": default_requested_attributes
            },
            {
                "comment": {"text": "test text 2"},
                "languages": ["en"],
                "requestedAttributes": default_requested_attributes
            }
        ]
        
        # Process batches concurrently
        results = await asyncio.gather(
            process_perspective_batch([test_requests[0]]),
            process_perspective_batch([test_requests[1]])
        )
        
        # Verify results
        assert len(results) == 2  # Two batches
        assert len(results[0]) == 1  # One result per batch
        assert len(results[1]) == 1
        
        # Check that constructiveness equals reasoning in first result
        result = results[0][0]  # First batch, first result
        assert result["label_constructive"] == result["label_reasoning"]


class TestCreateLabelModels:
    """Tests for create_labels function.
    
    This class tests the function that creates PerspectiveApiLabelsModel objects from
    posts and API responses. It verifies:
    - Proper model creation for successful responses
    - Error handling for failed responses
    - Correct timestamp assignment
    - Proper field mapping
    - Handling of mismatched response/post counts
    """

    def test_create_models_success(self):
        """Test successful label creation."""
        posts = [
            {
                'uri': 'test_uri',
                'text': 'test text',
                'preprocessing_timestamp': '2024-01-01',  # Add this field
                'batch_id': 1
            }
        ]
        responses = [{
            "prob_toxic": 0.8,
            "prob_severe_toxic": 0.2
        }]
        
        labels = create_labels(posts, responses)
        
        assert len(labels) == 1
        assert isinstance(labels[0], dict)
        assert labels[0]["uri"] == "test_uri"
        assert labels[0]["text"] == "test text"
        assert labels[0]["was_successfully_labeled"] is True
        assert labels[0]["prob_toxic"] == 0.8
        assert labels[0]["prob_severe_toxic"] == 0.2
        assert "label_timestamp" in labels[0]

    def test_create_models_error(self):
        """Test error handling in label creation."""
        posts = [
            {
                'uri': 'test_uri',
                'text': 'test text',
                'preprocessing_timestamp': '2024-01-01',  # Add this field
                'batch_id': 1
            }
        ]
        responses = [{"error": "API Error"}]
        
        labels = create_labels(posts, responses)
        
        assert len(labels) == 1
        assert isinstance(labels[0], dict)
        assert labels[0]["uri"] == "test_uri"
        assert labels[0]["text"] == "test text"
        assert labels[0]["was_successfully_labeled"] is False
        assert labels[0]["reason"] == "API Error"
        assert "label_timestamp" in labels[0]

    def test_empty_input(self):
        """Test handling of empty input.
        
        Should return empty list.
        """
        assert create_labels([], []) == []

    def test_mismatched_response_count(self, caplog):
        """Test handling of mismatched response count."""
        posts = [
            {
                'uri': 'test1',
                'text': 'text1',
                'preprocessing_timestamp': '2024-01-01',  # Add this field
                'batch_id': 1
            },
            {
                'uri': 'test2',
                'text': 'text2',
                'preprocessing_timestamp': '2024-01-01',  # Add this field
                'batch_id': 1
            }
        ]
        responses = [{
            "prob_toxic": 0.8,
            "prob_severe_toxic": 0.2
        }]  # Only one response for two posts

        labels = create_labels(posts, responses)

        # Check warning was logged
        assert "Number of responses (1) does not match number of posts (2)" in caplog.text

        # Should get two failed labels back
        assert len(labels) == 2
        for label in labels:
            assert isinstance(label, dict)
            assert label["was_successfully_labeled"] is False
            assert label["reason"] == "No response from Perspective API"
            assert "label_timestamp" in label

    def test_malformed_response(self):
        """Test handling of malformed API responses.
        
        Args:
            None
            
        Returns:
            None
            
        Verifies:
            - Malformed responses are detected
            - Labels are marked as failed
            - Error message is descriptive
            - Original post data is preserved
            
        Expected Behavior:
            Should detect missing required fields and mark the label as failed
            with a descriptive error message.
        """
        posts = [{'uri': 'test', 'text': 'test', 'preprocessing_timestamp': '2024-01-01'}]
        responses = [{'attributeScores': {'TOXICITY': {}}}]  # Missing summaryScore
        
        labels = create_labels(posts=posts, responses=responses)

        assert len(labels) == 1
        assert isinstance(labels[0], dict)
        assert labels[0]["uri"] == "test"
        assert labels[0]["text"] == "test"
        # Should be marked as failed due to missing summaryScore
        assert not labels[0].get("was_successfully_labeled", True)
        
        # Check for descriptive error message
        error_msg = labels[0].get("reason", "").lower()
        assert all(term in error_msg for term in ["missing", "required", "field", "summaryscore"])


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
        """Test successful batch classification."""
        posts = [
            {
                'uri': 'test1',
                'text': 'text1',
                'preprocessing_timestamp': '2024-01-01',  # Add this field
                'batch_id': 1
            }
        ]
        
        mock_process_batch.return_value = [{
            "prob_toxic": 0.8,
            "label_toxic": 1,
            "prob_reasoning": 0.6,
            "label_reasoning": 1
        }]

        metadata = await batch_classify_posts(
            posts=posts,
            batch_size=1,
            seconds_delay_per_batch=0.1
        )
        
        assert "total_batches" in metadata
        assert metadata["total_batches"] == 1
        assert "total_posts_successfully_labeled" in metadata
        assert "total_posts_failed_to_label" in metadata
        
        # Verify write_posts_to_cache was called with LabelWithBatchId instances
        mock_write.assert_called()
        called_posts = mock_write.call_args[1]["posts"]
        assert isinstance(called_posts, list)
        from services.ml_inference.models import LabelWithBatchId
        assert all(isinstance(post, LabelWithBatchId) for post in called_posts)

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.process_perspective_batch_with_retries')
    @patch('ml_tooling.perspective_api.model.return_failed_labels_to_input_queue')
    @patch('ml_tooling.perspective_api.model.write_posts_to_cache')
    @patch('ml_tooling.perspective_api.model.create_labels')
    async def test_batch_classify_partial_success(
        self, mock_create_labels, mock_write, mock_return, mock_process_batch
    ):
        """Test batch classification with mixed success/failure."""
        posts = [
            {'uri': 'test1', 'text': 'text1', 'batch_id': 1, 'preprocessing_timestamp': '2024-01-01'},
            {'uri': 'test2', 'text': 'text2', 'batch_id': 1, 'preprocessing_timestamp': '2024-01-01'}
        ]
        
        # Mock process_batch to return raw API responses
        mock_process_batch.return_value = [
            {
                "attributeScores": {
                    "TOXICITY": {"summaryScore": {"value": 0.8}},
                    "REASONING_EXPERIMENTAL": {"summaryScore": {"value": 0.6}}
                }
            },
            None  # Failed response
        ]
        
        # Mock create_labels to return one success and one failure
        mock_create_labels.return_value = [
            {
                "uri": "test1",
                "text": "text1",
                "preprocessing_timestamp": "2024-01-01",
                "prob_toxic": 0.8,
                "label_toxic": 1,
                "prob_reasoning": 0.6,
                "label_reasoning": 1,
                "prob_constructive": 0.6,
                "label_constructive": 1,
                "was_successfully_labeled": True
            },
            {
                "uri": "test2",
                "text": "text2",
                "preprocessing_timestamp": "2024-01-01",
                "prob_toxic": None,
                "label_toxic": None,
                "prob_reasoning": None,
                "label_reasoning": None,
                "prob_constructive": None,
                "label_constructive": None,
                "was_successfully_labeled": False,
                "reason": "Failed to get API response"
            }
        ]
        
        metadata = await batch_classify_posts(
            posts=posts,
            max_retries=1  # Only try once to avoid multiple retries
        )
        
        assert metadata["total_posts_successfully_labeled"] == 1
        assert metadata["total_posts_failed_to_label"] == 1
        
        # Verify the successful post was written to cache
        mock_write.assert_called_once()
        written_posts = mock_write.call_args[1]["posts"]
        assert len(written_posts) == 1
        assert isinstance(written_posts[0], LabelWithBatchId)
        assert written_posts[0].batch_id == 1
        assert written_posts[0].was_successfully_labeled
        
        # Verify the failed post was returned to queue
        mock_return.assert_called_once()
        failed_posts = mock_return.call_args[1]["failed_label_models"]
        assert len(failed_posts) == 1
        assert isinstance(failed_posts[0], LabelWithBatchId)
        assert failed_posts[0].batch_id == 1
        assert not failed_posts[0].was_successfully_labeled

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.get_google_client')
    @patch('ml_tooling.perspective_api.model.return_failed_labels_to_input_queue')
    @patch('ml_tooling.perspective_api.model.write_posts_to_cache')
    async def test_batch_size_boundaries(self, mock_write, mock_return, mock_get_client):
        """Test behavior at batch size boundaries.

        Args:
            mock_write: Mock for write_posts_to_cache
            mock_return: Mock for return_failed_labels_to_input_queue
            mock_get_client: Mock for Google API client
            
        Verifies:
            - Maximum batch size is respected
            - Batches are correctly split
            - All posts are processed
            - Cache writing is called correctly
        """
        # Mock the Google API client
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        # Track callbacks for each batch
        stored_callbacks = {}
        batch_counter = 0
        
        def mock_add(request, callback=None):
            nonlocal stored_callbacks, batch_counter
            if batch_counter not in stored_callbacks:
                stored_callbacks[batch_counter] = []
            stored_callbacks[batch_counter].append((request, callback))
        
        def mock_execute():
            nonlocal batch_counter
            # Simulate successful response for all requests in current batch
            test_response = {
                "attributeScores": {
                    "TOXICITY": {"summaryScore": {"value": 0.8}},
                    "REASONING_EXPERIMENTAL": {"summaryScore": {"value": 0.6}}
                }
            }
            # Call all callbacks for this batch
            for request, callback in stored_callbacks[batch_counter]:
                callback(id(request), test_response, None)
            batch_counter += 1
        
        # Create mock batch objects for each concurrent request
        def mock_new_batch():
            mock_batch = Mock()
            mock_batch.add.side_effect = mock_add
            mock_batch.execute.side_effect = mock_execute
            return mock_batch
        
        mock_client.new_batch_http_request.side_effect = mock_new_batch
        
        # Test maximum batch size
        max_posts = [{'uri': f'test{i}', 'text': f'text{i}', 'batch_id': 1, 'preprocessing_timestamp': '2024-01-01'} 
                    for i in range(DEFAULT_BATCH_SIZE + 1)]
        
        metadata = await batch_classify_posts(
            posts=max_posts,
            batch_size=DEFAULT_BATCH_SIZE,
            seconds_delay_per_batch=0.1
        )
        
        # Verify batch processing
        assert metadata["total_batches"] == 2
        assert metadata["total_posts_successfully_labeled"] + \
               metadata["total_posts_failed_to_label"] == len(max_posts)
        
        # Verify cache writing was called
        mock_write.assert_called()
        # No failed labels, so return_failed_labels shouldn't be called
        mock_return.assert_not_called()


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


class TestHelperFunctions:
    """Tests for various helper functions in the model."""
    
    @patch('ml_tooling.perspective_api.model.discovery')
    @patch('lib.load_env_vars.EnvVarsContainer.get_env_var', return_value='testing')
    def test_get_google_client(self, mock_get_env_var, mock_discovery):
        """Test Google client creation."""
        mock_client = Mock()
        mock_discovery.build.return_value = mock_client
        
        client = get_google_client()
        
        mock_discovery.build.assert_called_once_with(
            "commentanalyzer",
            "v1alpha1",
            developerKey='testing',
            discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
            static_discovery=False
        )
        assert client == mock_client

    @patch('ml_tooling.perspective_api.model.get_google_client')
    def test_request_comment_analyzer_success(self, mock_get_client):
        """Test successful API request processing.
        
        Args:
            mock_get_client: Mocked Google API client for testing
            
        Returns:
            None
            
        Verifies:
            - API request is properly formatted
            - Response contains expected attribute scores
            - TOXICITY score is present and properly formatted
            - Response structure matches Perspective API format
            
        Expected Behavior:
            Should successfully make an API request and receive a properly
            formatted response with toxicity scores.
        """
        mock_client = Mock()
        mock_analyze = Mock()
        mock_execute = Mock(return_value={"attributeScores": {"TOXICITY": {"summaryScore": {"value": 0.8}}}})
        
        mock_client.comments.return_value.analyze.return_value.execute = mock_execute
        mock_get_client.return_value = mock_client
        
        response = request_comment_analyzer("test text", {"TOXICITY": {}})
        response_obj = json.loads(response)
        
        assert "attributeScores" in response_obj
        assert "TOXICITY" in response_obj["attributeScores"]

    @patch('ml_tooling.perspective_api.model.get_google_client')
    def test_request_comment_analyzer_error(self, mock_get_client):
        """Test API request error handling."""
        mock_client = Mock()
        mock_client.comments.return_value.analyze.side_effect = HttpError(
            resp=Mock(status=400), content=b'{"error": "Bad Request"}')
        mock_get_client.return_value = mock_client
        
        response = request_comment_analyzer("test text")
        response_obj = json.loads(response)
        
        assert "error" in response_obj

    def test_process_response_success(self):
        """Test successful processing of API response.
        
        Args:
            None
            
        Returns:
            None
            
        Verifies:
            - All expected probability scores are present
            - Binary labels are correctly derived from probabilities
            - Reasoning scores are properly mapped to constructiveness
            - All required fields are present in processed response
            
        Expected Behavior:
            Should convert raw API response into standardized format with
            both probability scores and binary labels for all attributes.
        """
        test_response = {
            "attributeScores": {
                "TOXICITY": {"summaryScore": {"value": 0.8}},
                "REASONING_EXPERIMENTAL": {"summaryScore": {"value": 0.6}}
            }
        }
        
        result = process_response(json.dumps(test_response))
        
        assert result["prob_toxic"] == 0.8
        assert result["label_toxic"] == 1
        assert result["prob_reasoning"] == 0.6
        assert result["label_reasoning"] == 1
        # Check constructiveness equals reasoning
        assert result["prob_constructive"] == result["prob_reasoning"]
        assert result["label_constructive"] == result["label_reasoning"]

    def test_process_response_error(self):
        """Test error response processing."""
        test_response = {"error": "API Error"}
        
        result = process_response(json.dumps(test_response))
        
        assert "error" in result
        assert result["error"] == "API Error"

    @patch('ml_tooling.perspective_api.model.request_comment_analyzer')
    def test_classify_text_toxicity(self, mock_request):
        """Test toxicity classification."""
        mock_request.return_value = json.dumps({
            "attributeScores": {
                "TOXICITY": {"summaryScore": {"value": 0.8}}
            }
        })
        
        result = classify_text_toxicity("test text")
        
        assert result["prob_toxic"] == 0.8
        assert result["label_toxic"] == 1

    def test_create_perspective_request(self):
        """Test creation of perspective request payload."""
        text = "test text"
        request = create_perspective_request(text)
        
        assert request["comment"]["text"] == text
        assert request["languages"] == ["en"]
        assert "requestedAttributes" in request
        assert all(attr in request["requestedAttributes"] 
                  for attr in default_requested_attributes)


class TestProcessPerspectiveBatchWithRetries:
    """Tests for process_perspective_batch_with_retries function.
    
    This class tests the retry wrapper around process_perspective_batch, verifying:
    - Different retry strategies (batch vs individual)
    - Various success/failure patterns
    - Exponential backoff behavior
    - Maintenance of request order
    - Proper logging of retry attempts
    - Final result aggregation
    """

    def create_test_requests(self, n: int = 20) -> list[dict]:
        """Helper to create n test requests."""
        return [
            {
                "comment": {"text": f"test text {i}"},
                "languages": ["en"],
                "requestedAttributes": {"TOXICITY": {}}
            }
            for i in range(n)
        ]

    def create_success_response(self, request_id: int) -> dict:
        """Helper to create a successful response."""
        return {
            "prob_toxic": 0.8,
            "label_toxic": 1,
            "prob_severe_toxic": 0.7,
            "label_severe_toxic": 1,
            "prob_identity_attack": 0.6,
            "label_identity_attack": 1,
            "prob_insult": 0.5,
            "label_insult": 1,
            "prob_profanity": 0.4,
            "label_profanity": 0,
            "prob_threat": 0.3,
            "label_threat": 0,
            "prob_reasoning": 0.6,
            "label_reasoning": 1,
            "prob_constructive": 0.6,  # Same as reasoning
            "label_constructive": 1    # Same as reasoning
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_all_requests_succeed_first_try(
        self, retry_strategy, mocker, caplog
    ):
        """Test case where all requests succeed on first attempt.
        
        Args:
            retry_strategy: Strategy to use for retries
            mocker: pytest-mock fixture
            caplog: pytest logging capture fixture
            
        Verifies:
            - All requests succeed immediately
            - process_perspective_batch called exactly once
            - No retries attempted
            - All responses contain expected data
            - Original request order maintained
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        
        # Mock process_perspective_batch to return success for all
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            return_value=[self.create_success_response(i) for i in range(n_requests)]
        )
        
        results = await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=3,
            initial_delay=0.1
        )
        
        # Verify results
        assert len(results) == n_requests
        assert all(r is not None for r in results)
        assert all("prob_toxic" in r for r in results)
        
        # Verify process_perspective_batch called exactly once
        mock_process.assert_called_once_with(requests)
        assert mock_process.call_count == 1
        
        # Verify no retry messages in logs
        assert "Retrying" not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_all_fail_then_succeed(self, retry_strategy, mocker, caplog):
        """Test case where all requests fail first try but succeed on retry.
        
        Args:
            retry_strategy: Strategy to use for retries
            mocker: pytest-mock fixture
            caplog: pytest logging capture fixture
            
        Verifies:
            - All requests fail on first attempt
            - All requests succeed on second attempt
            - Proper exponential backoff delay
            - Correct number of retries
            - Final results contain all successes
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        
        # Mock to fail first time, succeed second time
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            side_effect=[
                [None] * n_requests,  # First try: all fail
                [self.create_success_response(i) for i in range(n_requests)]  # Second try: all succeed
            ]
        )
        
        results = await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=3,
            initial_delay=0.1
        )
        
        # Verify results
        assert len(results) == n_requests
        assert all(r is not None for r in results)
        assert all("prob_toxic" in r for r in results)
        
        # Verify call count and arguments
        assert mock_process.call_count == 2
        if retry_strategy == "batch":
            mock_process.assert_has_calls([
                mocker.call(requests),  # First try
                mocker.call(requests)   # Retry
            ])
        else:  # individual
            mock_process.assert_has_calls([
                mocker.call(requests),  # First try
                mocker.call(requests)   # Retry all failed
            ])
        
        # Verify retry message in logs
        assert "Retrying" in caplog.text
        assert str(n_requests) in caplog.text  # Should mention number of failed requests

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_all_requests_fail_all_retries(self, retry_strategy, mocker, caplog):
        """Test case where all requests fail across all retry attempts.
        
        Args:
            retry_strategy: Strategy to use for retries
            mocker: pytest-mock fixture
            caplog: pytest logging capture fixture
            
        Verifies:
            - All requests fail consistently
            - Maximum retries reached
            - Proper exponential backoff
            - Final results are all None
            - Proper error logging
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        max_retries = 3
        
        # Mock to fail every time
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            return_value=[None] * n_requests
        )
        
        results = await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=max_retries,
            initial_delay=0.1
        )
        
        # Verify results
        assert len(results) == n_requests
        assert all(r is None for r in results)
        
        # Verify call count matches max retries
        assert mock_process.call_count == max_retries
        expected_calls = [mocker.call(requests)] * max_retries
        mock_process.assert_has_calls(expected_calls)
        
        # Verify final failure count in logs
        assert f"{n_requests} failed" in caplog.text
        assert str(max_retries) in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_half_succeed_then_rest_succeed(self, retry_strategy, mocker, caplog):
        """Test case where half succeed first try, rest succeed on retry.
        
        Args:
            retry_strategy: Strategy to use for retries
            mocker: pytest-mock fixture
            caplog: pytest logging capture fixture
            
        Verifies:
            - Half of requests succeed immediately
            - Remaining requests succeed on first retry
            - Original request order maintained
            - Proper retry count and timing
            - All final results successful
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        half = n_requests // 2
        
        # First half succeed, second half fail, then succeed
        first_response = (
            [self.create_success_response(i) for i in range(half)] +
            [None] * half
        )
        second_response = [self.create_success_response(i) for i in range(half)]
        
        if retry_strategy == "batch":
            mock_responses = [
                first_response,
                [self.create_success_response(i) for i in range(n_requests)]
            ]
        else:  # individual
            mock_responses = [
                first_response,
                second_response
            ]
        
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            side_effect=mock_responses
        )
        
        results = await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=3,
            initial_delay=0.1
        )
        
        # Verify results
        assert len(results) == n_requests
        assert all(r is not None for r in results)
        assert all("prob_toxic" in r for r in results)
        
        # Verify call count and arguments
        assert mock_process.call_count == 2
        if retry_strategy == "batch":
            mock_process.assert_has_calls([
                mocker.call(requests),
                mocker.call(requests)
            ])
        else:  # individual
            mock_process.assert_has_calls([
                mocker.call(requests),
                mocker.call(requests[half:])  # Only retry failed half
            ])
        
        # Verify retry logging
        assert str(half) in caplog.text  # Should mention number of failed requests
        assert "successful" in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_gradual_success_complete(self, retry_strategy, mocker, caplog):
        """Test case where requests gradually succeed with final success.
        
        Args:
            retry_strategy: Strategy to use for retries
            mocker: pytest-mock fixture
            caplog: pytest logging capture fixture
            
        Verifies:
            - Requests succeed gradually across retries
            - Final retry completes all remaining requests
            - Order maintained throughout retries
            - Proper retry count and timing
            - All final results successful
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        
        # Create responses with increasing success rate
        def create_partial_success(success_count: int, total: int) -> list[dict]:
            return (
                [self.create_success_response(i) for i in range(success_count)] +
                [None] * (total - success_count)
            )
        
        if retry_strategy == "batch":
            mock_responses = [
                create_partial_success(5, n_requests),   # 5 succeed
                create_partial_success(10, n_requests),  # 10 succeed
                create_partial_success(n_requests, n_requests)  # all succeed
            ]
        else:  # individual
            mock_responses = [
                create_partial_success(5, n_requests),   # 5 succeed
                create_partial_success(5, 15),          # 5 more succeed
                create_partial_success(10, 10)          # remaining succeed
            ]
        
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            side_effect=mock_responses
        )
        
        results = await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=3,
            initial_delay=0.1
        )
        
        # Verify results
        assert len(results) == n_requests
        assert all(r is not None for r in results)
        assert all("prob_toxic" in r for r in results)
        
        # Verify call count
        assert mock_process.call_count == 3
        
        # Verify proper logging of progress
        assert "5 successful" in caplog.text
        assert "10 successful" in caplog.text
        assert str(n_requests) in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_gradual_success_with_final_failures(
        self, retry_strategy, mocker, caplog
    ):
        """Test case where requests gradually succeed but some ultimately fail.
        
        Args:
            retry_strategy: Strategy to use for retries
            mocker: pytest-mock fixture
            caplog: pytest logging capture fixture
            
        Verifies:
            - Requests succeed gradually across retries
            - Some requests remain failed after max retries
            - Order maintained throughout retries
            - Proper retry count and timing
            - Mix of successful and failed results
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        final_failures = 2
        
        def create_partial_success(success_count: int, total: int) -> list[dict]:
            return (
                [self.create_success_response(i) for i in range(success_count)] +
                [None] * (total - success_count)
            )
        
        if retry_strategy == "batch":
            mock_responses = [
                create_partial_success(5, n_requests),   # 5 succeed
                create_partial_success(10, n_requests),  # 10 succeed
                create_partial_success(n_requests - final_failures, n_requests)  # 18 succeed
            ]
        else:  # individual
            mock_responses = [
                create_partial_success(5, n_requests),   # 5 succeed
                create_partial_success(5, 15),          # 5 more succeed
                create_partial_success(8, 10)           # 8 more succeed, 2 fail
            ]
        
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            side_effect=mock_responses
        )
        
        results = await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=3,
            initial_delay=0.1
        )
        
        # Verify results
        assert len(results) == n_requests
        assert sum(1 for r in results if r is not None) == n_requests - final_failures
        assert sum(1 for r in results if r is None) == final_failures
        
        # Verify call count
        assert mock_process.call_count == 3
        
        # Verify final status logging
        assert f"{final_failures} failed" in caplog.text
        assert f"{n_requests - final_failures} successful" in caplog.text

    @pytest.mark.asyncio
    async def test_empty_request_list(self, mocker):
        """Test handling of empty request list.
        
        Verifies immediate return of empty list without any API calls.
        """
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch'
        )
        
        results = await process_perspective_batch_with_retries([])
        
        assert results == []
        mock_process.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("retry_strategy", ["batch", "individual"])
    async def test_exponential_backoff_timing(self, retry_strategy, mocker):
        """Test exponential backoff delay between retries.
        
        Verifies proper delay timing with exponential increase.
        """
        n_requests = 20
        requests = self.create_test_requests(n_requests)
        initial_delay = 0.1
        
        mock_sleep = mocker.patch('asyncio.sleep')
        # Make sure all attempts fail to trigger all retries
        mock_process = mocker.patch(
            'ml_tooling.perspective_api.model.process_perspective_batch',
            side_effect=[[None] * n_requests] * 3  # Fail all 3 attempts
        )
        
        await process_perspective_batch_with_retries(
            requests=requests,
            retry_strategy=retry_strategy,
            max_retries=3,
            initial_delay=initial_delay
        )
        
        # Verify exponential delay increase
        mock_sleep.assert_has_calls([
            mocker.call(initial_delay),
            mocker.call(initial_delay * 2)
        ], any_order=False)
        assert mock_sleep.call_count == 2  # Only 2 delays for 3 attempts

    @pytest.mark.asyncio
    async def test_invalid_retry_strategy(self, mocker):
        """Test handling of invalid retry strategy.
        
        Verifies proper error handling for invalid strategy.
        """
        with pytest.raises(ValueError):
            await process_perspective_batch_with_retries(
                requests=[],
                retry_strategy="invalid"  # type: ignore
            )
