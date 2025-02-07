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
)

from services.ml_inference.models import PerspectiveApiLabelsModel
from lib.helper import GOOGLE_API_KEY


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
        
        # Create multiple batches
        batches = [[create_perspective_request(f"text_{i}_{j}") 
                   for j in range(5)] for i in range(3)]
        
        # Process batches concurrently
        tasks = [process_perspective_batch(batch) for batch in batches]
        results = await asyncio.gather(*tasks)
        
        # Verify results
        assert len(results) == 3  # Three batches
        for batch_result in results:
            assert len(batch_result) == 5  # Five items per batch
            for result in batch_result:
                assert isinstance(result, dict)
                assert "prob_toxic" in result
                assert "label_toxic" in result
                assert "prob_reasoning" in result
                assert "label_reasoning" in result
                assert result["prob_constructive"] == result["prob_reasoning"]
                assert result["label_constructive"] == result["label_reasoning"]

        # Verify each batch got its own client request
        assert mock_client.new_batch_http_request.call_count == 3


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
                'created_at': '2024-01-01',  # Add this field
                'batch_id': 'batch1'
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
                'created_at': '2024-01-01',  # Add this field
                'batch_id': 'batch1'
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
                'created_at': '2024-01-01',  # Add this field
                'batch_id': 'batch1'
            },
            {
                'uri': 'test2',
                'text': 'text2',
                'created_at': '2024-01-01',  # Add this field
                'batch_id': 'batch1'
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
        posts = [{'uri': 'test', 'text': 'test', 'created_at': '2024-01-01'}]
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
                'created_at': '2024-01-01',  # Add this field
                'batch_id': 'batch1'
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
        
        # Verify write_posts_to_cache was called with dicts
        mock_write.assert_called()
        called_posts = mock_write.call_args[1]["posts"]
        assert isinstance(called_posts, list)
        assert all(isinstance(post, dict) for post in called_posts)

    @pytest.mark.asyncio
    @patch('ml_tooling.perspective_api.model.process_perspective_batch')
    @patch('ml_tooling.perspective_api.model.return_failed_labels_to_input_queue')
    @patch('ml_tooling.perspective_api.model.write_posts_to_cache')
    async def test_batch_classify_partial_success(self, mock_write, mock_return, mock_process_batch):
        """Test batch classification with mixed success/failure."""
        posts = [
            {'uri': 'test1', 'text': 'text1', 'batch_id': 'batch1', 'created_at': '2024-01-01'},
            {'uri': 'test2', 'text': 'text2', 'batch_id': 'batch1', 'created_at': '2024-01-01'}
        ]
        
        # First post succeeds, second fails
        mock_process_batch.return_value = [{"prob_toxic": 0.8}, None]
        
        metadata = await batch_classify_posts(posts=posts)
        assert metadata["total_posts_successfully_labeled"] == 1
        assert metadata["total_posts_failed_to_label"] == 1
        mock_write.assert_called_once()
        mock_return.assert_called_once()

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
        max_posts = [{'uri': f'test{i}', 'text': f'text{i}', 'batch_id': 'batch1', 'created_at': '2024-01-01'} 
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
    @patch('lib.helper.GOOGLE_API_KEY', 'testing')
    def test_get_google_client(self, mock_discovery):
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
