"""Tests for the model.py module."""

import json
from unittest.mock import patch, Mock, call, MagicMock
from datetime import datetime, timezone
import time

import pytest

from services.ml_inference.sociopolitical.model import (
    generate_prompt,
    parse_llm_result,
    process_sociopolitical_batch,
    create_labels,
    batch_classify_posts,
    run_batch_classification,
    process_sociopolitical_batch_with_retries,
)
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel,
    LLMSociopoliticalLabelsModel,
    PostToLabelModel,
)


def _make_post(
    *,
    uri: str = "test_uri",
    text: str = "test post",
    preprocessing_timestamp: str = "2024-01-01-00:00:00",
    batch_id: int = 1,
    batch_metadata: str = "{}",
) -> PostToLabelModel:
    """Create a PostToLabelModel for sociopolitical unit tests."""
    return PostToLabelModel(
        uri=uri,
        text=text,
        preprocessing_timestamp=preprocessing_timestamp,
        batch_id=batch_id,
        batch_metadata=batch_metadata,
    )


class TestGeneratePrompt:
    """Tests for generate_prompt() function."""

    def test_generate_prompt_single_post(self):
        """Test prompt generation for a single post.
        
        Should properly format the post with enumeration and newlines.
        """
        posts = [_make_post(text="Test post")]
        prompt = generate_prompt(posts)
        assert "1. Test post\n" in prompt

    def test_generate_prompt_multiple_posts(self):
        """Test prompt generation for multiple posts.
        
        Should properly format multiple posts with enumeration and newlines.
        """
        posts = [
            _make_post(uri="uri_1", text="First post", batch_id=1),
            _make_post(uri="uri_2", text="Second post", batch_id=2),
        ]
        prompt = generate_prompt(posts)
        assert "1. First post\n" in prompt
        assert "2. Second post\n" in prompt

    def test_generate_prompt_with_empty_text(self):
        """Test handling of posts with empty text field.
        
        Should handle empty strings without error and strip whitespace.
        """
        posts = [_make_post(text="")]
        prompt = generate_prompt(posts)
        assert "1. \n" in prompt

    def test_generate_prompt_with_whitespace_text(self):
        """Test handling of posts with whitespace-only text.
        
        Should properly strip whitespace from text.
        """
        posts = [_make_post(text="  leading and trailing spaces  ")]
        prompt = generate_prompt(posts)
        assert "1. leading and trailing spaces\n" in prompt

    def test_generate_prompt_with_special_characters(self):
        """Test handling of posts with special characters.
        
        Should properly handle newlines, quotes, and other special characters.
        """
        posts = [
            _make_post(
                text="Post with\nnewline and \"quotes\" and other $pecial ch@racters!"
            )
        ]
        prompt = generate_prompt(posts)
        assert "1. Post with\nnewline and \"quotes\" and other $pecial ch@racters!\n" in prompt


class TestParseLLMResult:
    """Tests for parse_llm_result() function."""

    def test_parse_valid_result(self):
        """Test parsing a valid LLM result.
        
        Should properly parse JSON and return list of label models.
        """
        json_str = json.dumps({
            "labels": [
                {
                    "is_sociopolitical": True,
                    "political_ideology_label": "left"
                }
            ]
        })
        result = parse_llm_result(json_str, expected_number_of_posts=1)
        assert len(result) == 1
        assert isinstance(result[0], LLMSociopoliticalLabelModel)

    def test_parse_invalid_count(self):
        """Test parsing result with wrong number of labels.
        
        Should raise ValueError if count doesn't match expected.
        """
        json_str = json.dumps({
            "labels": [
                {
                    "is_sociopolitical": True,
                    "political_ideology_label": "left"
                }
            ]
        })
        with pytest.raises(ValueError):
            parse_llm_result(json_str, expected_number_of_posts=2)

    def test_parse_invalid_json_format(self):
        """Test handling of malformed JSON input.
        
        Should raise JSONDecodeError for malformed JSON.
        """
        invalid_json = "{ this is not valid json }"
        with pytest.raises(json.JSONDecodeError):
            parse_llm_result(invalid_json, expected_number_of_posts=1)

    def test_parse_missing_labels_field(self):
        """Test handling of JSON missing required fields.
        
        Should raise ValidationError when 'labels' field is missing.
        """
        json_str = json.dumps({
            "other_field": "some value"
        })
        with pytest.raises(ValueError):
            parse_llm_result(json_str, expected_number_of_posts=1)

    def test_parse_empty_labels_list(self):
        """Test handling of empty labels list.
        
        Should raise ValueError when labels list is empty but expecting posts.
        """
        json_str = json.dumps({
            "labels": []
        })
        with pytest.raises(ValueError):
            parse_llm_result(json_str, expected_number_of_posts=1)


class TestProcessSociopoliticalBatch:
    """Tests for process_sociopolitical_batch() function."""

    @patch('services.ml_inference.sociopolitical.model.get_llm_service')
    def test_batch_size_boundaries(self, mock_get_service):
        """Test processing with different batch sizes.
        
        Should properly handle various batch sizes including edge cases.
        """
        # Test with exactly DEFAULT_MINIBATCH_SIZE posts
        posts = [
            _make_post(uri=f"uri_{i}", text=f"post{i}", batch_id=1) for i in range(10)
        ]  # DEFAULT_MINIBATCH_SIZE is 10
        mock_labels = [
            LLMSociopoliticalLabelModel(
                is_sociopolitical=True,
                political_ideology_label="left"
            )
        ] * 10
        mock_result = LLMSociopoliticalLabelsModel(labels=mock_labels)
        mock_service = Mock()
        mock_service.structured_batch_completion.return_value = [mock_result]
        mock_get_service.return_value = mock_service
        
        results = process_sociopolitical_batch(posts)
        assert len(results) == 10
        assert all(isinstance(r, dict) for r in results)

    @patch('services.ml_inference.sociopolitical.model.get_llm_service')
    def test_large_batch_handling(self, mock_get_service):
        """Test processing with batch larger than DEFAULT_MINIBATCH_SIZE.
        
        Should properly split into mini-batches and combine results.
        """
        # Test with 2.5 times DEFAULT_MINIBATCH_SIZE
        posts = [
            _make_post(uri=f"uri_{i}", text=f"post{i}", batch_id=1) for i in range(25)
        ]
        mock_results = [
            LLMSociopoliticalLabelsModel(
                labels=[
                    LLMSociopoliticalLabelModel(
                        is_sociopolitical=True,
                        political_ideology_label="left"
                    )
                ] * 10
            ),
            LLMSociopoliticalLabelsModel(
                labels=[
                    LLMSociopoliticalLabelModel(
                        is_sociopolitical=True,
                        political_ideology_label="left"
                    )
                ] * 10
            ),
            LLMSociopoliticalLabelsModel(
                labels=[
                    LLMSociopoliticalLabelModel(
                        is_sociopolitical=True,
                        political_ideology_label="left"
                    )
                ] * 5
            )
        ]
        mock_service = Mock()
        mock_service.structured_batch_completion.return_value = mock_results
        mock_get_service.return_value = mock_service
        
        results = process_sociopolitical_batch(posts)
        assert len(results) == 25
        assert all(isinstance(r, dict) for r in results)

    @patch('services.ml_inference.sociopolitical.model.get_llm_service')
    def test_single_post_batch(self, mock_get_service):
        """Test processing with single-post batch.
        
        Should handle single post batches efficiently.
        """
        posts = [_make_post(uri="single_uri", text="single post", batch_id=1)]
        mock_result = LLMSociopoliticalLabelsModel(
            labels=[
                LLMSociopoliticalLabelModel(
                    is_sociopolitical=True,
                    political_ideology_label="left"
                )
            ]
        )
        mock_service = Mock()
        mock_service.structured_batch_completion.return_value = [mock_result]
        mock_get_service.return_value = mock_service
        
        results = process_sociopolitical_batch(posts)
        assert len(results) == 1
        assert isinstance(results[0], dict)


class TestCreateLabels:
    """Tests for create_labels() function."""

    def test_create_labels_successful(self):
        """Test creation of labels from successful responses.
        
        Should properly create label models with success status.
        """
        posts = [
            _make_post(
                uri="test",
                text="test post",
                preprocessing_timestamp="2023-01-01T00:00:00Z",
                batch_id=1,
            )
        ]
        responses = [{
            "is_sociopolitical": True,
            "political_ideology_label": "left"
        }]
        
        labels = create_labels(posts, responses)
        assert len(labels) == 1
        assert labels[0]["was_successfully_labeled"] is True

    def test_create_labels_empty(self):
        """Test handling of empty post list.
        
        Should return empty list when no posts provided.
        """
        labels = create_labels([], [])
        assert labels == []

    def test_create_labels_failed(self):
        """Test creation of labels from failed responses.
        
        Should properly create label models with failed status.
        """
        posts = [
            _make_post(
                uri="test1",
                text="test post",
                preprocessing_timestamp="2023-01-01T00:00:00Z",
                batch_id=1,
            )
        ]
        responses = [None]  # None represents failed response
        
        labels = create_labels(posts, responses)
        assert len(labels) == 1
        assert labels[0]["was_successfully_labeled"] is False

    def test_create_labels_mismatched_lengths(self):
        """Test handling of mismatched post and response lengths.
        
        When there are fewer responses than posts, all posts should be marked as failed
        since we can't guarantee which posts correspond to which responses.
        """
        posts = [
            _make_post(
                uri="test1",
                text="post1",
                preprocessing_timestamp="2023-01-01T00:00:00Z",
                batch_id=1,
            ),
            _make_post(
                uri="test2",
                text="post2",
                preprocessing_timestamp="2023-01-01T00:00:00Z",
                batch_id=1,
            ),
        ]
        responses = [{
            "is_sociopolitical": True,
            "political_ideology_label": "left"
        }]
        
        labels = create_labels(posts, responses)
        assert len(labels) == 2
        # When lengths mismatch, all posts are marked as failed for safety
        assert labels[0]["was_successfully_labeled"] is False
        assert labels[1]["was_successfully_labeled"] is False

    def test_create_labels_mixed(self):
        """Test creation of labels from mixed successful and failed responses.
        
        Should properly handle both successful and failed responses in same batch.
        """
        posts = [
            {
                "uri": "test1",
                "text": "post1",
                "preprocessing_timestamp": "2023-01-01T00:00:00Z"
            },
            {
                "uri": "test2",
                "text": "post2", 
                "preprocessing_timestamp": "2023-01-01T00:00:00Z"
            },
            {
                "uri": "test3",
                "text": "post3",
                "preprocessing_timestamp": "2023-01-01T00:00:00Z"
            }
        ]
        responses = [
            {
                "is_sociopolitical": True,
                "political_ideology_label": "left"
            },
            None,  # Failed response
            {
                "is_sociopolitical": False,
                "political_ideology_label": None
            }
        ]
        
        labels = create_labels(posts, responses)
        assert len(labels) == 3
        assert labels[0]["was_successfully_labeled"] is True
        assert labels[1]["was_successfully_labeled"] is False
        assert labels[2]["was_successfully_labeled"] is True


class TestBatchClassifyPosts:
    """Tests for batch_classify_posts() function."""

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch_with_retries')
    @patch('services.ml_inference.sociopolitical.model.write_posts_to_cache')
    @patch('services.ml_inference.sociopolitical.model.return_failed_labels_to_input_queue')
    def test_successful_batch_classification(
        self, 
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test successful batch classification.
        
        Should process all posts and return correct metadata.
        """
        posts = [
            {
                "uri": "test1",
                "text": "post1",
                "batch_id": 1,
                "preprocessing_timestamp": "2024-01-01-00:00:00"
            },
            {
                "uri": "test2",
                "text": "post2",
                "batch_id": 1,
                "preprocessing_timestamp": "2024-01-01-00:00:00"
            }
        ]
        
        mock_process_batch.return_value = [
            {"is_sociopolitical": True, "political_ideology_label": "left"},
            {"is_sociopolitical": False, "political_ideology_label": "unclear"}
        ]
        
        result = batch_classify_posts(posts, batch_size=2)
        
        assert result["total_posts_successfully_labeled"] == 2
        assert result["total_posts_failed_to_label"] == 0
        mock_write_cache.assert_called_once()
        mock_return_failed.assert_not_called()

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch_with_retries')
    @patch('services.ml_inference.sociopolitical.model.write_posts_to_cache')
    @patch('services.ml_inference.sociopolitical.model.return_failed_labels_to_input_queue')
    def test_failed_batch_classification(
        self,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test failed batch classification.
        
        Should properly handle failed classifications and return correct metadata.
        """
        posts = [
            {
                "uri": "test1",
                "text": "post1",
                "batch_id": 1,
                "preprocessing_timestamp": "2024-01-01-00:00:00"
            },
            {
                "uri": "test2",
                "text": "post2",
                "batch_id": 1,
                "preprocessing_timestamp": "2024-01-01-00:00:00"
            }
        ]
        
        mock_process_batch.return_value = [None, None]  # None represents failed classifications
        
        result = batch_classify_posts(posts, batch_size=2)
        
        assert result["total_posts_successfully_labeled"] == 0
        assert result["total_posts_failed_to_label"] == 2
        mock_write_cache.assert_not_called()
        mock_return_failed.assert_called_once()

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch_with_retries')
    @patch('services.ml_inference.sociopolitical.model.write_posts_to_cache')
    @patch('services.ml_inference.sociopolitical.model.return_failed_labels_to_input_queue')
    def test_mixed_batch_classification(
        self,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test mixed successful and failed batch classifications.
        
        Should properly handle both successful and failed batches.
        """
        # Create 25 posts with batch size of 5
        posts = [
            _make_post(
                uri=f"test{i}",
                text=f"post{i}",
                batch_id=i // 5,
                preprocessing_timestamp="2024-01-01-00:00:00",
            )
            for i in range(25)
        ]
        
        # Mock responses for 5 batches - 3 successful, 2 failed
        successful_batch = [
            {"is_sociopolitical": True, "political_ideology_label": "left"},
            {"is_sociopolitical": False, "political_ideology_label": "unclear"},
            {"is_sociopolitical": True, "political_ideology_label": "right"},
            {"is_sociopolitical": True, "political_ideology_label": "left"},
            {"is_sociopolitical": False, "political_ideology_label": "unclear"}
        ]
        failed_batch = [None, None, None, None, None]
        
        mock_process_batch.side_effect = [
            successful_batch,  # Batch 1 succeeds
            failed_batch,     # Batch 2 fails
            successful_batch, # Batch 3 succeeds
            failed_batch,     # Batch 4 fails
            successful_batch  # Batch 5 succeeds
        ]
        
        result = batch_classify_posts(posts, batch_size=5)
        
        assert result["total_posts_successfully_labeled"] == 15  # 3 successful batches * 5 posts
        assert result["total_posts_failed_to_label"] == 10      # 2 failed batches * 5 posts
        assert mock_write_cache.call_count == 3  # Called for each successful batch
        assert mock_return_failed.call_count == 2  # Called for each failed batch


class TestRunBatchClassification:
    """Tests for run_batch_classification() function."""

    @patch('services.ml_inference.sociopolitical.model.batch_classify_posts')
    def test_run_classification(self, mock_batch_classify):
        """Test running batch classification.
        
        Should properly call batch_classify_posts and return metadata.
        """
        posts = [_make_post(uri="test", text="post", batch_id=1)]
        expected_result = {
            "total_batches": 1,
            "total_posts_successfully_labeled": 1,
            "total_posts_failed_to_label": 0
        }
        mock_batch_classify.return_value = expected_result
        
        result = run_batch_classification(posts)
        assert result == expected_result
        mock_batch_classify.assert_called_once_with(posts=posts, batch_size=100)


@pytest.fixture
def sample_posts():
    """Creates a list of sample posts for testing."""
    return [
        _make_post(uri=f"test_uri_{i}", text=f"Test post {i}", batch_id=i // 10)
        for i in range(20)
    ]


def create_mock_result(success: bool = True) -> dict | None:
    """Helper to create a mock result dictionary."""
    if success:
        return {
            "is_sociopolitical": True,
            "political_ideology_label": "left"
        }
    return None


@patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
@patch('time.sleep')  # Mock sleep to speed up tests
def test_classification_passes_first_try(mock_sleep, mock_process_batch, sample_posts):
    """Test case where all posts are successfully classified on the first attempt."""
    # Setup mock to return successful results for all posts
    mock_results = [create_mock_result(True) for _ in range(20)]
    mock_process_batch.return_value = mock_results

    # Run the function
    results = process_sociopolitical_batch_with_retries(sample_posts)

    # Verify process_batch was called exactly once with correct args
    mock_process_batch.assert_called_once_with(sample_posts)
    
    # Verify sleep was not called (no retries needed)
    mock_sleep.assert_not_called()
    
    # Verify results
    assert len(results) == 20
    assert all(isinstance(r, dict) for r in results)
    assert all(r.get('is_sociopolitical') is True for r in results)


@patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
@patch('time.sleep')
def test_classification_passes_after_two_tries(mock_sleep, mock_process_batch, sample_posts):
    """Test case where half posts pass on first try, rest on second try."""
    # First call: half success, half failure
    first_results = ([create_mock_result(True)] * 10) + ([None] * 10)
    # Second call: all remaining succeed
    second_results = [create_mock_result(True)] * 10
    
    mock_process_batch.side_effect = [first_results, second_results]

    results = process_sociopolitical_batch_with_retries(sample_posts)

    # Verify process_batch calls
    assert mock_process_batch.call_count == 2
    mock_process_batch.assert_has_calls([
        call(sample_posts),  # First call with all posts
        call(sample_posts[10:])  # Second call with failed posts
    ])

    # Verify sleep was called once with correct delay
    mock_sleep.assert_called_once_with(1.0)  # initial_delay * (2 ** 0)

    # Verify results
    assert len(results) == 20
    assert all(isinstance(r, dict) for r in results)
    assert all(r.get('is_sociopolitical') is True for r in results)


@patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
@patch('time.sleep')
def test_classification_fails_all_retries(mock_sleep, mock_process_batch, sample_posts):
    """Test case where all posts fail across all retries."""
    # All posts fail in each try
    mock_process_batch.side_effect = [[None] * 20] * 5  # Initial + 4 retries

    results = process_sociopolitical_batch_with_retries(sample_posts)

    # Verify process_batch was called max_retries times
    assert mock_process_batch.call_count == 5  # Initial + 4 retries
    mock_process_batch.assert_has_calls([call(sample_posts)] * 5)

    # Verify sleep was called with exponential backoff
    mock_sleep.assert_has_calls([
        call(1.0),   # initial_delay * (2 ** 0)
        call(2.0),   # initial_delay * (2 ** 1)
        call(4.0),   # initial_delay * (2 ** 2)
        call(8.0)    # initial_delay * (2 ** 3)
    ])

    # Verify results (all should be None after failure)
    assert len(results) == 20
    assert all(r is None for r in results)


@patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
@patch('time.sleep')
def test_classification_partial_success_after_retries(mock_sleep, mock_process_batch, sample_posts):
    """Test case where some posts succeed and 3 ultimately fail after all retries."""
    # Sequence of results where eventually 3 posts fail
    mock_process_batch.side_effect = [
        ([create_mock_result(True)] * 5) + ([None] * 15),    # 5 succeed
        ([create_mock_result(True)] * 7) + ([None] * 8),     # 7 more succeed
        ([create_mock_result(True)] * 5) + ([None] * 3),     # 5 more succeed
        [None] * 3,                                          # 3 fail in final try
        [None] * 3                                           # Extra value for safety
    ]

    results = process_sociopolitical_batch_with_retries(sample_posts)

    # Verify process_batch calls with correct remaining posts
    assert mock_process_batch.call_count == 5  # Initial + 4 retries
    mock_process_batch.assert_has_calls([
        call(sample_posts),          # 20 posts
        call(sample_posts[5:]),      # 15 posts
        call(sample_posts[12:]),     # 8 posts
        call(sample_posts[17:]),     # 3 posts
        call(sample_posts[17:])      # Final retry with 3 posts
    ])

    # Verify sleep calls with exponential backoff
    mock_sleep.assert_has_calls([
        call(1.0),   # initial_delay * (2 ** 0)
        call(2.0),   # initial_delay * (2 ** 1)
        call(4.0),   # initial_delay * (2 ** 2)
        call(8.0)    # initial_delay * (2 ** 3)
    ])

    # Verify results
    assert len(results) == 20
    successful_results = sum(1 for r in results if r is not None)
    failed_results = sum(1 for r in results if r is None)
    assert successful_results == 17  # 17 succeeded
    assert failed_results == 3      # 3 failed


class TestProcessSociopoliticalBatchWithRetries:
    """Tests for process_sociopolitical_batch_with_retries() function."""

    def test_invalid_input_missing_text(self, sample_posts):
        """Test that dict inputs are rejected.

        The public interface expects PostToLabelModel. Passing dicts should fail fast.
        """
        invalid_posts = [{"uri": "test1"}, {"uri": "test2", "text": "valid post"}]

        with pytest.raises(AttributeError):
            process_sociopolitical_batch_with_retries(invalid_posts)  # type: ignore[arg-type]

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_custom_retry_parameters(self, mock_sleep, mock_process_batch):
        """Test custom max_retries and initial_delay parameters.
        
        Should respect custom retry parameters and follow exponential backoff pattern.
        """
        posts = [_make_post(uri="test1", text="test", batch_id=1)]
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [None],  # Second attempt fails
            [None]   # Extra value for safety
        ]
        max_retries = 2
        initial_delay = 2.0
        
        results = process_sociopolitical_batch_with_retries(
            posts=posts,
            max_retries=max_retries,
            initial_delay=initial_delay
        )
        
        # Should try initial + max_retries times
        assert mock_process_batch.call_count == 3
        
        # Verify exponential backoff timing
        mock_sleep.assert_has_calls([
            call(2.0),  # initial_delay * (2 ** 0)
            call(4.0)   # initial_delay * (2 ** 1)
        ])

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_zero_retries(self, mock_sleep, mock_process_batch):
        """Test behavior when max_retries is set to 0.
        
        Should attempt classification exactly once with no retries or sleep calls.
        """
        posts = [_make_post(uri="test1", text="test", batch_id=1)]
        mock_process_batch.return_value = [None]  # First attempt fails
        
        results = process_sociopolitical_batch_with_retries(posts=posts, max_retries=0)
        
        # Should try exactly once
        mock_process_batch.assert_called_once_with(posts)
        mock_sleep.assert_not_called()
        assert len(results) == 1
        assert results[0] is None

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_negative_retries(self, mock_sleep, mock_process_batch):
        """Test behavior when max_retries is negative.
        
        Should handle negative max_retries same as zero retries - one attempt, no sleep.
        """
        posts = [_make_post(uri="test1", text="test", batch_id=1)]
        mock_process_batch.return_value = [None]  # First attempt fails
        
        results = process_sociopolitical_batch_with_retries(posts=posts, max_retries=-1)
        
        # Should try exactly once
        mock_process_batch.assert_called_once_with(posts)
        mock_sleep.assert_not_called()
        assert len(results) == 1
        assert results[0] is None

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_negative_initial_delay(self, mock_sleep, mock_process_batch):
        """Test behavior when initial_delay is negative.
        
        Should use absolute value of initial_delay for sleep calculations.
        """
        posts = [_make_post(uri="test1", text="test", batch_id=1)]
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [{"result": "success"}]  # Second attempt succeeds
        ]
        initial_delay = -1.0
        
        results = process_sociopolitical_batch_with_retries(
            posts=posts,
            max_retries=2,
            initial_delay=initial_delay
        )
        
        # Should use absolute value for delay calculations
        mock_sleep.assert_called_once_with(abs(initial_delay) * (2 ** 0))  # 1.0 seconds
        assert len(results) == 1
        assert results[0]["result"] == "success"

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    def test_empty_posts_list(self, mock_process_batch):
        """Test behavior with empty posts list.
        
        Should return empty list immediately without calling process_batch.
        """
        results = process_sociopolitical_batch_with_retries([])
        
        mock_process_batch.assert_not_called()
        assert results == []

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_partial_success_result_order(self, mock_sleep, mock_process_batch):
        """Test that result order is preserved with partial successes.
        
        Should maintain original post order in results regardless of when each post succeeds.
        """
        posts = [
            _make_post(uri=f"test{i}", text=f"test{i}", batch_id=1) for i in range(3)
        ]
        
        # First try: first post succeeds
        # Second try: last post succeeds
        # Third try: middle post succeeds
        mock_process_batch.side_effect = [
            [{"result": "first"}, None, None],
            [None, {"result": "last"}],
            [{"result": "middle"}]
        ]
        
        results = process_sociopolitical_batch_with_retries(posts)
        
        # Verify results maintain original order
        assert results[0]["result"] == "first"
        assert results[1]["result"] == "middle"
        assert results[2]["result"] == "last"

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_exact_retry_timing(self, mock_sleep, mock_process_batch):
        """Test exact timing of retries with exponential backoff.
        
        Should follow precise exponential backoff pattern with provided initial_delay.
        """
        posts = [_make_post(uri="test1", text="test", batch_id=1)]
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [{"result": "success"}],  # Second attempt succeeds
            [None]   # Extra value for safety
        ]
        initial_delay = 0.5
        max_retries = 1
        
        results = process_sociopolitical_batch_with_retries(
            posts=posts,
            max_retries=max_retries,
            initial_delay=initial_delay
        )
        
        # Verify exact exponential backoff sequence
        assert mock_process_batch.call_count == 2  # Initial attempt + 1 retry
        mock_sleep.assert_called_once_with(initial_delay * (2 ** 0))  # 0.5 seconds
        assert results[0]["result"] == "success"

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_retry_sleep_timing(self, mock_sleep, mock_process_batch):
        """Test that sleep is called before each retry attempt."""
        posts = [_make_post(uri="test1", text="test", batch_id=1)]
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [None],  # Second attempt fails
            [None]   # Extra value for safety
        ]
        max_retries = 2
        
        process_sociopolitical_batch_with_retries(
            posts=posts,
            max_retries=max_retries,
            initial_delay=1.0
        )
        
        # Should sleep before each retry
        assert mock_sleep.call_count == 2
        mock_sleep.assert_has_calls([
            call(1.0),  # initial_delay * (2 ** 0)
            call(2.0)   # initial_delay * (2 ** 1)
        ])

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_output_order_with_mixed_retries(self, mock_sleep, mock_process_batch):
        """Test that output order matches input order when posts succeed at different retry attempts.
        
        This test verifies that even when different posts succeed at different retry attempts,
        the final output maintains the same order as the input posts.
        """
        # Create posts with distinct text and URIs to track order
        posts = [
            _make_post(uri=f"uri{i}", text=f"test{i}", batch_id=1) for i in range(5)
        ]
        
        # Mock responses where posts succeed at different retry attempts:
        # - Posts 0 and 2 succeed on first try
        # - Post 4 succeeds on second try
        # - Posts 1 and 3 succeed on third try
        mock_process_batch.side_effect = [
            [
                {"result": "success-0", "uri": "uri0"},
                None,
                {"result": "success-2", "uri": "uri2"},
                None,
                None
            ],
            [
                None,
                None,
                {"result": "success-4", "uri": "uri4"}
            ],
            [
                {"result": "success-1", "uri": "uri1"},
                {"result": "success-3", "uri": "uri3"}
            ]
        ]
        
        results = process_sociopolitical_batch_with_retries(posts, max_retries=3)
        
        # Verify results maintain original order
        assert len(results) == 5
        assert results[0]["result"] == "success-0"  # First post
        assert results[1]["result"] == "success-1"  # Second post
        assert results[2]["result"] == "success-2"  # Third post
        assert results[3]["result"] == "success-3"  # Fourth post
        assert results[4]["result"] == "success-4"  # Fifth post

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_output_order_with_failures(self, mock_sleep, mock_process_batch):
        """Test that output order matches input order when some posts permanently fail.
        
        This test verifies that the output maintains the same order as input posts
        even when some posts succeed and others fail after all retries.
        """
        # Create posts with distinct text and URIs
        posts = [
            _make_post(uri=f"uri{i}", text=f"test{i}", batch_id=1) for i in range(4)
        ]
        
        # Mock responses where:
        # - Post 0 fails all attempts
        # - Post 1 succeeds on first try
        # - Post 2 fails all attempts
        # - Post 3 succeeds on second try
        mock_process_batch.side_effect = [
            [
                None,
                {"result": "success-1", "uri": "uri1"},
                None,
                None
            ],
            [
                None,
                None,
                {"result": "success-3", "uri": "uri3"}
            ],
            [
                None,
                None
            ]
        ]
        
        results = process_sociopolitical_batch_with_retries(posts, max_retries=2)
        
        # Verify results maintain original order
        assert len(results) == 4
        assert results[0] is None  # First post (failed)
        assert results[1]["result"] == "success-1"  # Second post
        assert results[2] is None  # Third post (failed)
        assert results[3]["result"] == "success-3"  # Fourth post

    @patch('services.ml_inference.sociopolitical.model.get_llm_service')
    def test_process_batch_kwargs_handling(self, mock_get_service):
        """Test that process_sociopolitical_batch correctly handles kwargs for structured_batch_completion.
        
        This test verifies that the function correctly passes through the role and model
        parameters to structured_batch_completion.
        """
        posts = [_make_post(uri="uri_1", text="test post", batch_id=1)]
        mock_label = LLMSociopoliticalLabelModel(
            is_sociopolitical=True,
            political_ideology_label="left"
        )
        mock_result = LLMSociopoliticalLabelsModel(labels=[mock_label])
        mock_service = Mock()
        mock_service.structured_batch_completion.return_value = [mock_result]
        mock_get_service.return_value = mock_service
        
        process_sociopolitical_batch(posts)
        
        # Verify structured_batch_completion was called with correct kwargs
        mock_service.structured_batch_completion.assert_called_once()
        call_args = mock_service.structured_batch_completion.call_args
        assert call_args.kwargs['role'] == 'user'
        assert call_args.kwargs['model'] == 'gpt-4o-mini'
        assert call_args.kwargs['response_model'] == LLMSociopoliticalLabelsModel
        
        # Verify the prompts were passed correctly
        assert isinstance(call_args.kwargs['prompts'], list)  # prompts should be passed as kwarg
        assert len(call_args.kwargs['prompts']) == 1  # Should have one prompt for one post

    @patch('services.ml_inference.sociopolitical.model.process_sociopolitical_batch')
    @patch('time.sleep')
    def test_process_batch_with_retries_kwargs_handling(self, mock_sleep, mock_process_batch):
        """Test that process_sociopolitical_batch_with_retries correctly handles and validates kwargs.
        
        This test verifies that:
        1. max_retries and initial_delay are correctly used
        2. Negative values are properly handled
        3. Default values are correctly applied when not specified
        """
        posts = [_make_post(uri="uri_1", text="test post", batch_id=1)]
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [{"result": "success"}]  # Second attempt succeeds
        ]
        
        # Test with custom kwargs
        custom_kwargs = {
            "max_retries": 3,
            "initial_delay": 2.0
        }
        process_sociopolitical_batch_with_retries(posts, **custom_kwargs)
        
        # Verify process_batch was called with posts
        mock_process_batch.assert_called_with(posts)
        
        # Verify sleep was called with correct initial delay
        mock_sleep.assert_called_once_with(2.0)  # initial_delay * (2 ** 0)
        
        # Reset mocks and test with negative values
        mock_sleep.reset_mock()
        mock_process_batch.reset_mock()
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [{"result": "success"}]  # Second attempt succeeds
        ]
        
        negative_kwargs = {
            "max_retries": -1,
            "initial_delay": -2.0
        }
        process_sociopolitical_batch_with_retries(posts, **negative_kwargs)
        
        # Verify process_batch was called exactly once (negative max_retries)
        mock_process_batch.assert_called_once_with(posts)
        
        # Verify sleep was not called (negative max_retries means no retries)
        mock_sleep.assert_not_called()
        
        # Reset mocks and test with default values
        mock_sleep.reset_mock()
        mock_process_batch.reset_mock()
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [{"result": "success"}]  # Second attempt succeeds
        ]
        
        process_sociopolitical_batch_with_retries(posts)  # No kwargs specified
        
        # Verify process_batch was called with posts
        mock_process_batch.assert_called_with(posts)
        
        # Verify sleep was called with default initial delay
        mock_sleep.assert_called_once_with(1.0)  # default initial_delay * (2 ** 0)
