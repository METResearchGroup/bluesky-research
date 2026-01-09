"""Tests for the intergroup_model.py module."""

import json
from unittest.mock import patch, call
import time

import pytest

from ml_tooling.llm.intergroup_model import (
    generate_prompt,
    parse_llm_result,
    process_intergroup_batch,
    create_labels,
    batch_classify_posts,
    run_batch_classification,
    process_intergroup_batch_with_retries,
)
from services.ml_inference.models import LLMIntergroupLabelModel


class TestGeneratePrompt:
    """Tests for generate_prompt() function."""

    def test_generate_prompt_single_post(self):
        """Test prompt generation for a single post.

        Should properly format the post with enumeration and newlines.
        """
        # Arrange
        posts = [{"text": "Test post"}]
        expected_text = "1. Test post\n"

        # Act
        result = generate_prompt(posts)

        # Assert
        assert expected_text in result
        assert "intergroup" in result.lower()
        assert "not intergroup" in result.lower()

    def test_generate_prompt_multiple_posts(self):
        """Test prompt generation for multiple posts.

        Should properly format multiple posts with enumeration and newlines.
        """
        # Arrange
        posts = [
            {"text": "First post"},
            {"text": "Second post"},
        ]
        expected_first = "1. First post\n"
        expected_second = "2. Second post\n"

        # Act
        result = generate_prompt(posts)

        # Assert
        assert expected_first in result
        assert expected_second in result

    def test_generate_prompt_with_empty_text(self):
        """Test handling of posts with empty text field.

        Should handle empty strings without error and strip whitespace.
        """
        # Arrange
        posts = [{"text": ""}]
        expected_text = "1. \n"

        # Act
        result = generate_prompt(posts)

        # Assert
        assert expected_text in result

    def test_generate_prompt_with_whitespace_text(self):
        """Test handling of posts with whitespace-only text.

        Should properly strip whitespace from text.
        """
        # Arrange
        posts = [{"text": "  leading and trailing spaces  "}]
        expected_text = "1. leading and trailing spaces\n"

        # Act
        result = generate_prompt(posts)

        # Assert
        assert expected_text in result

    def test_generate_prompt_with_special_characters(self):
        """Test handling of posts with special characters.

        Should properly handle newlines, quotes, and other special characters.
        """
        # Arrange
        posts = [{
            "text": "Post with\nnewline and \"quotes\" and other $pecial ch@racters!"
        }]
        expected_text = "1. Post with\nnewline and \"quotes\" and other $pecial ch@racters!\n"

        # Act
        result = generate_prompt(posts)

        # Assert
        assert expected_text in result

    def test_generate_prompt_intergroup_definition(self):
        """Test that prompt includes intergroup content definition.

        Should clearly define what intergroup content means.
        """
        # Arrange
        posts = [{"text": "Test post"}]

        # Act
        result = generate_prompt(posts)

        # Assert
        assert "intergroup content" in result.lower()
        assert "social groups" in result.lower()
        assert "us vs them" in result.lower() or "us vs. them" in result.lower()


class TestParseLlmResult:
    """Tests for parse_llm_result() function."""

    def test_parse_valid_result(self):
        """Test parsing a valid LLM result.

        Should properly parse JSON and return list of label models.
        """
        # Arrange
        json_str = json.dumps({
            "labels": [
                {
                    "is_intergroup": True
                }
            ]
        })
        expected_count = 1

        # Act
        result = parse_llm_result(json_str, expected_number_of_posts=expected_count)

        # Assert
        assert len(result) == expected_count
        assert isinstance(result[0], LLMIntergroupLabelModel)
        assert result[0].is_intergroup is True

    def test_parse_invalid_count(self):
        """Test parsing result with wrong number of labels.

        Should raise ValueError if count doesn't match expected.
        """
        # Arrange
        json_str = json.dumps({
            "labels": [
                {
                    "is_intergroup": True
                }
            ]
        })
        expected_count = 2

        # Act & Assert
        with pytest.raises(ValueError, match="Number of results"):
            parse_llm_result(json_str, expected_number_of_posts=expected_count)

    def test_parse_invalid_json_format(self):
        """Test handling of malformed JSON input.

        Should raise JSONDecodeError for malformed JSON.
        """
        # Arrange
        invalid_json = "{ this is not valid json }"
        expected_count = 1

        # Act & Assert
        with pytest.raises(json.JSONDecodeError):
            parse_llm_result(invalid_json, expected_number_of_posts=expected_count)

    def test_parse_missing_labels_field(self):
        """Test handling of JSON missing required fields.

        Should raise ValidationError when 'labels' field is missing.
        """
        # Arrange
        json_str = json.dumps({
            "other_field": "some value"
        })
        expected_count = 1

        # Act & Assert
        with pytest.raises(ValueError):
            parse_llm_result(json_str, expected_number_of_posts=expected_count)

    def test_parse_empty_labels_list(self):
        """Test handling of empty labels list.

        Should raise ValueError when labels list is empty but expecting posts.
        """
        # Arrange
        json_str = json.dumps({
            "labels": []
        })
        expected_count = 1

        # Act & Assert
        with pytest.raises(ValueError, match="Number of results"):
            parse_llm_result(json_str, expected_number_of_posts=expected_count)

    def test_parse_correct_label_extraction(self):
        """Test correct label extraction from JSON.

        Should properly extract is_intergroup field from labels.
        """
        # Arrange
        json_str = json.dumps({
            "labels": [
                {"is_intergroup": True},
                {"is_intergroup": False},
                {"is_intergroup": True}
            ]
        })
        expected_count = 3

        # Act
        result = parse_llm_result(json_str, expected_number_of_posts=expected_count)

        # Assert
        assert len(result) == 3
        assert result[0].is_intergroup is True
        assert result[1].is_intergroup is False
        assert result[2].is_intergroup is True


class TestProcessIntergroupBatch:
    """Tests for process_intergroup_batch() function."""

    @patch('ml_tooling.llm.intergroup_model.run_batch_queries')
    def test_with_empty_posts_list(self, mock_run_queries):
        """Test processing with empty posts list.

        Should return empty list without calling LLM API.
        """
        # Arrange
        posts = []
        expected = []

        # Act
        result = process_intergroup_batch(posts)

        # Assert
        assert result == expected
        mock_run_queries.assert_not_called()

    @patch('ml_tooling.llm.intergroup_model.run_batch_queries')
    def test_successful_processing_single_minibatch(self, mock_run_queries):
        """Test successful processing with single minibatch.

        Should properly process posts and return results.
        """
        # Arrange
        posts = [{"text": f"post{i}"} for i in range(10)]  # DEFAULT_MINIBATCH_SIZE is 10
        mock_result = json.dumps({
            "labels": [
                {"is_intergroup": True}
            ] * 10
        })
        mock_run_queries.return_value = [mock_result]

        # Act
        result = process_intergroup_batch(posts)

        # Assert
        assert len(result) == 10
        assert all(isinstance(r, dict) for r in result)
        mock_run_queries.assert_called_once()

    @patch('ml_tooling.llm.intergroup_model.run_batch_queries')
    def test_successful_processing_multiple_minibatches(self, mock_run_queries):
        """Test successful processing with multiple minibatches.

        Should properly split into mini-batches and combine results.
        """
        # Arrange
        posts = [{"text": f"post{i}"} for i in range(25)]  # 2.5 times DEFAULT_MINIBATCH_SIZE
        mock_results = [
            json.dumps({
                "labels": [{"is_intergroup": True}] * 10
            }),
            json.dumps({
                "labels": [{"is_intergroup": False}] * 10
            }),
            json.dumps({
                "labels": [{"is_intergroup": True}] * 5
            })
        ]
        mock_run_queries.return_value = mock_results

        # Act
        result = process_intergroup_batch(posts)

        # Assert
        assert len(result) == 25
        assert all(isinstance(r, dict) for r in result)
        assert mock_run_queries.call_count == 1

    @patch('ml_tooling.llm.intergroup_model.run_batch_queries')
    def test_llm_api_failure(self, mock_run_queries):
        """Test LLM API failure handling.

        Should return None list when result count doesn't match minibatch count.
        """
        # Arrange
        posts = [{"text": f"post{i}"} for i in range(10)]
        mock_run_queries.return_value = []  # Empty result list

        # Act
        result = process_intergroup_batch(posts)

        # Assert
        assert len(result) == 10
        assert all(r is None for r in result)

    @patch('ml_tooling.llm.intergroup_model.run_batch_queries')
    def test_json_parsing_error_handling(self, mock_run_queries):
        """Test JSON parsing error handling.

        Should handle JSON parsing errors and return None for failed posts.
        """
        # Arrange
        posts = [{"text": "post1"}]
        mock_run_queries.return_value = ["{ invalid json }"]

        # Act
        result = process_intergroup_batch(posts)

        # Assert
        assert len(result) == 1
        assert result[0] is None

    @patch('ml_tooling.llm.intergroup_model.run_batch_queries')
    def test_result_count_mismatches(self, mock_run_queries):
        """Test handling of result count mismatches.

        Should return None for posts when result count doesn't match.
        """
        # Arrange
        posts = [{"text": f"post{i}"} for i in range(10)]
        mock_result = json.dumps({
            "labels": [{"is_intergroup": True}] * 5  # Only 5 labels for 10 posts
        })
        mock_run_queries.return_value = [mock_result]

        # Act
        result = process_intergroup_batch(posts)

        # Assert
        assert len(result) == 10
        assert all(r is None for r in result)


class TestProcessIntergroupBatchWithRetries:
    """Tests for process_intergroup_batch_with_retries() function."""

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch')
    @patch('time.sleep')
    def test_successful_first_attempt(self, mock_sleep, mock_process_batch):
        """Test successful first attempt (no retries).

        Should return results without retrying or sleeping.
        """
        # Arrange
        posts = [{"text": "test", "uri": "test1"}]
        expected_results = [{"is_intergroup": True}]
        mock_process_batch.return_value = expected_results

        # Act
        result = process_intergroup_batch_with_retries(posts)

        # Assert
        assert result == expected_results
        mock_process_batch.assert_called_once_with(posts)
        mock_sleep.assert_not_called()

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch')
    @patch('time.sleep')
    def test_retry_logic_with_exponential_backoff(self, mock_sleep, mock_process_batch):
        """Test retry logic with exponential backoff.

        Should retry failed posts with exponential backoff delays.
        """
        # Arrange
        posts = [{"text": "test", "uri": "test1"}]
        mock_process_batch.side_effect = [
            [None],  # First attempt fails
            [{"is_intergroup": True}]  # Second attempt succeeds
        ]

        # Act
        result = process_intergroup_batch_with_retries(posts, max_retries=4, initial_delay=1.0)

        # Assert
        assert mock_process_batch.call_count == 2
        mock_sleep.assert_called_once_with(1.0)  # initial_delay * (2 ** 0)
        assert result[0]["is_intergroup"] is True

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch')
    @patch('time.sleep')
    def test_max_retries_limit(self, mock_sleep, mock_process_batch):
        """Test max retries limit.

        Should stop retrying after max_retries attempts.
        """
        # Arrange
        posts = [{"text": "test", "uri": "test1"}]
        mock_process_batch.return_value = [None]  # Always fails

        # Act
        result = process_intergroup_batch_with_retries(posts, max_retries=2, initial_delay=1.0)

        # Assert
        assert mock_process_batch.call_count == 3  # Initial + 2 retries
        assert mock_sleep.call_count == 2
        assert result[0] is None

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch')
    @patch('time.sleep')
    def test_partial_success_after_retry(self, mock_sleep, mock_process_batch):
        """Test partial success (some posts succeed after retry).

        Should successfully retry failed posts and maintain order.
        """
        # Arrange
        posts = [
            {"text": "test1", "uri": "test1"},
            {"text": "test2", "uri": "test2"},
            {"text": "test3", "uri": "test3"}
        ]
        mock_process_batch.side_effect = [
            [{"is_intergroup": True}, None, None],  # First attempt: 1 success, 2 failures
            [{"is_intergroup": False}, {"is_intergroup": True}]  # Second attempt: both succeed
        ]

        # Act
        result = process_intergroup_batch_with_retries(posts, max_retries=4, initial_delay=1.0)

        # Assert
        assert len(result) == 3
        assert result[0]["is_intergroup"] is True
        assert result[1]["is_intergroup"] is False
        assert result[2]["is_intergroup"] is True
        assert mock_process_batch.call_count == 2
        mock_sleep.assert_called_once()

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch')
    @patch('time.sleep')
    def test_all_posts_fail_after_max_retries(self, mock_sleep, mock_process_batch):
        """Test all posts fail after max retries.

        Should return None for all posts after exhausting retries.
        """
        # Arrange
        posts = [
            {"text": "test1", "uri": "test1"},
            {"text": "test2", "uri": "test2"}
        ]
        mock_process_batch.return_value = [None, None]  # Always fails

        # Act
        result = process_intergroup_batch_with_retries(posts, max_retries=2, initial_delay=1.0)

        # Assert
        assert len(result) == 2
        assert all(r is None for r in result)
        assert mock_process_batch.call_count == 3  # Initial + 2 retries

    def test_input_validation_missing_text_field(self):
        """Test input validation (missing 'text' field raises KeyError).

        Should raise KeyError when posts are missing required 'text' field.
        """
        # Arrange
        invalid_posts = [
            {"uri": "test1"},  # Missing text field
            {"uri": "test2", "text": "valid post"}  # Valid post
        ]

        # Act & Assert
        with pytest.raises(KeyError, match="text"):
            process_intergroup_batch_with_retries(invalid_posts)

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch')
    def test_with_empty_posts_list(self, mock_process_batch):
        """Test with empty posts list.

        Should return empty list without calling process_batch.
        """
        # Arrange
        posts = []
        expected = []

        # Act
        result = process_intergroup_batch_with_retries(posts)

        # Assert
        assert result == expected
        mock_process_batch.assert_not_called()


class TestCreateLabels:
    """Tests for create_labels() function."""

    def test_successful_labels_creation(self):
        """Test successful labels creation.

        Should properly create label models with success status.
        """
        # Arrange
        posts = [{
            "uri": "test",
            "text": "test post",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z"
        }]
        responses = [{
            "is_intergroup": True
        }]

        # Act
        result = create_labels(posts, responses)

        # Assert
        assert len(result) == 1
        assert result[0]["was_successfully_labeled"] is True
        assert result[0]["is_intergroup"] is True
        assert result[0]["uri"] == "test"
        assert result[0]["text"] == "test post"

    def test_failed_labels(self):
        """Test failed labels (None responses).

        Should properly create label models with failed status.
        """
        # Arrange
        posts = [{
            "uri": "test1",
            "text": "test post",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z"
        }]
        responses = [None]  # None represents failed response

        # Act
        result = create_labels(posts, responses)

        # Assert
        assert len(result) == 1
        assert result[0]["was_successfully_labeled"] is False
        assert result[0]["is_intergroup"] is None

    def test_count_mismatches(self):
        """Test count mismatches (responses != posts).

        When there are fewer responses than posts, all posts should be marked as failed.
        """
        # Arrange
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
            }
        ]
        responses = [{
            "is_intergroup": True
        }]  # Only one response for two posts

        # Act
        result = create_labels(posts, responses)

        # Assert
        assert len(result) == 2
        assert result[0]["was_successfully_labeled"] is False
        assert result[1]["was_successfully_labeled"] is False

    def test_with_empty_posts_list(self):
        """Test with empty posts list.

        Should return empty list when no posts provided.
        """
        # Arrange
        posts = []
        responses = []

        # Act
        result = create_labels(posts, responses)

        # Assert
        assert result == []

    def test_label_timestamp_generation(self):
        """Test label timestamp generation.

        Should generate timestamp in correct format.
        """
        # Arrange
        posts = [{
            "uri": "test",
            "text": "test post",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z"
        }]
        responses = [{"is_intergroup": True}]

        # Act
        result = create_labels(posts, responses)

        # Assert
        assert "label_timestamp" in result[0]
        assert result[0]["label_timestamp"] is not None
        assert len(result[0]["label_timestamp"]) > 0

    def test_all_required_fields_in_output(self):
        """Test all required fields in output.

        Should include all required fields in label output.
        """
        # Arrange
        posts = [{
            "uri": "test",
            "text": "test post",
            "preprocessing_timestamp": "2023-01-01T00:00:00Z"
        }]
        responses = [{"is_intergroup": True}]

        # Act
        result = create_labels(posts, responses)

        # Assert
        required_fields = [
            "uri", "text", "preprocessing_timestamp", "llm_model_name",
            "was_successfully_labeled", "label_timestamp", "is_intergroup"
        ]
        for field in required_fields:
            assert field in result[0]

    def test_was_successfully_labeled_flag(self):
        """Test was_successfully_labeled flag.

        Should correctly set flag based on response status.
        """
        # Arrange
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
            }
        ]
        responses = [
            {"is_intergroup": True},  # Success
            None  # Failure
        ]

        # Act
        result = create_labels(posts, responses)

        # Assert
        assert result[0]["was_successfully_labeled"] is True
        assert result[1]["was_successfully_labeled"] is False


class TestBatchClassifyPosts:
    """Tests for batch_classify_posts() function."""

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch_with_retries')
    @patch('ml_tooling.llm.intergroup_model.write_posts_to_cache')
    @patch('ml_tooling.llm.intergroup_model.return_failed_labels_to_input_queue')
    @patch('lib.helper.create_batches')
    def test_successful_batch_processing(
        self,
        mock_create_batches,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test successful batch processing.

        Should process all posts and return correct metadata.
        """
        # Arrange
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
        mock_create_batches.return_value = [posts]
        mock_process_batch.return_value = [
            {"is_intergroup": True},
            {"is_intergroup": False}
        ]

        # Act
        result = batch_classify_posts(posts, batch_size=2)

        # Assert
        assert result["total_posts_successfully_labeled"] == 2
        assert result["total_posts_failed_to_label"] == 0
        mock_write_cache.assert_called_once()
        mock_return_failed.assert_not_called()

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch_with_retries')
    @patch('ml_tooling.llm.intergroup_model.write_posts_to_cache')
    @patch('ml_tooling.llm.intergroup_model.return_failed_labels_to_input_queue')
    @patch('lib.helper.create_batches')
    def test_batch_processing_with_failures(
        self,
        mock_create_batches,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test batch processing with failures.

        Should properly handle failed classifications and return correct metadata.
        """
        # Arrange
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
        mock_create_batches.return_value = [posts]
        mock_process_batch.return_value = [None, None]  # None represents failed classifications

        # Act
        result = batch_classify_posts(posts, batch_size=2)

        # Assert
        assert result["total_posts_successfully_labeled"] == 0
        assert result["total_posts_failed_to_label"] == 2
        mock_write_cache.assert_not_called()
        mock_return_failed.assert_called_once()

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch_with_retries')
    @patch('ml_tooling.llm.intergroup_model.write_posts_to_cache')
    @patch('ml_tooling.llm.intergroup_model.return_failed_labels_to_input_queue')
    @patch('lib.helper.create_batches')
    def test_empty_posts_list(
        self,
        mock_create_batches,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test empty posts list.

        Should return metadata with zero counts.
        """
        # Arrange
        posts = []
        mock_create_batches.return_value = []

        # Act
        result = batch_classify_posts(posts, batch_size=100)

        # Assert
        assert result["total_batches"] == 0
        assert result["total_posts_successfully_labeled"] == 0
        assert result["total_posts_failed_to_label"] == 0
        mock_process_batch.assert_not_called()

    @patch('ml_tooling.llm.intergroup_model.process_intergroup_batch_with_retries')
    @patch('ml_tooling.llm.intergroup_model.write_posts_to_cache')
    @patch('ml_tooling.llm.intergroup_model.return_failed_labels_to_input_queue')
    @patch('lib.helper.create_batches')
    def test_custom_batch_size(
        self,
        mock_create_batches,
        mock_return_failed,
        mock_write_cache,
        mock_process_batch
    ):
        """Test custom batch size.

        Should respect custom batch size parameter.
        """
        # Arrange
        posts = [{"uri": f"test{i}", "text": f"post{i}", "batch_id": i//5, "preprocessing_timestamp": "2024-01-01-00:00:00"} for i in range(25)]
        mock_create_batches.return_value = [posts[i:i+5] for i in range(0, 25, 5)]
        mock_process_batch.side_effect = [
            [{"is_intergroup": True}] * 5
        ] * 5

        # Act
        result = batch_classify_posts(posts, batch_size=5)

        # Assert
        assert result["total_batches"] == 5
        assert result["total_posts_successfully_labeled"] == 25
        mock_create_batches.assert_called_once_with(batch_list=posts, batch_size=5)


class TestRunBatchClassification:
    """Tests for run_batch_classification() function."""

    @patch('ml_tooling.llm.intergroup_model.batch_classify_posts')
    def test_wrapper_calls_batch_classify_posts_correctly(self, mock_batch_classify):
        """Test wrapper calls batch_classify_posts correctly.

        Should properly call batch_classify_posts and return metadata.
        """
        # Arrange
        posts = [{"uri": "test", "text": "post"}]
        expected_result = {
            "total_batches": 1,
            "total_posts_successfully_labeled": 1,
            "total_posts_failed_to_label": 0
        }
        mock_batch_classify.return_value = expected_result

        # Act
        result = run_batch_classification(posts)

        # Assert
        assert result == expected_result
        mock_batch_classify.assert_called_once_with(posts=posts, batch_size=100)

    @patch('ml_tooling.llm.intergroup_model.batch_classify_posts')
    def test_metadata_return_structure(self, mock_batch_classify):
        """Test metadata return structure.

        Should return metadata with correct structure.
        """
        # Arrange
        posts = [{"uri": "test", "text": "post"}]
        expected_metadata = {
            "total_batches": 1,
            "total_posts_successfully_labeled": 1,
            "total_posts_failed_to_label": 0
        }
        mock_batch_classify.return_value = expected_metadata

        # Act
        result = run_batch_classification(posts)

        # Assert
        assert "total_batches" in result
        assert "total_posts_successfully_labeled" in result
        assert "total_posts_failed_to_label" in result
        assert result == expected_metadata

