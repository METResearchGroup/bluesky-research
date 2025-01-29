"""Tests for the model.py module."""

import json
from unittest.mock import patch, Mock

import pytest

from ml_tooling.llm.model import (
    generate_prompt,
    parse_llm_result,
    process_sociopolitical_batch,
    create_labels,
    batch_classify_posts,
    run_batch_classification,
)
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel,
    LLMSociopoliticalLabelsModel,
)


class TestGeneratePrompt:
    """Tests for generate_prompt() function."""

    def test_generate_prompt_single_post(self):
        """Test prompt generation for a single post.
        
        Should properly format the post with enumeration and newlines.
        """
        posts = [{"text": "Test post"}]
        prompt = generate_prompt(posts)
        assert "1. Test post\n" in prompt

    def test_generate_prompt_multiple_posts(self):
        """Test prompt generation for multiple posts.
        
        Should properly format multiple posts with enumeration and newlines.
        """
        posts = [
            {"text": "First post"},
            {"text": "Second post"},
        ]
        prompt = generate_prompt(posts)
        assert "1. First post\n" in prompt
        assert "2. Second post\n" in prompt


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


class TestProcessSociopoliticalBatch:
    """Tests for process_sociopolitical_batch() function."""

    @patch('ml_tooling.llm.model.run_batch_queries')
    def test_successful_batch_processing(self, mock_run_queries):
        """Test successful processing of a batch.
        
        Should properly process posts and return parsed results.
        """
        posts = [{"text": "Test post"}]
        mock_result = json.dumps({
            "labels": [
                {
                    "is_sociopolitical": True,
                    "political_ideology_label": "left"
                }
            ]
        })
        mock_run_queries.return_value = [mock_result]
        
        results = process_sociopolitical_batch(posts)
        assert len(results) == 1
        assert isinstance(results[0], dict)

    @patch('ml_tooling.llm.model.run_batch_queries')
    def test_failed_batch_processing(self, mock_run_queries):
        """Test handling of failed batch processing.
        
        Should return empty dicts for failed results.
        """
        posts = [{"text": "Test post"}]
        mock_run_queries.return_value = ["invalid json"]
        
        results = process_sociopolitical_batch(posts)
        assert len(results) == 1
        assert results[0] == {}

    @patch('ml_tooling.llm.model.run_batch_queries')
    def test_mixed_batch_processing(self, mock_run_queries):
        """Test processing batch with mix of successful and failed results.
        
        Should properly handle both successful and failed results in same batch.
        """
        posts = [
            {"text": "Test post 1"},
            {"text": "Test post 2"},
            {"text": "Test post 3"}
        ]
        mock_results = [
            json.dumps({
                "labels": [
                    {
                        "is_sociopolitical": True,
                        "political_ideology_label": "left"
                    }
                ]
            }),
            "invalid json",
            json.dumps({
                "labels": [
                    {
                        "is_sociopolitical": False,
                        "political_ideology_label": None
                    }
                ]
            })
        ]
        mock_run_queries.return_value = mock_results
        
        results = process_sociopolitical_batch(posts)
        assert len(results) == 3
        assert isinstance(results[0], dict)
        assert results[1] == {}  # Failed result
        assert isinstance(results[2], dict)


class TestCreateLabels:
    """Tests for create_labels() function."""

    def test_create_labels_successful(self):
        """Test creation of labels from successful responses.
        
        Should properly create label models with success status.
        """
        posts = [{"uri": "test", "text": "test post"}]
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
        posts = [{"uri": "test1", "text": "test post"}]
        responses = [{}]  # Empty dict represents failed response
        
        labels = create_labels(posts, responses)
        assert len(labels) == 1
        assert labels[0]["was_successfully_labeled"] is False
        assert "reason" in labels[0]

    def test_create_labels_mixed(self):
        """Test creation of labels from mixed successful and failed responses.
        
        Should properly handle both successful and failed responses in same batch.
        """
        posts = [
            {"uri": "test1", "text": "post1"},
            {"uri": "test2", "text": "post2"},
            {"uri": "test3", "text": "post3"}
        ]
        responses = [
            {
                "is_sociopolitical": True,
                "political_ideology_label": "left"
            },
            {},  # Failed response
            {
                "is_sociopolitical": False,
                "political_ideology_label": None
            }
        ]
        
        labels = create_labels(posts, responses)
        assert len(labels) == 3
        assert labels[0]["was_successfully_labeled"] is True
        assert labels[1]["was_successfully_labeled"] is False
        assert "reason" in labels[1]
        assert labels[2]["was_successfully_labeled"] is True


class TestBatchClassifyPosts:
    """Tests for batch_classify_posts() function."""

    @patch('ml_tooling.llm.model.process_sociopolitical_batch')
    @patch('ml_tooling.llm.model.write_posts_to_cache')
    @patch('ml_tooling.llm.model.return_failed_labels_to_input_queue')
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
            {"uri": "test1", "text": "post1", "batch_id": 1},
            {"uri": "test2", "text": "post2", "batch_id": 1}
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

    @patch('ml_tooling.llm.model.process_sociopolitical_batch')
    @patch('ml_tooling.llm.model.write_posts_to_cache')
    @patch('ml_tooling.llm.model.return_failed_labels_to_input_queue')
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
            {"uri": "test1", "text": "post1", "batch_id": 1},
            {"uri": "test2", "text": "post2", "batch_id": 1}
        ]
        
        mock_process_batch.return_value = [{}, {}]  # Empty dicts represent failed classifications
        
        result = batch_classify_posts(posts, batch_size=2)
        
        assert result["total_posts_successfully_labeled"] == 0
        assert result["total_posts_failed_to_label"] == 2
        mock_write_cache.assert_not_called()
        mock_return_failed.assert_called_once()

    @patch('ml_tooling.llm.model.process_sociopolitical_batch')
    @patch('ml_tooling.llm.model.write_posts_to_cache')
    @patch('ml_tooling.llm.model.return_failed_labels_to_input_queue')
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
            {"uri": f"test{i}", "text": f"post{i}", "batch_id": i//5}
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
        failed_batch = [{}, {}, {}, {}, {}]
        
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

    @patch('ml_tooling.llm.model.batch_classify_posts')
    def test_run_classification(self, mock_batch_classify):
        """Test running batch classification.
        
        Should properly call batch_classify_posts and return metadata.
        """
        posts = [{"uri": "test", "text": "post"}]
        expected_result = {
            "total_batches": 1,
            "total_posts_successfully_labeled": 1,
            "total_posts_failed_to_label": 0
        }
        mock_batch_classify.return_value = expected_result
        
        result = run_batch_classification(posts)
        assert result == expected_result
        mock_batch_classify.assert_called_once_with(posts=posts, batch_size=100)
