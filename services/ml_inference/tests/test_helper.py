import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from services.ml_inference.helper import (
    get_latest_labeling_session,
    get_posts_to_classify,
    load_cached_jsons_as_df,
    process_file,
)

class TestGetLatestLabelingSession:
    """Tests for get_latest_labeling_session function.
    
    This function queries DynamoDB for labeling sessions of a given inference type
    and returns the most recent one based on timestamp.
    """

    @pytest.fixture
    def mock_dynamodb(self):
        """Mock DynamoDB client."""
        with patch("services.ml_inference.helper.dynamodb") as mock:
            yield mock

    def test_get_latest_labeling_session_no_items(self, mock_dynamodb):
        """Test when no labeling sessions exist.
        
        Expected behavior:
        - When DynamoDB returns empty list, function should return None
        """
        mock_dynamodb.query_items_by_inference_type.return_value = []
        result = get_latest_labeling_session("llm")
        assert result is None
        mock_dynamodb.query_items_by_inference_type.assert_called_once_with(
            table_name="ml_inference_labeling_sessions",
            inference_type="llm"
        )

    def test_get_latest_labeling_session_with_items(self, mock_dynamodb):
        """Test when multiple labeling sessions exist.
        
        Expected behavior:
        - Should return the session with the most recent timestamp
        - Should properly parse DynamoDB format with nested 'S' keys
        """
        mock_items = [
            {
                "inference_timestamp": {"S": "2024-01-01"},
                "inference_type": {"S": "llm"},
                "metadata": {"M": {"data": {"S": "old"}}}
            },
            {
                "inference_timestamp": {"S": "2024-02-01"},
                "inference_type": {"S": "llm"},
                "metadata": {"M": {"data": {"S": "newest"}}}
            },
            {
                "inference_timestamp": {"S": "2024-01-15"},
                "inference_type": {"S": "llm"},
                "metadata": {"M": {"data": {"S": "middle"}}}
            }
        ]
        mock_dynamodb.query_items_by_inference_type.return_value = mock_items
        result = get_latest_labeling_session("llm")
        assert result == mock_items[1]  # Should return the newest timestamp
        mock_dynamodb.query_items_by_inference_type.assert_called_once_with(
            table_name="ml_inference_labeling_sessions",
            inference_type="llm"
        )

class TestGetPostsToClassify:
    """Tests for get_posts_to_classify function.
    
    This function retrieves posts from a queue that need to be classified,
    filtering by timestamp and optionally limiting by source.
    """

    @pytest.fixture
    def mock_dependencies(self):
        """Mock Queue and get_latest_labeling_session dependencies."""
        with patch("services.ml_inference.helper.Queue") as mock_queue, \
             patch("services.ml_inference.helper.get_latest_labeling_session") as mock_session:
            yield mock_queue.return_value, mock_session

    def test_get_posts_invalid_inference_type(self):
        """Test behavior with invalid inference type.
        
        Expected behavior:
        - Should raise ValueError when inference type is not one of: llm, perspective_api, ime
        """
        with pytest.raises(ValueError, match="Invalid inference type: invalid_type"):
            get_posts_to_classify("invalid_type")

    def test_get_posts_no_items(self, mock_dependencies):
        """Test when queue has no items.
        
        Expected behavior:
        - Should return empty list when queue has no items
        - Should properly handle empty JSON array string from queue
        """
        mock_queue, mock_session = mock_dependencies
        mock_queue.load_items_from_queue.return_value = "[]"
        mock_session.return_value = {
            "metadata": {
                "latest_id_classified": None,
                "inference_timestamp": "2024-01-01"
            }
        }

        result = get_posts_to_classify("llm")
        assert result == []

    def test_get_posts_with_max_per_source(self, mock_dependencies):
        """Test limiting posts per source.
        
        Expected behavior:
        - Should return at most max_per_source posts for each unique source
        - Should maintain all required fields (uri, text) in output
        - Should properly handle JSON parsing of queue items
        """
        mock_queue, mock_session = mock_dependencies
        queue_items = [
            {"payload": {"uri": "1", "text": "text1", "source": "src1"}},
            {"payload": {"uri": "2", "text": "text2", "source": "src1"}},
            {"payload": {"uri": "3", "text": "text3", "source": "src2"}},
        ]
        mock_queue.load_items_from_queue.return_value = json.dumps(queue_items)
        mock_session.return_value = {
            "metadata": {
                "latest_id_classified": None,
                "inference_timestamp": "2024-01-01"
            }
        }
        
        result = get_posts_to_classify("llm", max_per_source=1)
        assert len(result) == 2  # One from each source
        # Verify structure of returned items
        for item in result:
            assert set(item.keys()) == {"uri", "text"}

class TestLoadCachedJsonsAsDf:
    """Tests for load_cached_jsons_as_df function.
    
    This function loads multiple JSONL files in parallel and combines them
    into a single pandas DataFrame with specified dtypes.
    """

    @pytest.fixture
    def mock_file(self, tmp_path):
        """Create a temporary JSONL file for testing."""
        test_data = [
            {"uri": "1", "text": "text1"},
            {"uri": "2", "text": "text2"},
        ]
        file_path = tmp_path / "test.jsonl"
        with open(file_path, "w") as f:
            for item in test_data:
                f.write(json.dumps(item) + "\n")
        return str(file_path)

    def test_load_cached_jsons_empty_list(self):
        """Test behavior with empty file list.
        
        Expected behavior:
        - Should return None when no files are provided
        """
        result = load_cached_jsons_as_df([], {"uri": str, "text": str})
        assert result is None

    def test_load_cached_jsons_valid_file(self, mock_file):
        """Test loading valid JSONL file.
        
        Expected behavior:
        - Should return DataFrame with correct structure and dtypes
        - Should properly handle JSONL format
        - Should deduplicate based on uri
        """
        dtypes = {"uri": str, "text": str}
        result = load_cached_jsons_as_df([mock_file], dtypes)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["uri", "text"]
        assert result.dtypes["uri"] == "object"  # str type in pandas
        assert result.dtypes["text"] == "object"

    def test_process_file(self, mock_file):
        """Test process_file helper function.
        
        Expected behavior:
        - Should properly parse JSONL file into list of dictionaries
        - Should maintain structure of each JSON object
        """
        result = process_file(mock_file)
        assert len(result) == 2
        assert all(isinstance(item, dict) for item in result)
        assert all(set(item.keys()) == {"uri", "text"} for item in result)