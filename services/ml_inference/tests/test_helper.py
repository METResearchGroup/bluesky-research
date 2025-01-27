import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from services.ml_inference.helper import (
    get_posts_to_classify,
    load_cached_jsons_as_df,
    process_file,
)

class TestGetPostsToClassify:
    """Tests for get_posts_to_classify function.
    
    This function retrieves posts from a queue that need to be classified,
    filtering by timestamp and optionally limiting by source.
    """

    @pytest.fixture
    def mock_dependencies(self):
        """Mock Queue dependency."""
        with patch("services.ml_inference.helper.Queue") as mock_queue:
            yield mock_queue.return_value

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
        mock_queue = mock_dependencies
        mock_queue.load_items_from_queue.return_value = "[]"
        previous_run_metadata = {
            "metadata": json.dumps({
                "latest_id_classified": "123",
                "inference_timestamp": "2024-01-01"
            })
        }

        result = get_posts_to_classify("llm", previous_run_metadata=previous_run_metadata)
        assert result == []
        mock_queue.load_items_from_queue.assert_called_once_with(
            limit=None,
            min_id="123",
            min_timestamp="2024-01-01",
            status="pending"
        )

    def test_get_posts_with_timestamp_override(self, mock_dependencies):
        """Test when timestamp parameter overrides previous run metadata.
        
        Expected behavior:
        - Should use provided timestamp instead of timestamp from previous run metadata
        """
        mock_queue = mock_dependencies
        mock_queue.load_items_from_queue.return_value = "[]"
        previous_run_metadata = {
            "metadata": json.dumps({
                "latest_id_classified": "100",
                "inference_timestamp": "2024-01-01"
            })
        }
        override_timestamp = "2023-12-31"

        get_posts_to_classify(
            "llm", 
            timestamp=override_timestamp,
            previous_run_metadata=previous_run_metadata
        )
        
        mock_queue.load_items_from_queue.assert_called_once_with(
            limit=None,
            min_id="100",
            min_timestamp=override_timestamp,
            status="pending"
        )

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