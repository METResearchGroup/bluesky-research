import pytest
from typing import List, Dict, Any
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data

# Assume the existence of a function: filter_and_preview_sample_data(filters: Dict[str, Any], data: List[Dict[str, Any]]) -> List[Dict[str, Any]]

SAMPLE_POSTS = [
    {"id": 1, "user": "alice", "text": "climate change is real #climate", "date": "2024-06-01", "hashtags": ["#climate"]},
    {"id": 2, "user": "bob", "text": "politics and #election", "date": "2024-06-02", "hashtags": ["#election"]},
    {"id": 3, "user": "carol", "text": "#climate rally today", "date": "2024-06-03", "hashtags": ["#climate"]},
    {"id": 4, "user": "dave", "text": "sports update", "date": "2024-06-04", "hashtags": []},
    {"id": 5, "user": "eve", "text": "#climate and #election", "date": "2024-06-05", "hashtags": ["#climate", "#election"]},
    {"id": 6, "user": "frank", "text": "random post", "date": "2024-06-06", "hashtags": []},
]

class TestSampleDataPreview:
    """
    Unit tests for the sample data preview logic.
    Tests filtering and previewing top 5 rows from a static sample posts table.
    """

    def test_keyword_filter(self):
        """
        Test filtering by keyword returns only rows containing the keyword in text.
        """
        filters = {"Content": {"keywords": ["climate"]}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all("climate" in row["text"] for row in result)
        assert len(result) <= 5

    def test_hashtag_filter(self):
        """
        Test filtering by hashtag returns only rows containing the hashtag.
        """
        filters = {"Content": {"hashtags": ["#election"]}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all("#election" in row["hashtags"] for row in result)
        assert len(result) <= 5

    def test_date_range_filter(self):
        """
        Test filtering by date range returns only rows within the range.
        """
        filters = {"Temporal": {"date_range": "2024-06-02 to 2024-06-05"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        for row in result:
            assert "2024-06-02" <= row["date"] <= "2024-06-05"
        assert len(result) <= 5

    def test_user_handle_filter(self):
        """
        Test filtering by user handle returns only rows by those users.
        """
        filters = {"User": {"handles": ["alice", "eve"]}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["user"] in ["alice", "eve"] for row in result)
        assert len(result) <= 5

    def test_combined_filters(self):
        """
        Test combined filters (e.g., keyword and hashtag and date range).
        """
        filters = {
            "Content": {"keywords": ["climate"], "hashtags": ["#climate"]},
            "Temporal": {"date_range": "2024-06-01 to 2024-06-05"},
        }
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        for row in result:
            assert "climate" in row["text"]
            assert "#climate" in row["hashtags"]
            assert "2024-06-01" <= row["date"] <= "2024-06-05"
        assert len(result) <= 5

    def test_preview_limit(self):
        """
        Test that only the top 5 rows are returned, even if more match.
        """
        filters = {"Content": {"keywords": ["#climate"]}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert len(result) <= 5 