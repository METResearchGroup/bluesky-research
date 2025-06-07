import pytest
from typing import List, Dict, Any
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data

# Assume the existence of a function: filter_and_preview_sample_data(filters: Dict[str, Any], data: List[Dict[str, Any]]) -> List[Dict[str, Any]]

SAMPLE_POSTS = [
    {"id": 1, "user": "alice", "text": "climate change is real #climate", "date": "2024-06-01", "hashtags": ["#climate"], "valence": "positive", "toxic": None, "political": True, "slant": "left"},
    {"id": 2, "user": "bob", "text": "politics and #election", "date": "2024-06-02", "hashtags": ["#election"], "valence": "neutral", "toxic": None, "political": False, "slant": None},
    {"id": 3, "user": "carol", "text": "#climate rally today", "date": "2024-06-03", "hashtags": ["#climate"], "valence": "negative", "toxic": True, "political": True, "slant": "center"},
    {"id": 4, "user": "dave", "text": "sports update", "date": "2024-06-04", "hashtags": [], "valence": "positive", "toxic": None, "political": False, "slant": None},
    {"id": 5, "user": "eve", "text": "#climate and #election", "date": "2024-06-05", "hashtags": ["#climate", "#election"], "valence": "neutral", "toxic": None, "political": True, "slant": "right"},
    {"id": 6, "user": "frank", "text": "random post", "date": "2024-06-06", "hashtags": [], "valence": "negative", "toxic": False, "political": False, "slant": None},
    {"id": 7, "user": "grace", "text": "AI is the future #ai", "date": "2024-06-07", "hashtags": ["#ai"], "valence": "positive", "toxic": None, "political": True, "slant": "unclear"},
    {"id": 8, "user": "heidi", "text": "music festival #music", "date": "2024-06-08", "hashtags": ["#music"], "valence": "neutral", "toxic": None, "political": False, "slant": None},
    {"id": 9, "user": "ivan", "text": "news update #news", "date": "2024-06-09", "hashtags": ["#news"], "valence": "negative", "toxic": True, "political": True, "slant": "left"},
    {"id": 10, "user": "judy", "text": "fun times #fun", "date": "2024-06-10", "hashtags": ["#fun"], "valence": "positive", "toxic": None, "political": False, "slant": None},
]

class TestSampleDataPreview:
    """
    Unit tests for the sample data preview logic.
    Tests filtering and previewing top 5 rows from a static sample posts table.
    """

    def test_valence_filter(self):
        """
        Test filtering by valence returns only rows with that valence.
        """
        filters = {"Sentiment": {"valence": "positive"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["valence"] == "positive" for row in result)
        assert len(result) <= 5

    def test_toxic_filter(self):
        """
        Test filtering by toxicity returns only rows with that toxicity.
        """
        filters = {"Sentiment": {"toxicity": "Toxic"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["toxic"] is True for row in result)
        assert len(result) <= 5
        filters = {"Sentiment": {"toxicity": "Not Toxic"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["toxic"] is False for row in result)
        assert len(result) <= 5
        filters = {"Sentiment": {"toxicity": "Uncertain"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["toxic"] is None for row in result)
        assert len(result) <= 5

    def test_political_filter(self):
        """
        Test filtering by political returns only rows with that political value.
        """
        filters = {"Political": {"political": "Yes"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["political"] is True for row in result)
        assert len(result) <= 5
        filters = {"Political": {"political": "No"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["political"] is False for row in result)
        assert len(result) <= 5

    def test_slant_filter(self):
        """
        Test filtering by slant returns only rows with that slant.
        """
        filters = {"Political": {"slant": "left"}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert all(row["slant"] == "left" for row in result)
        assert len(result) <= 5

    def test_combined_filters(self):
        """
        Test combined filters (e.g., valence and political and slant).
        """
        filters = {
            "Sentiment": {"valence": "negative"},
            "Political": {"political": "Yes"},
            "Political": {"slant": "center"}
        }
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        for row in result:
            assert row["valence"] == "negative"
            assert row["political"] is True
            assert row["slant"] == "center"
        assert len(result) <= 5

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

    def test_preview_limit(self):
        """
        Test that only the top 5 rows are returned, even if more match.
        """
        filters = {"Content": {"keywords": ["#climate"]}}
        result = filter_and_preview_sample_data(filters, SAMPLE_POSTS)
        assert len(result) <= 5 