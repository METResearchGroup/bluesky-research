import pytest
from search_engine.app_v2.sample_data import get_sample_posts
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data

class TestQueryPreviewIntegration:
    """
    Integration tests for the query preview logic using the real sample data.
    Each test submits a query and checks the number and content of results.
    """
    @classmethod
    def setup_class(cls):
        cls.data = get_sample_posts()

    def test_hashtag_query(self):
        """
        Query for posts with hashtag #climate. Assert correct number and content.
        """
        filters = {"Content": {"hashtags": ["#climate"]}}
        expected_results = [row for row in self.data if "#climate" in row["hashtags"]][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_user_query(self):
        """
        Query for posts by a specific user. Assert correct number and content.
        """
        if not self.data:
            pytest.skip("No sample data available.")
        user = self.data[0]["user"]
        filters = {"User": {"handles": [user]}}
        expected_results = [row for row in self.data if row["user"] == user][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_date_range_query(self):
        """
        Query for posts in a specific date range. Assert correct number and content.
        """
        filters = {"Temporal": {"date_range": "2024-06-02 to 2024-06-05"}}
        expected_results = [row for row in self.data if "2024-06-02" <= row["date"] <= "2024-06-05"][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_valence_query(self):
        """
        Query for posts with valence 'negative'.
        """
        filters = {"Sentiment": {"valence": "negative"}}
        expected_results = [row for row in self.data if row["valence"] == "negative"][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_toxic_query(self):
        """
        Query for posts that are toxic.
        """
        filters = {"Sentiment": {"toxicity": "Toxic"}}
        expected_results = [row for row in self.data if row["toxic"] is True][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_political_slant_query(self):
        """
        Query for posts that are political and have slant 'left'.
        """
        filters = {"Political": {"political": "Yes", "slant": "left"}}
        expected_results = [row for row in self.data if row["political"] is True and row["slant"] == "left"][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_combined_query(self):
        """
        Query for posts by a user with a specific hashtag, valence, and political slant in a date range.
        """
        if not self.data:
            pytest.skip("No sample data available.")
        user = self.data[0]["user"]
        filters = {
            "User": {"handles": [user]},
            "Content": {"hashtags": ["#climate"]},
            "Temporal": {"date_range": "2024-06-01 to 2024-06-10"},
            "Sentiment": {"valence": "positive"},
            "Political": {"political": "Yes", "slant": "left"}
        }
        expected_results = [
            row for row in self.data
            if row["user"] == user
            and "#climate" in row["hashtags"]
            and "2024-06-01" <= row["date"] <= "2024-06-10"
            and row["valence"] == "positive"
            and row["political"] is True
            and row["slant"] == "left"
        ][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_no_match_query(self):
        """
        Query for a hashtag that does not exist. Should return no results.
        """
        filters = {"Content": {"hashtags": ["#notarealhashtag"]}}
        expected_results = []
        result = filter_and_preview_sample_data(filters, self.data)
        assert result == expected_results
        assert len(result) == 0 