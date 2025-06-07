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
        filters = {"Temporal": {"date_range": "2024-06-10 to 2024-06-15"}}
        expected_results = [row for row in self.data if "2024-06-10" <= row["date"] <= "2024-06-15"][:5]
        result = filter_and_preview_sample_data(filters, self.data)
        print("Filters:", filters)
        print("First 10 rows:", self.data[:10])
        print("Expected results:", expected_results)
        print("Actual results:", result)
        assert result == expected_results
        assert len(result) == len(expected_results)

    def test_combined_query(self):
        """
        Query for posts by a user with a specific hashtag in a date range.
        """
        user = self.data[0]["user"]
        filters = {
            "User": {"handles": [user]},
            "Content": {"hashtags": ["#bsky"]},
            "Temporal": {"date_range": "2024-06-01 to 2024-06-30"},
        }
        expected_results = [
            row for row in self.data
            if row["user"] == user and "#bsky" in row["hashtags"] and "2024-06-01" <= row["date"] <= "2024-06-30"
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