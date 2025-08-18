import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from search_engine.app.backend import app

@pytest.fixture
def client() -> TestClient:
    """Fixture for FastAPI test client."""
    return TestClient(app)

class TestFetchResultsEndpoint:
    """
    Test suite for the /fetch-results endpoint in the FastAPI backend.
    This endpoint should:
      - Load posts from local parquet files using DuckDB for the date range 2024-11-10 to 2024-11-21
      - Return the top 10 posts as JSON
      - Include a count of total results in the response
    """
    @patch("search_engine.app.backend.load_data_from_local_storage")
    def test_fetch_results_returns_top_10_posts(self, mock_load_data, client: TestClient) -> None:
        """
        Test that /fetch-results returns the top 10 posts and total count.
        Mocks the data loading function to return a DataFrame with 25 posts.
        Expects:
          - Response status 200
          - 'posts' key with 10 items
          - 'total_count' key with value 25
        """
        import pandas as pd
        # Create mock DataFrame with 25 posts
        mock_df = pd.DataFrame({
            "id": [f"post_{i}" for i in range(25)],
            "text": [f"Post text {i}" for i in range(25)],
            "likes": [i for i in range(25)],
        })
        mock_load_data.return_value = mock_df

        response = client.get("/fetch-results")
        assert response.status_code == 200
        data = response.json()
        assert "posts" in data
        assert "total_count" in data
        assert len(data["posts"]) == 10
        assert data["total_count"] == 25
        # Check that the first post matches the mock data
        assert data["posts"][0]["id"] == "post_0"
        assert data["posts"][0]["text"] == "Post text 0"
        assert data["posts"][0]["likes"] == 0

class TestGetQueryIntentEndpoint:
    """
    Test suite for the /get-query-intent endpoint in the FastAPI backend.
    This endpoint should:
      - Classify the intent of a query as 'top-k', 'summarize', or 'unknown'
      - Return a JSON with 'intent' and 'reason'
    """
    def test_top_k_intent(self, client: TestClient) -> None:
        """
        Test that a 'top-k' query returns the correct intent and reason.
        """
        response = client.get("/get-query-intent", params={"query": "What are the most liked posts?"})
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "top-k"
        assert "top" in data["reason"] or "most" in data["reason"]

    def test_summarize_intent(self, client: TestClient) -> None:
        """
        Test that a 'summarize' query returns the correct intent and reason.
        """
        response = client.get("/get-query-intent", params={"query": "Summarize what people are saying about climate."})
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "summarize"
        assert "summary" in data["reason"] or "summarize" in data["reason"]

    def test_unknown_intent(self, client: TestClient) -> None:
        """
        Test that an unknown query returns the correct intent and reason.
        """
        response = client.get("/get-query-intent", params={"query": "Tell me something interesting."})
        assert response.status_code == 200
        data = response.json()
        assert data["intent"] == "unknown"
        assert "does not match" in data["reason"] 