import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from search_engine.app.backend import app

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """
    Fixture for a sample DataFrame of posts with partition_date and text columns.
    """
    return pd.DataFrame({
        "partition_date": ["2024-11-10", "2024-11-10", "2024-11-11", "2024-11-12", "2024-11-12"],
        "text": ["Post 1", "Post 2", "Post 3", "Post 4", "Post 5"],
    })

class TestSemanticSummarizer:
    """
    Tests for the summarize_router_results function in semantic_summarizer.py.
    """
    def test_summarize_router_results_returns_summary_and_graph(self, sample_df):
        """
        Given a DataFrame, summarize_router_results should return a dict with:
        - 'text': a summary string listing counts per partition_date
        - 'graph': a list of graph specs for line and bar plots
        """
        from search_engine.app.semantic_summarizer import summarize_router_results
        result = summarize_router_results(sample_df)
        assert "text" in result
        assert "graph" in result
        assert isinstance(result["graph"], list)
        assert any(g["type"] == "line" for g in result["graph"])
        assert any(g["type"] == "bar" for g in result["graph"])
        # The text should mention each date and count
        for date in sample_df["partition_date"].unique():
            assert date in result["text"]

class TestAnswerComposer:
    """
    Tests for the compose_answer function in answer_composer.py.
    """
    @patch("search_engine.app.answer_composer.plt.savefig")
    def test_compose_answer_generates_output_and_saves_visuals(self, mock_savefig, sample_df):
        """
        compose_answer should:
        - Take summarizer output and DataFrame
        - Generate .png files for each graph (mocked)
        - Return a dict with 'text', 'df', and 'visuals' (with type and path)
        """
        from search_engine.app.answer_composer import compose_answer
        summarizer_output = {
            "text": "The total number of posts per day is:\n2024-11-10: 2\n2024-11-11: 1\n2024-11-12: 2",
            "graph": [
                {"type": "line", "kwargs": {"transform": {"groupby": True, "groupby_col": "partition_date", "agg_func": "count", "agg_col": "counts"}, "graph": {"col_x": "partition_date", "xlabel": "Date", "col_y": "counts", "ylabel": "Date"}}},
                {"type": "bar", "kwargs": {"transform": {"groupby": True, "groupby_col": "partition_date", "agg_func": "count", "agg_col": "counts"}, "graph": {"col_x": "partition_date", "xlabel": "Date", "col_y": "counts", "ylabel": "Date"}}},
            ]
        }
        result = compose_answer(summarizer_output, sample_df)
        assert "text" in result
        assert "df" in result
        assert isinstance(result["visuals"], list)
        assert all("type" in v and "path" in v for v in result["visuals"])
        assert mock_savefig.call_count == 2

class TestBackendSummarizerEndpoints:
    """
    Tests for the /summarize-results and /compose-answer endpoints in backend.py.
    """
    @pytest.fixture(autouse=True)
    def setup_client(self):
        self.client = TestClient(app)

    @patch("search_engine.app.backend.summarize_router_results")
    def test_summarize_results_endpoint(self, mock_summarize, sample_df):
        """
        POST /summarize-results should call summarize_router_results and return its output.
        """
        expected = {"text": "summary", "graph": []}
        mock_summarize.return_value = expected
        # Simulate sending DataFrame as JSON records
        payload = {"posts": sample_df.to_dict(orient="records")}
        response = self.client.post("/summarize-results", json=payload)
        assert response.status_code == 200
        assert response.json() == expected
        mock_summarize.assert_called_once()

    @patch("search_engine.app.backend.compose_answer")
    def test_compose_answer_endpoint(self, mock_compose, sample_df):
        """
        POST /compose-answer should call compose_answer and return its output.
        """
        expected = {"text": "summary", "df": [], "visuals": []}
        mock_compose.return_value = expected
        payload = {
            "summarizer_output": {"text": "summary", "graph": []},
            "posts": sample_df.to_dict(orient="records")
        }
        response = self.client.post("/compose-answer", json=payload)
        assert response.status_code == 200
        assert response.json() == expected
        mock_compose.assert_called_once() 