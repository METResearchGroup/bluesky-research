import pytest
from unittest.mock import patch
from search_engine.app.router import route_query
from search_engine.app.dataloader import load_data

class TestRouteQuery:
    """
    Tests for the route_query function in router.py.
    """
    @patch("search_engine.app.dataloader.get_data_from_keyword")
    def test_route_query_top_k(self, mock_loader):
        """
        route_query with intent 'top-k' should call get_data_from_keyword.
        """
        expected_result = {"posts": [], "total_count": 0, "source": "keyword"}
        mock_loader.return_value = expected_result
        result = route_query("top-k", "test query")
        assert result == expected_result
        mock_loader.assert_called_once_with("test query")

    @patch("search_engine.app.dataloader.get_data_from_query_engine")
    def test_route_query_summarize(self, mock_loader):
        """
        route_query with intent 'summarize' should call get_data_from_query_engine.
        """
        expected_result = {"posts": [], "total_count": 0, "source": "query_engine"}
        mock_loader.return_value = expected_result
        result = route_query("summarize", "test query")
        assert result == expected_result
        mock_loader.assert_called_once_with("test query")

    @patch("search_engine.app.dataloader.get_data_from_rag")
    def test_route_query_unknown(self, mock_loader):
        """
        route_query with intent 'unknown' should call get_data_from_rag.
        """
        expected_result = {"posts": [], "total_count": 0, "source": "rag"}
        mock_loader.return_value = expected_result
        result = route_query("unknown", "test query")
        assert result == expected_result
        mock_loader.assert_called_once_with("test query")

class TestLoadData:
    """
    Tests for the load_data function in dataloader.py.
    """
    @patch("search_engine.app.dataloader.get_data_from_keyword")
    def test_load_data_keyword(self, mock_loader):
        """
        load_data with source 'keyword' should call get_data_from_keyword.
        """
        expected_result = {"posts": [], "total_count": 0, "source": "keyword"}
        mock_loader.return_value = expected_result
        result = load_data("keyword", "test query")
        assert result == expected_result
        mock_loader.assert_called_once_with("test query")

    @patch("search_engine.app.dataloader.get_data_from_query_engine")
    def test_load_data_query_engine(self, mock_loader):
        """
        load_data with source 'query_engine' should call get_data_from_query_engine.
        """
        expected_result = {"posts": [], "total_count": 0, "source": "query_engine"}
        mock_loader.return_value = expected_result
        result = load_data("query_engine", "test query")
        assert result == expected_result
        mock_loader.assert_called_once_with("test query")

    @patch("search_engine.app.dataloader.get_data_from_rag")
    def test_load_data_rag(self, mock_loader):
        """
        load_data with source 'rag' should call get_data_from_rag.
        """
        expected_result = {"posts": [], "total_count": 0, "source": "rag"}
        mock_loader.return_value = expected_result
        result = load_data("rag", "test query")
        assert result == expected_result
        mock_loader.assert_called_once_with("test query")

    def test_load_data_invalid_source(self):
        """
        load_data with an invalid source should raise ValueError.
        """
        with pytest.raises(ValueError):
            load_data("invalid", "test query") 