import pytest
from search_engine.app_v2.filter_state import FilterState
from search_engine.app_v2.components.example_queries_panel import apply_example_query
from search_engine.app_v2.example_queries import EXAMPLE_QUERIES

class TestApplyExampleQuery:
    """
    Unit tests for apply_example_query logic (headless, CI-safe).
    Verifies that filter_state and session_state are updated as expected.
    """
    @pytest.mark.parametrize("example_query", EXAMPLE_QUERIES)
    def test_apply_example_query(self, example_query):
        """
        Test that apply_example_query sets filter_state and session_state correctly for each example query.
        Input: Each example query from EXAMPLE_QUERIES.
        Expected: filter_state.filters and session_state reflect the example's filters and session keys.
        """
        filter_state = FilterState()
        session_state = {}
        apply_example_query(filter_state, example_query, session_state)
        # Check filter_state
        for category, params in example_query["filters"].items():
            for key, value in params.items():
                assert filter_state.filters[category][key] == value
        # Check session_state for keywords, hashtags, user_handles
        content = example_query["filters"].get("Content", {})
        user = example_query["filters"].get("User", {})
        assert session_state["keywords"] == content.get("keywords", [])
        assert session_state["hashtags"] == content.get("hashtags", [])
        assert session_state["user_handles"] == user.get("handles", [])
        assert session_state["show_query_preview"] is False 