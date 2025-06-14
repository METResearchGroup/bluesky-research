import streamlit as st
from typing import Any, Dict
from search_engine.app_v2.filter_state import FilterState
from search_engine.app_v2.example_queries import EXAMPLE_QUERIES


def apply_example_query(
    filter_state: FilterState,
    example_query: Dict[str, Any],
    session_state: Dict[str, Any],
) -> None:
    """
    Applies the filters from an example query to the filter state and session state.
    Args:
        filter_state: The FilterState object to update.
        example_query: The example query dict with 'filters'.
        session_state: The session state dict to update (e.g., st.session_state).
    """
    filter_state.clear_filters()
    for category, params in example_query["filters"].items():
        for key, value in params.items():
            filter_state.add_filter(category, key, value)
    # Update session state for keywords, hashtags, user_handles if present
    if "Content" in example_query["filters"]:
        if "keywords" in example_query["filters"]["Content"]:
            session_state["keywords"] = example_query["filters"]["Content"]["keywords"]
        else:
            session_state["keywords"] = []
        if "hashtags" in example_query["filters"]["Content"]:
            session_state["hashtags"] = example_query["filters"]["Content"]["hashtags"]
        else:
            session_state["hashtags"] = []
    else:
        session_state["keywords"] = []
        session_state["hashtags"] = []
    if (
        "User" in example_query["filters"]
        and "handles" in example_query["filters"]["User"]
    ):
        session_state["user_handles"] = example_query["filters"]["User"]["handles"]
    else:
        session_state["user_handles"] = []
    session_state["show_query_preview"] = False


def render_example_queries_panel(filter_state: FilterState) -> None:
    """
    Renders the Example Queries panel as a dropdown with buttons.
    When a button is clicked, replaces the current filter state with the example's filters.
    Args:
        filter_state: The FilterState object managing current filters.
    """
    st.subheader("Example Queries")
    with st.expander("Show Example Queries", expanded=False):
        st.caption(
            "Quickly load a sample query. Selecting one will replace your current filters."
        )
        for i, example in enumerate(EXAMPLE_QUERIES):
            if st.button(example["name"], key=f"example_query_{i}"):
                apply_example_query(filter_state, example, st.session_state)
                st.rerun()
