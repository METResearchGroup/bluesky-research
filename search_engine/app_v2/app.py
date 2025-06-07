import streamlit as st

from search_engine.app_v2.filter_state import FilterState
from search_engine.app_v2.components.filter_chips_panel import render_filter_chips_panel
from search_engine.app_v2.components.filter_builder_panel import (
    render_filter_builder_panel,
)
from search_engine.app_v2.components.query_preview_panel import (
    render_query_preview_panel,
)
from search_engine.app_v2.components.visualization_quicklook_panel import (
    render_visualization_quicklook_panel,
)
from search_engine.app_v2.generate_sample_data import get_sample_posts


# --- Streamlit UI for Filter Builder Panel ---
def get_filter_state() -> FilterState:
    """
    Retrieve or initialize the FilterState in Streamlit session state.
    Returns:
        FilterState: The current filter state object.
    """
    if "filter_state" not in st.session_state:
        st.session_state["filter_state"] = FilterState()
    return st.session_state["filter_state"]


def main() -> None:
    """
    Main entrypoint for the Streamlit app. Renders the modular UI.
    Uses Streamlit's wide layout. Filters are 2/3, active filters/actions are 1/3.
    """
    st.set_page_config(page_title="Bluesky Data Access App Demo", layout="wide")
    st.title("Bluesky Data Access App Demo")
    st.caption("Build and preview research data queries interactively.")

    filter_state = get_filter_state()
    if "show_query_preview" not in st.session_state:
        st.session_state["show_query_preview"] = False

    # Two-column layout: filters left (2/3), active filters + actions right (1/3)
    left, right = st.columns([2, 1])

    with left:
        render_filter_builder_panel(filter_state)

    with right:
        render_filter_chips_panel(filter_state)
        st.divider()
        # Action buttons
        submit = st.button("Submit query", key="submit_filters")
        clear = st.button("Clear All Filters", key="clear_all_filters")
        if submit:
            from components.filter_builder_panel import build_human_readable_summary

            summary = build_human_readable_summary(filter_state)
            st.success(summary)
            st.session_state["show_query_preview"] = True
        if clear:
            filter_state.clear_filters()
            # Also clear session state for keywords and user_handles and hashtags
            st.session_state["keywords"] = []
            st.session_state["user_handles"] = []
            st.session_state["hashtags"] = []
            st.session_state["show_query_preview"] = False
            st.rerun()

    st.divider()
    render_query_preview_panel(
        filter_state, show=st.session_state["show_query_preview"]
    )

    # Visualization Quick-Look panel (only show after query preview is shown)
    if st.session_state["show_query_preview"]:
        st.divider()
        sample_posts = get_sample_posts()
        render_visualization_quicklook_panel(filter_state.filters, sample_posts)


if __name__ == "__main__":
    main()
