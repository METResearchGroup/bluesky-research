import streamlit as st
from filter_state import FilterState
from search_engine.app_v2.generate_sample_data import get_sample_posts
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data
import pandas as pd


def render_query_preview_panel(filter_state: FilterState, show: bool) -> None:
    """
    Renders the Query Preview panel, showing the top 5 rows of filtered sample data.
    The preview table is horizontally scrollable for better UX with wide data.
    Args:
        filter_state: The FilterState object managing current filters.
        show: Whether to display the preview (only after query submission).
    """
    if not show:
        return
    st.subheader("Query Preview")
    filters = filter_state.filters
    sample_posts = get_sample_posts()
    preview_rows = filter_and_preview_sample_data(filters, sample_posts)
    total_matches = len(
        [
            row
            for row in sample_posts
            if row in preview_rows or filter_and_preview_sample_data(filters, [row])
        ]
    )
    st.markdown(f"*Found {total_matches} results matching your query*")
    if preview_rows:
        df = pd.DataFrame(preview_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No matching sample data for the current filters.")
