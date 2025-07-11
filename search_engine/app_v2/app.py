import streamlit as st
import os

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
from search_engine.app_v2.components.export_templates_panel import (
    render_export_templates_panel,
)
from search_engine.app_v2.components.example_queries_panel import (
    render_example_queries_panel,
)
from search_engine.app_v2.sample_data import get_sample_posts
from search_engine.app_v2.sample_data_preview import filter_and_preview_sample_data
from search_engine.app_v2.components.sna_sidebar_controls_panel import (
    render_sna_sidebar_controls_panel,
)
from search_engine.app_v2.components.sna_mini_graph_panel import (
    render_sna_mini_graph_panel,
    filter_sample_graph,
)
from search_engine.app_v2.components.sna_metric_summary_panel import (
    render_sna_metric_summary_panel,
)
from search_engine.app_v2.components.sna_export_simulation_panel import (
    render_sna_export_simulation_panel,
)
from search_engine.app_v2.components.sna_time_slider_panel import (
    render_sna_time_slider_panel,
)


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
    # Must be the first Streamlit command
    st.set_page_config(page_title="Bluesky Data Access App Demo", layout="wide")
    st.title("Bluesky Data Access App Demo")
    st.caption("Build and preview research data queries interactively.")
    # Collapsible Getting Started/help section (should be directly below title/caption)
    with st.expander("Getting Started / Help", expanded=False):
        st.markdown("""
        **Welcome to the Bluesky Data Access App Demo!**

        - Use the Filter Builder Panel to select filters for your query.
        - Submit your query to preview results and see visualizations.
        - Export your results as CSV or Parquet using the floating action button.
        - Use the 'Reset All' button to clear all filters and start over.
        """)
    # High-contrast mode toggle
    high_contrast = st.checkbox("Enable High-Contrast Mode", key="high_contrast_mode")
    if high_contrast:
        st.markdown(
            """
            <style>
            body, .stApp, .stMarkdown, .stDataFrame, .stButton>button, .stRadio>div, .stTextInput>div, .stSelectbox>div, .stExpander, .stNumberInput>div {
                background-color: #fff !important;
                color: #111 !important;
            }
            .stButton>button, .fab-export button {
                background-color: #000 !important;
                color: #fff !important;
                border: 2px solid #fff !important;
                font-weight: bold !important;
            }
            .stExpander, .stRadio>div, .stTextInput>div, .stSelectbox>div, .stNumberInput>div {
                border: 2px solid #000 !important;
            }
            .stDataFrame, .stDataFrame th, .stDataFrame td {
                background-color: #fff !important;
                color: #111 !important;
                border-color: #000 !important;
            }
            .stMarkdown strong, .stMarkdown b {
                color: #000 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

    # Add two tabs: 'Query data' and 'Perform social network analysis'
    tab1, tab2 = st.tabs(["Query data", "Perform social network analysis"])

    with tab1:
        filter_state = get_filter_state()
        if "show_query_preview" not in st.session_state:
            st.session_state["show_query_preview"] = False

        # Load sample data with correct scaling once
        demo_mode = os.environ.get("DEMO_MODE", True)
        scale = 1000 if demo_mode else 1
        sample_posts = get_sample_posts(scale=scale)

        # Two-column layout: filters left (2/3), active filters + actions right (1/3)
        left, right = st.columns([2, 1])

        with left:
            render_filter_builder_panel(filter_state)

        with right:
            render_example_queries_panel(filter_state)
            render_filter_chips_panel(filter_state)
            # Copy query/filter state to clipboard (Nice-to-Have)
            if st.button("Copy filter state as JSON", key="copy_filter_state_btn"):
                st.toast("Filter state JSON shown below. Copy manually.")
            st.json(filter_state.filters, expanded=False)
            st.divider()
            # Action buttons
            submit = st.button("Submit query", key="submit_filters")
            clear = st.button("Clear All Filters", key="clear_all_filters")
            reset = st.button("Reset All", key="reset_all_filters")
            if submit:
                from components.filter_builder_panel import build_human_readable_summary

                summary = build_human_readable_summary(filter_state)
                st.success(summary)
                st.session_state["show_query_preview"] = True
            if clear or reset:
                filter_state.clear_filters()
                # Also clear session state for keywords and user_handles and hashtags
                st.session_state["keywords"] = []
                st.session_state["user_handles"] = []
                st.session_state["hashtags"] = []
                st.session_state["show_query_preview"] = False
                st.session_state["preview_rows_mode"] = "less"
                st.rerun()
            # --- Export & Templates Panel ---
            st.divider()
            render_export_templates_panel(filter_state, sample_posts)

        st.divider()
        render_query_preview_panel(
            filter_state,
            show=st.session_state["show_query_preview"],
            sample_posts=sample_posts,
        )

        # Visualization Quick-Look panel: always show and update live with filter changes
        st.divider()
        if st.session_state["show_query_preview"]:
            # Filter data for visualization (up to max_results)
            filtered_for_viz = filter_and_preview_sample_data(
                filter_state.filters, sample_posts, preview=False
            )
            render_visualization_quicklook_panel(filter_state.filters, filtered_for_viz)

    with tab2:
        # SNA Tab: Add onboarding/help and demo instructions at the top
        st.title("Social Network Analysis (SNA) Tab")
        st.caption(
            "Analyze social network structure, polarization, and key actors using interactive controls and visualizations."
        )
        # --- Onboarding/How to Use Section ---
        with st.expander("How to Use (SNA Tab)", expanded=False):
            st.markdown("""
            **Welcome to the Social Network Analysis (SNA) Tab!**

            - **Sidebar Controls:** Use the left panel to select edge type, community algorithm, centrality metric, hop depth, time range, and content filters. Hover over any label for a tooltip explaining the concept.
            - **Mini-Graph Preview:** See a live preview of the network graph based on your filters. Node colors indicate political slant. Hover over nodes for details.
            - **Metric Summary Panel:** View key metrics (top central nodes, community count, assortativity) for the current graph snapshot. Tooltips explain each metric.
            - **Export Simulation:** Download the current network as CSV or GEXF for further analysis.
            - **Time Slider Animation:** Animate the network over time using the date range slider and play button.
            - For more details, see the 'Demo instructions' section below.
            """)
        # --- Demo Instructions Section ---
        with st.expander("Demo instructions", expanded=False):
            st.markdown("""
            **How to Run the Demo:**
            1. Launch the app and navigate to the 'Perform social network analysis' tab.
            2. Use the sidebar to select filters and parameters. Try changing edge type, community algorithm, or time range.
            3. Observe how the mini-graph, metrics, and export options update live.
            4. Use the time slider to animate the network over different days.
            5. Download the edge list, node metrics, or GEXF for offline analysis.

            **Sample Research Questions:**
            - Who are the most central users in the network during a given week?
            - How do communities form and change over time?
            - Is there evidence of polarization (e.g., left vs. right slant) in the network structure?
            - How does toxicity or valence affect engagement and network position?

            **Demo Flow Script:**
            - Start with all filters at default. Show the full network and explain the color legend.
            - Select 'retweet' as edge type and 'Louvain' for community detection. Show how the graph and metrics update.
            - Use the time slider to animate the network from June 1 to June 14, highlighting changes in community structure.
            - Apply a filter for 'Political' and 'left' slant. Show how the subgraph and metrics change.
            - Download the edge list and node metrics to demonstrate export functionality.
            - Reset filters and try a different centrality metric (e.g., Betweenness). Discuss the impact on top nodes.
            """)
        # --- SNA Tab Main Layout ---
        sna_left, sna_right = st.columns([2, 1], gap="large")
        if "sna_state" not in st.session_state:
            st.session_state["sna_state"] = {}
        state = st.session_state["sna_state"]
        with sna_left:
            render_sna_sidebar_controls_panel(state)
        with sna_right:
            render_sna_time_slider_panel(state)
            G = filter_sample_graph(state)
            render_sna_mini_graph_panel(state)
            render_sna_metric_summary_panel(state, G)
            render_sna_export_simulation_panel(state, G)


if __name__ == "__main__":
    main()
