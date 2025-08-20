import pytest
import streamlit as st
from unittest.mock import patch, MagicMock, call
from search_engine.app_v2.components.sna_metric_summary_panel import render_sna_metric_summary_panel
import networkx as nx
import datetime

# Note: These tests are for TDD Red Phase. The SNA tab and its components do not yet exist.
# All tests are expected to fail until the corresponding implementation is provided.

class TestSnaSidebarControls:
    """
    Tests for the Sidebar Controls panel in the SNA tab.
    Covers dropdowns, sliders, checkboxes, filter chips, and submit button.
    """
    def test_sidebar_controls_render(self):
        """
        Should render all required controls: edge type, community algorithm, centrality metric, hop depth, time range, content filters, and submit button.
        """
        # This will fail until the sidebar controls are implemented
        assert False

    def test_filter_chip_selection_and_deselection(self):
        """
        Should allow users to select filters and display them as chips, and deselect by clicking chips.
        """
        assert False

    def test_submit_button_displays_parameters(self):
        """
        Should display a string summary of chosen parameters when submit is clicked.
        """
        assert False

class TestSnaMiniGraphPreview:
    """
    Tests for the Mini-Graph Preview panel in the SNA tab.
    Covers networkx integration, Streamlit/pyvis rendering, and updates on filter change.
    """
    def test_graph_renders_with_sample_data(self):
        """
        Should render a 50-node sample graph using networkx and a Streamlit visualization component.
        The graph should be visible in the UI and have the correct number of nodes and edges.
        """
        assert False

    def test_graph_updates_on_control_change(self):
        """
        Should update the graph visualization when sidebar controls are changed (e.g., edge type, hop depth).
        Changing a control should result in a visibly different graph (e.g., different edges or layout).
        """
        assert False

    def test_graph_rendering_logic_unit(self):
        """
        Should correctly generate a networkx graph object with the expected structure (50 nodes, sample edges) given a set of filter parameters.
        This is a pure logic test, not a UI test.
        """
        assert False

class TestSnaMetricSummaryPanel:
    """
    Tests for the Metric Summary Panel in the SNA tab.
    Covers display and refresh of static metrics (centrality, community count, assortativity).
    """
    def test_metrics_display(self):
        """
        Should display top 5 central nodes, community count, and assortativity for the current graph snapshot.
        This test expects the Metric Summary Panel to render these metrics in the UI, using either hard-coded or sample data.
        """
        state = {}
        G = nx.Graph()
        G.add_nodes_from(["A", "B", "C", "D", "E", "F"])
        G.add_edges_from([("A", "B"), ("B", "C"), ("C", "D"), ("D", "E"), ("E", "F"), ("F", "A")])
        with patch("streamlit.subheader") as mock_subheader, \
             patch("streamlit.markdown") as mock_markdown:
            render_sna_metric_summary_panel(state, G)
            # Check that the subheader and key metrics are rendered
            mock_subheader.assert_called_with("Metric Summary Panel")
            markdown_calls = [call[0][0] for call in mock_markdown.call_args_list]
            assert any("Community Count" in c for c in markdown_calls)
            assert any("Assortativity" in c for c in markdown_calls)
            assert any("Top 5 Central Nodes" in c for c in markdown_calls)

    def test_metrics_refresh_on_filter_change(self):
        """
        Should refresh metrics when filters or parameters are changed.
        This test expects the Metric Summary Panel to update its displayed metrics when the sidebar controls are changed.
        """
        state1 = {"centrality_metric": "Degree"}
        state2 = {"centrality_metric": "Betweenness"}
        G1 = nx.cycle_graph(6)
        G2 = nx.path_graph(6)
        with patch("streamlit.markdown") as mock_markdown1:
            render_sna_metric_summary_panel(state1, G1)
            calls1 = [call[0][0] for call in mock_markdown1.call_args_list]
        with patch("streamlit.markdown") as mock_markdown2:
            render_sna_metric_summary_panel(state2, G2)
            calls2 = [call[0][0] for call in mock_markdown2.call_args_list]
        # The metrics output should differ between the two calls
        assert calls1 != calls2

class TestSnaExportSimulation:
    """
    Tests for the Export Simulation panel in the SNA tab.
    Covers download buttons and file simulation for edge list, node metrics, and GEXF.
    """
    def test_export_buttons_present(self):
        """
        Should render buttons for 'Download Edge List (CSV)', 'Download Node Metrics (CSV)', and 'Download GEXF'.
        """
        from search_engine.app_v2.components import sna_export_simulation_panel
        state = {}
        G = nx.cycle_graph(6)
        with patch("streamlit.button") as mock_button:
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G)
            button_calls = [call[0][0] for call in mock_button.call_args_list]
            assert any("Download Edge List" in c for c in button_calls)
            assert any("Download Node Metrics" in c for c in button_calls)
            assert any("Download GEXF" in c for c in button_calls)

    def test_export_triggers_sample_file_download(self):
        """
        Should trigger a simulated file download with sample network data when export buttons are clicked.
        """
        from search_engine.app_v2.components import sna_export_simulation_panel
        state = {}
        G = nx.cycle_graph(6)
        with patch("streamlit.button", side_effect=[True, False, False]), \
             patch("streamlit.download_button") as mock_download:
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G)
            assert mock_download.called
        with patch("streamlit.button", side_effect=[False, True, False]), \
             patch("streamlit.download_button") as mock_download:
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G)
            assert mock_download.called
        with patch("streamlit.button", side_effect=[False, False, True]), \
             patch("streamlit.download_button") as mock_download:
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G)
            assert mock_download.called

class TestSnaTimeSliderAnimation:
    """
    Tests for the Time Slider Animation in the SNA tab.
    Covers st.slider for date range and snapshot switching.
    """
    def test_time_slider_renders_and_updates(self):
        """
        Should render a date range slider and update graph/metrics when the slider is moved.
        """
        from search_engine.app_v2.components import sna_time_slider_panel
        state = {}
        with patch("streamlit.slider") as mock_slider, \
             patch("search_engine.app_v2.components.sna_time_slider_panel.update_graph_and_metrics") as mock_update:
            mock_slider.return_value = ("2024-06-01", "2024-06-05")
            sna_time_slider_panel.render_sna_time_slider_panel(state)
            mock_slider.assert_called()
            mock_update.assert_called_with(state, ("2024-06-01", "2024-06-05"))

    def test_play_button_advances_network(self):
        """
        Should render a play button and, when pressed, call update_graph_and_metrics at least once for the selected date range.
        Note: Due to Streamlit rerun limitations, we cannot test the full animation loop in a unit test.
        """
        from search_engine.app_v2.components import sna_time_slider_panel
        state = {}
        mock_col = MagicMock()
        with patch("streamlit.columns", return_value=[mock_col, MagicMock()]) as mock_columns, \
             patch("streamlit.slider") as mock_slider, \
             patch("streamlit.session_state", {}) as mock_session_state, \
             patch("search_engine.app_v2.components.sna_time_slider_panel.update_graph_and_metrics") as mock_update, \
             patch("streamlit.rerun") as mock_rerun:
            mock_slider.return_value = (datetime.date(2024, 6, 1), datetime.date(2024, 6, 3))
            # Simulate play button pressed
            mock_col.button.return_value = True
            sna_time_slider_panel.render_sna_time_slider_panel(state)
            # Should update session state for at least one day in range
            assert mock_update.call_count >= 1
            # Should render the play button
            mock_col.button.assert_called_with("Play", disabled=False, key="sna_play_btn")

class TestSnaTabIntegration:
    """
    Integration tests for the overall SNA tab layout and onboarding/help.
    Covers tab visibility, layout, and help elements.
    """
    def test_sna_tab_visible_and_accessible(self):
        """
        Should display the SNA tab alongside the Data Access tab in the main app.
        """
        assert False

    def test_help_and_tooltips_present(self):
        """
        Should render onboarding/help section and tooltips for SNA concepts.
        """
        assert False

class TestSnaUnifiedSampleData:
    """
    Tests for unified sample data usage across all SNA components (task #007).
    Ensures all components use the same sample data loader and that all required fields are present.
    """
    def test_sample_data_loader_fields(self):
        """
        Should load node and edge data with all required fields for all SNA components.
        """
        from search_engine.app_v2 import generate_sample_network_data
        node_df, edge_df = generate_sample_network_data.load_sample_network_data()
        # Required node fields
        required_node_fields = {"id", "date", "valence", "toxic", "political", "slant"}
        # Required edge fields
        required_edge_fields = {"source", "target", "type", "date"}
        assert set(node_df.columns) >= required_node_fields
        assert set(edge_df.columns) >= required_edge_fields
        assert len(node_df) > 0
        assert len(edge_df) > 0

    def test_all_components_use_unified_data(self):
        """
        Should ensure all SNA components (mini-graph, metrics, export, time slider) use the unified sample data loader and reflect the same filtered data.
        """
        from search_engine.app_v2 import generate_sample_network_data
        from search_engine.app_v2.components import (
            sna_mini_graph_panel,
            sna_metric_summary_panel,
            sna_export_simulation_panel,
            sna_time_slider_panel,
        )
        import datetime
        node_df, edge_df = generate_sample_network_data.load_sample_network_data()
        # Simulate a filter state (e.g., date range and edge type)
        state = {
            "date_range": (datetime.date(2024, 6, 2), datetime.date(2024, 6, 3)),
            "edge_type": "retweet",
        }
        # All components should construct their graph from the same filtered data
        def filter_data(node_df, edge_df, state):
            # Filter by date range and edge type
            start, end = state["date_range"]
            edge_mask = (
                (edge_df["date"] >= str(start)) &
                (edge_df["date"] <= str(end)) &
                (edge_df["type"] == state["edge_type"])
            )
            filtered_edges = edge_df[edge_mask]
            node_ids = set(filtered_edges["source"]).union(filtered_edges["target"])
            filtered_nodes = node_df[node_df["id"].isin(node_ids)]
            return filtered_nodes, filtered_edges
        filtered_nodes, filtered_edges = filter_data(node_df, edge_df, state)
        # Build reference graph
        import networkx as nx
        G_ref = nx.from_pandas_edgelist(filtered_edges, "source", "target")
        G_ref.add_nodes_from(filtered_nodes["id"])  # Ensure all nodes present
        # Mini-graph panel should use this graph
        with patch("streamlit.subheader"), patch("streamlit.components.v1.html"), patch("streamlit.caption") as mock_caption:
            sna_mini_graph_panel.render_sna_mini_graph_panel(state)
            # Should display correct node/edge counts or subgraph message
            found = False
            for call in mock_caption.call_args_list:
                arg = str(call.args[0])
                if f"Nodes: {G_ref.number_of_nodes()}, Edges: {G_ref.number_of_edges()}" in arg or "subgraph of largest component" in arg:
                    found = True
                    break
            assert found, "Expected node/edge count or subgraph caption not found."
        # Metric summary panel should use the same graph
        with patch("streamlit.subheader"), patch("streamlit.markdown") as mock_markdown:
            sna_metric_summary_panel.render_sna_metric_summary_panel(state, G_ref)
            markdown_calls = [call[0][0] for call in mock_markdown.call_args_list]
            assert any("Community Count" in c for c in markdown_calls)
        # Export simulation panel should export data from this graph
        with patch("streamlit.subheader"), patch("streamlit.button", return_value=True), patch("streamlit.download_button") as mock_download:
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G_ref)
            assert mock_download.called
        # Time slider panel should update state and trigger graph/metrics update
        with patch("streamlit.slider", return_value=state["date_range"]), patch("search_engine.app_v2.components.sna_time_slider_panel.update_graph_and_metrics") as mock_update:
            sna_time_slider_panel.render_sna_time_slider_panel(state)
            mock_update.assert_called_with(state, state["date_range"])

def columns_side_effect(*args, **kwargs):
    # Handle both st.columns(3) and st.columns([2, 1], gap="large")
    if args and isinstance(args[0], int):
        return [MagicMock() for _ in range(args[0])]
    elif args and isinstance(args[0], (list, tuple)):
        return [MagicMock() for _ in range(len(args[0]))]
    return [MagicMock(), MagicMock()]

class TestSnaTabLayoutStyling:
    """
    Tests for SNA tab layout and styling (Task #008).
    Covers cohesive layout, correct grouping, and visual consistency.
    """
    def test_sna_tab_layout_is_cohesive(self):
        """
        Should render the SNA tab with a visually cohesive layout: correct use of columns, spacing, headers, and color scheme per UI guidelines.
        """
        with patch("streamlit.title") as mock_title, \
             patch("streamlit.caption") as mock_caption, \
             patch("streamlit.columns", side_effect=columns_side_effect) as mock_columns:
            from search_engine.app_v2 import app
            app.main()
            mock_title.assert_any_call("Social Network Analysis (SNA) Tab")
            mock_caption.assert_any_call("Analyze social network structure, polarization, and key actors using interactive controls and visualizations.")
            mock_columns.assert_any_call([2, 1], gap="large")

    def test_sna_panels_are_grouped_and_aligned(self):
        """
        Should group all SNA panels (sidebar, mini-graph, metrics, export, time slider) logically and align them visually in the tab.
        """
        with patch("streamlit.columns", side_effect=columns_side_effect) as mock_columns:
            from search_engine.app_v2 import app
            app.main()
            # Check that columns are created for SNA tab
            mock_columns.assert_any_call([2, 1], gap="large")

    def test_sna_tab_matches_visual_design_guidelines(self):
        """
        Should match the visual design guidelines for spacing, typography, and color as specified in UI_RULES.md.
        """
        # This is a visual check, so we check for presence of key layout elements
        with patch("streamlit.title") as mock_title, \
             patch("streamlit.caption") as mock_caption:
            from search_engine.app_v2 import app
            app.main()
            mock_title.assert_any_call("Social Network Analysis (SNA) Tab")
            mock_caption.assert_any_call("Analyze social network structure, polarization, and key actors using interactive controls and visualizations.")

class TestSnaOnboardingHelp:
    """
    Tests for onboarding/help features in the SNA tab (Task #009).
    Covers tooltips for SNA concepts, 'How to Use' help section, and contextual help.
    """
    def test_tooltips_present_for_sna_concepts(self):
        """
        Should render tooltips for SNA concepts (centrality, community, etc.) in the sidebar, metric summary, and mini-graph panels.
        """
        with patch("streamlit.selectbox") as mock_selectbox, \
             patch("streamlit.slider") as mock_slider, \
             patch("streamlit.checkbox") as mock_checkbox, \
             patch("streamlit.multiselect") as mock_multiselect, \
             patch("streamlit.caption") as mock_caption:
            from search_engine.app_v2.components import sna_sidebar_controls_panel, sna_metric_summary_panel, sna_mini_graph_panel, sna_time_slider_panel, sna_export_simulation_panel
            state = {}
            import networkx as nx
            G = nx.cycle_graph(6)
            sna_sidebar_controls_panel.render_sna_sidebar_controls_panel(state)
            sna_metric_summary_panel.render_sna_metric_summary_panel(state, G)
            sna_mini_graph_panel.render_sna_mini_graph_panel(state)
            sna_time_slider_panel.render_sna_time_slider_panel(state)
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G)
            # Check that help/tooltips/captions are present
            assert any("Type of connection" in str(call.kwargs.get("help", "")) for call in mock_selectbox.call_args_list)
            assert any("Algorithm for detecting" in str(call.kwargs.get("help", "")) for call in mock_selectbox.call_args_list)
            assert any("Metric for ranking" in str(call.kwargs.get("help", "")) for call in mock_selectbox.call_args_list)
            assert any("How many steps" in str(call.kwargs.get("help", "")) for call in mock_slider.call_args_list)
            assert any("Select the range" in str(call.kwargs.get("help", "")) for call in mock_slider.call_args_list)
            assert any("Include posts/nodes marked as toxic" in str(call.kwargs.get("help", "")) for call in mock_checkbox.call_args_list)
            assert any("Emotional tone" in str(call.kwargs.get("help", "")) for call in mock_multiselect.call_args_list)
            assert any("Community Count" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("Assortativity" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("Centrality" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("interactive graph" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("date range slider" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("Download the current network" in str(call.args[0]) for call in mock_caption.call_args_list)

    def test_how_to_use_help_section_present(self):
        """
        Should render a 'How to Use' collapsible help section at the top of the SNA tab.
        """
        with patch("streamlit.expander") as mock_expander:
            from search_engine.app_v2 import app
            app.main()
            mock_expander.assert_any_call("How to Use (SNA Tab)", expanded=False)

    def test_contextual_help_elements_appear(self):
        """
        Should display contextual help elements (e.g., tooltips on hover, help expander) in relevant SNA panels.
        """
        with patch("streamlit.caption") as mock_caption:
            from search_engine.app_v2.components import sna_mini_graph_panel, sna_metric_summary_panel, sna_time_slider_panel, sna_export_simulation_panel
            state = {}
            import networkx as nx
            G = nx.cycle_graph(6)
            sna_mini_graph_panel.render_sna_mini_graph_panel(state)
            sna_metric_summary_panel.render_sna_metric_summary_panel(state, G)
            sna_time_slider_panel.render_sna_time_slider_panel(state)
            sna_export_simulation_panel.render_sna_export_simulation_panel(state, G)
            # Check for contextual help/captions
            assert any("interactive graph" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("Community Count" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("date range slider" in str(call.args[0]) for call in mock_caption.call_args_list)
            assert any("Download the current network" in str(call.args[0]) for call in mock_caption.call_args_list)

class TestSnaDemoInstructions:
    """
    Tests for demo instructions in the SNA tab (Task #010).
    Covers presence of a collapsible section, content, and accessibility for stakeholders.
    """
    def test_demo_instructions_section_present(self):
        """
        Should render a collapsible 'Demo instructions' section in the SNA tab.
        """
        with patch("streamlit.expander") as mock_expander:
            from search_engine.app_v2 import app
            app.main()
            mock_expander.assert_any_call("Demo instructions", expanded=False)

    def test_demo_instructions_content(self):
        """
        Should include instructions for running the demo, navigating the SNA tab, and sample research questions/flow script.
        """
        with patch("streamlit.markdown") as mock_markdown:
            from search_engine.app_v2 import app
            app.main()
            markdown_calls = [str(call.args[0]) for call in mock_markdown.call_args_list]
            assert any("How to Run the Demo" in c for c in markdown_calls)
            assert any("Sample Research Questions" in c for c in markdown_calls)
            assert any("Demo Flow Script" in c for c in markdown_calls)

    def test_demo_instructions_accessible(self):
        """
        Should allow stakeholders to access the demo instructions section without assistance (clearly visible and easy to use).
        """
        with patch("streamlit.expander") as mock_expander:
            from search_engine.app_v2 import app
            app.main()
            # The expander should be present and not hidden
            mock_expander.assert_any_call("Demo instructions", expanded=False) 