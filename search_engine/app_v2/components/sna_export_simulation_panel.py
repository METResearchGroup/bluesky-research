import streamlit as st
from typing import Any, Dict
import networkx as nx
from io import StringIO, BytesIO


def graph_to_edge_csv(G: nx.Graph) -> str:
    """
    Convert a networkx graph to a CSV string of edges.
    Args:
        G (nx.Graph): The graph to convert.
    Returns:
        str: CSV string with columns 'source,target'.
    """
    output = StringIO()
    output.write("source,target\n")
    for u, v in G.edges():
        output.write(f"{u},{v}\n")
    return output.getvalue()


def graph_to_node_metrics_csv(G: nx.Graph) -> str:
    """
    Convert a networkx graph to a CSV string of node metrics (degree).
    Args:
        G (nx.Graph): The graph to convert.
    Returns:
        str: CSV string with columns 'node,degree'.
    """
    output = StringIO()
    output.write("node,degree\n")
    for node in G.nodes():
        output.write(f"{node},{G.degree[node]}\n")
    return output.getvalue()


def graph_to_gexf_bytes(G: nx.Graph) -> bytes:
    """
    Convert a networkx graph to GEXF format as bytes.
    Args:
        G (nx.Graph): The graph to convert.
    Returns:
        bytes: GEXF file content.
    """
    output = BytesIO()
    nx.write_gexf(G, output)
    return output.getvalue()


def render_sna_export_simulation_panel(state: Dict[str, Any], G: nx.Graph) -> None:
    """
    Render the Export Simulation panel for the SNA tab.
    Renders buttons for exporting edge list, node metrics, and GEXF, and triggers simulated file downloads.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
        G (nx.Graph): The current networkx graph for which to export data.
    """
    st.subheader("Export Simulation")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Download Edge List (CSV)"):
            csv = graph_to_edge_csv(G)
            st.download_button(
                label="Download Edge List (CSV)",
                data=csv,
                file_name="edge_list.csv",
                mime="text/csv",
            )
    with col2:
        if st.button("Download Node Metrics (CSV)"):
            csv = graph_to_node_metrics_csv(G)
            st.download_button(
                label="Download Node Metrics (CSV)",
                data=csv,
                file_name="node_metrics.csv",
                mime="text/csv",
            )
    with col3:
        if st.button("Download GEXF"):
            gexf = graph_to_gexf_bytes(G)
            st.download_button(
                label="Download GEXF",
                data=gexf,
                file_name="network.gexf",
                mime="application/octet-stream",
            )
