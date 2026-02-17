import csv
from io import BytesIO, StringIO
from typing import Any, Dict

import networkx as nx
import streamlit as st


CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")


def _to_safe_csv_cell(value: Any) -> str:
    """
    Convert a value to a CSV-safe cell value.

    Prefix values that start with spreadsheet formula characters to reduce
    formula-injection risk when files are opened in spreadsheet tools.
    """
    value_as_text = str(value)
    if value_as_text.startswith(CSV_FORMULA_PREFIXES):
        return f"'{value_as_text}"
    return value_as_text


def graph_to_edge_csv(G: nx.Graph) -> str:
    """
    Convert a networkx graph to a CSV string of edges.
    Args:
        G (nx.Graph): The graph to convert.
    Returns:
        str: CSV string with columns 'source,target'.
    """
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["source", "target"])

    normalized_edges = []
    for source, target in G.edges():
        if not G.is_directed():
            source, target = sorted((source, target), key=str)
        normalized_edges.append((source, target))
    sorted_edges = sorted(normalized_edges, key=lambda edge: (str(edge[0]), str(edge[1])))

    for source, target in sorted_edges:
        writer.writerow([_to_safe_csv_cell(source), _to_safe_csv_cell(target)])
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
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["node", "degree"])
    sorted_nodes = sorted(G.nodes(), key=str)
    for node in sorted_nodes:
        writer.writerow([_to_safe_csv_cell(node), G.degree[node]])
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
    nx.write_gexf(G, output, encoding="utf-8")
    return output.getvalue()


def _build_export_suffix(state: Dict[str, Any]) -> str:
    """
    Build a lightweight context suffix for exported filenames.
    """
    date_range = state.get("date_range")
    edge_type = state.get("edge_type")

    suffix_parts = []
    if edge_type:
        suffix_parts.append(str(edge_type).replace(" ", "_"))
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        suffix_parts.append(f"{start_date}_{end_date}")

    return "_".join(suffix_parts) if suffix_parts else "current_snapshot"


def render_sna_export_simulation_panel(state: Dict[str, Any], G: nx.Graph) -> None:
    """
    Render the Export Simulation panel for the SNA tab.
    Renders buttons for exporting edge list, node metrics, and GEXF, and triggers simulated file downloads.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
        G (nx.Graph): The current networkx graph for which to export data.
    """
    st.subheader("Export Simulation")
    st.caption(
        "Download the current network as an edge list (CSV), node metrics (CSV), or full network (GEXF) for offline analysis. Each file contains data for the current graph snapshot."
    )

    has_graph_data = G.number_of_nodes() > 0
    if not has_graph_data:
        st.info("No graph data available for export with the current filters.")

    suffix = _build_export_suffix(state)
    if has_graph_data:
        edge_csv = graph_to_edge_csv(G)
        node_metrics_csv = graph_to_node_metrics_csv(G)
        gexf_bytes = graph_to_gexf_bytes(G)
    else:
        edge_csv = "source,target\n"
        node_metrics_csv = "node,degree\n"
        gexf_bytes = b""

    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button(
            label="Download Edge List (CSV)",
            data=edge_csv,
            file_name=f"edge_list_{suffix}.csv",
            mime="text/csv",
            key="sna_export_edge_list",
            disabled=not has_graph_data,
        )
    with col2:
        st.download_button(
            label="Download Node Metrics (CSV)",
            data=node_metrics_csv,
            file_name=f"node_metrics_{suffix}.csv",
            mime="text/csv",
            key="sna_export_node_metrics",
            disabled=not has_graph_data,
        )
    with col3:
        st.download_button(
            label="Download GEXF",
            data=gexf_bytes,
            file_name=f"network_{suffix}.gexf",
            mime="application/gexf+xml",
            key="sna_export_gexf",
            disabled=not has_graph_data,
        )
