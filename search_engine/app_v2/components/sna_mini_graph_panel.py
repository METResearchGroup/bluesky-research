import streamlit as st
from typing import Any, Dict
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
import random
from search_engine.app_v2 import generate_sample_network_data
import datetime
import os

CACHE_DIR = os.path.join(os.path.dirname(__file__), "../graph_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

MAX_NODES = 100  # Maximum nodes to display in the mini-graph

SLANT_COLORS = {
    "left": "#6fa8dc",
    "center": "#f6b26b",
    "right": "#93c47d",
    "unclear": "#cccccc",
}


def filter_sample_graph(state: Dict[str, Any]) -> nx.Graph:
    """
    Construct a networkx graph from the unified sample node/edge data, filtered by state.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
    Returns:
        nx.Graph: The filtered networkx graph.
    """
    node_df, edge_df = generate_sample_network_data.load_sample_network_data()
    # Filter by date range and edge type
    date_range = state.get(
        "date_range", (datetime.date(2024, 6, 1), datetime.date(2024, 6, 14))
    )
    edge_type = state.get("edge_type", None)
    start, end = date_range
    edge_mask = (edge_df["date"] >= str(start)) & (edge_df["date"] <= str(end))
    if edge_type:
        edge_mask &= edge_df["type"] == edge_type
    filtered_edges = edge_df[edge_mask]
    node_ids = set(filtered_edges["source"]).union(filtered_edges["target"])
    filtered_nodes = node_df[node_df["id"].isin(node_ids)]
    G = nx.from_pandas_edgelist(filtered_edges, "source", "target")
    for _, row in filtered_nodes.iterrows():
        G.add_node(row["id"], **row.to_dict())
    return G


@st.cache_data(show_spinner=False)
def get_pyvis_html(filtered_edges, filtered_nodes, cache_key: str, mode: str) -> str:
    """
    Generate or load cached pyvis HTML for the given graph state (with subgraphing for large graphs).
    Args:
        filtered_edges: DataFrame of filtered edges.
        filtered_nodes: DataFrame of filtered nodes.
        cache_key: Unique key for this graph state (e.g., date range, edge type, subgraph seed).
        mode: 'full' or 'subgraph'.
    Returns:
        str: HTML string for the pyvis graph.
    """
    html_path = os.path.join(CACHE_DIR, f"mini_graph_{cache_key}_{mode}.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    # Otherwise, generate and cache
    G = nx.from_pandas_edgelist(filtered_edges, "source", "target")
    for _, row in filtered_nodes.iterrows():
        G.add_node(row["id"], **row.to_dict())
    # Subgraphing: if too large, show largest connected component or random sample
    if mode == "subgraph" and G.number_of_nodes() > MAX_NODES:
        components_list = list(nx.connected_components(G))
        largest_cc = max(components_list, key=len)
        if len(largest_cc) > MAX_NODES:
            sampled_nodes = set(random.sample(list(largest_cc), MAX_NODES))
        else:
            sampled_nodes = set(largest_cc)
        SG = G.subgraph(sampled_nodes).copy()
    else:
        SG = G
    net = Network(height="400px", width="100%", bgcolor="#ffffff", font_color="#222222")
    net.barnes_hut()
    for node, data in SG.nodes(data=True):
        # Enhanced tooltip with all node attributes, nicely formatted
        tooltip = (
            f"<b>ID:</b> {data.get('id', node)}<br>"
            f"<b>Valence:</b> {data.get('valence', '')}<br>"
            f"<b>Slant:</b> {data.get('slant', '')}<br>"
            f"<b>Toxic:</b> {'✅' if data.get('toxic', False) else '❌'}<br>"
            f"<b>Political:</b> {'✅' if data.get('political', False) else '❌'}<br>"
            f"<b>Date:</b> {data.get('date', '')}"
        )
        group = data.get("slant", "unclear")
        net.add_node(node, label="", title=tooltip, group=group)
    for u, v in SG.edges():
        net.add_edge(u, v)
    net.set_options("""
{
  "nodes": {
    "shape": "dot",
    "size": 18,
    "font": { "size": 12, "color": "#222", "vadjust": 2 },
    "borderWidth": 2
  },
  "edges": {
    "width": 1,
    "color": { "color": "#bbb", "highlight": "#222" }
  },
  "groups": {
    "left": { "color": { "background": "#6fa8dc", "border": "#3d85c6" } },
    "center": { "color": { "background": "#f6b26b", "border": "#e69138" } },
    "right": { "color": { "background": "#93c47d", "border": "#38761d" } },
    "unclear": { "color": { "background": "#cccccc", "border": "#888888" } }
  },
  "physics": { "barnesHut": { "gravitationalConstant": -20000, "springLength": 120 } }
}
""")
    net.save_graph(html_path)
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    return html


def render_sna_mini_graph_panel(state: Dict[str, Any]) -> None:
    """
    Render the Mini-Graph Preview panel for the Social Network Analysis (SNA) tab.
    Shows a subgraph if the full graph is too large for fast rendering.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
    """
    st.subheader("Mini-Graph Preview")
    st.caption(
        "This interactive graph shows a sample of the network based on your filters. Node color = political slant. Hover nodes for details. Use the buttons to toggle between full graph and subgraph views."
    )
    # Full/subgraph toggle
    if "sna_graph_mode" not in st.session_state:
        st.session_state["sna_graph_mode"] = "subgraph"
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button(
            "Show Full Graph", disabled=st.session_state["sna_graph_mode"] == "full"
        ):
            st.session_state["sna_graph_mode"] = "full"
    with col2:
        if st.button(
            "Show Subgraph", disabled=st.session_state["sna_graph_mode"] == "subgraph"
        ):
            st.session_state["sna_graph_mode"] = "subgraph"
    mode = st.session_state["sna_graph_mode"]

    node_df, edge_df = generate_sample_network_data.load_sample_network_data()
    date_range = state.get(
        "date_range", (datetime.date(2024, 6, 1), datetime.date(2024, 6, 14))
    )
    edge_type = state.get("edge_type", None)
    start, end = date_range
    edge_mask = (edge_df["date"] >= str(start)) & (edge_df["date"] <= str(end))
    if edge_type:
        edge_mask &= edge_df["type"] == edge_type
    filtered_edges = edge_df[edge_mask]
    node_ids = set(filtered_edges["source"]).union(filtered_edges["target"])
    filtered_nodes = node_df[node_df["id"].isin(node_ids)]
    node_hash = str(hash(frozenset(node_ids))) if len(node_ids) > MAX_NODES else "full"
    cache_key = f"{start}_{end}_{edge_type or 'all'}_{node_hash}"
    html = get_pyvis_html(filtered_edges, filtered_nodes, cache_key, mode)
    components.html(html, height=420, scrolling=False)
    if mode == "subgraph" and len(node_ids) > MAX_NODES:
        st.caption(
            f"Nodes: {min(len(node_ids), MAX_NODES)}, Edges: (subgraph of largest component, sampled if >{MAX_NODES} nodes)"
        )
    elif mode == "full":
        st.caption(f"Nodes: {len(node_ids)}, Edges: {len(filtered_edges)} (full graph)")
    else:
        st.caption(f"Nodes: {len(node_ids)}, Edges: {len(filtered_edges)}")
    # Color legend
    st.markdown("**Node Color Legend (Slant):**")
    legend_html = "<div style='display:flex;gap:16px;'>"
    for slant, color in SLANT_COLORS.items():
        legend_html += f"<div style='display:flex;align-items:center;'><span style='display:inline-block;width:16px;height:16px;background:{color};border-radius:8px;margin-right:6px;border:1px solid #888;'></span>{slant.capitalize()}</div>"
    legend_html += "</div>"
    st.markdown(legend_html, unsafe_allow_html=True)
