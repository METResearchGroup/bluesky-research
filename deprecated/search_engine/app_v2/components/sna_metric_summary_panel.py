import streamlit as st
from typing import Any, Dict
import networkx as nx


def compute_sample_metrics(G: nx.Graph) -> Dict[str, Any]:
    """
    Compute metrics for the given graph: top 5 central nodes (by degree), community count, and assortativity.
    Args:
        G (nx.Graph): The networkx graph to analyze.
    Returns:
        Dict[str, Any]: Dictionary with 'top_central_nodes', 'community_count', and 'assortativity'.
    """
    if G.number_of_nodes() == 0:
        return {
            "top_central_nodes": [],
            "community_count": 0,
            "assortativity": float("nan"),
        }
    degree_centrality = nx.degree_centrality(G)
    top_central_nodes = sorted(
        degree_centrality.items(), key=lambda x: x[1], reverse=True
    )[:5]
    communities = list(nx.connected_components(G))
    community_count = len(communities)
    assortativity = nx.degree_assortativity_coefficient(G)
    return {
        "top_central_nodes": top_central_nodes,
        "community_count": community_count,
        "assortativity": assortativity,
    }


def render_sna_metric_summary_panel(state: Dict[str, Any], G: nx.Graph) -> None:
    """
    Render the Metric Summary Panel for the Social Network Analysis (SNA) tab.
    Displays top 5 central nodes, community count, and assortativity for the current graph snapshot.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
        G (nx.Graph): The current networkx graph for which to compute metrics.
    """
    st.subheader("Metric Summary Panel")
    metrics = compute_sample_metrics(G)
    st.markdown(f"**Community Count:** {metrics['community_count']}")
    st.caption(
        "Community Count: Number of distinct groups or clusters in the network, as detected by the selected community algorithm."
    )
    st.markdown(f"**Assortativity:** {metrics['assortativity']:.2f}")
    st.caption(
        "Assortativity: Measures the tendency of nodes to connect to similar nodes (e.g., high value = homophily, low value = mixing). Range: -1 to 1."
    )
    st.markdown("**Top 5 Central Nodes (by degree):**")
    st.caption(
        "Centrality: Nodes with high centrality are important for information flow or influence. Degree = most connections."
    )
    for node, centrality in metrics["top_central_nodes"]:
        st.markdown(f"- {node} (centrality: {centrality:.2f})")
