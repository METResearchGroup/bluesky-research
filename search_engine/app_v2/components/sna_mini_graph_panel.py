import streamlit as st
from typing import Any, Dict
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
import random


def generate_clustered_broker_graph() -> nx.Graph:
    """
    Generate a networkx graph with two dense clusters and 4-5 broker nodes connecting them.
    Returns:
        nx.Graph: The generated networkx graph.
    """
    G = nx.Graph()
    cluster_size = 22
    num_brokers = 6
    # Cluster 1
    cluster1 = [f"User {i+1}" for i in range(cluster_size)]
    # Cluster 2
    cluster2 = [f"User {i+1+cluster_size}" for i in range(cluster_size)]
    # Brokers
    brokers = [f"Broker {i+1}" for i in range(num_brokers)]
    # Add nodes
    for node in cluster1:
        G.add_node(node, group="Cluster 1")
    for node in cluster2:
        G.add_node(node, group="Cluster 2")
    for node in brokers:
        G.add_node(node, group="Broker")
    # Add dense intra-cluster edges
    for i in range(cluster_size):
        for j in range(i + 1, cluster_size):
            if random.random() < 0.5:
                G.add_edge(cluster1[i], cluster1[j])
            if random.random() < 0.5:
                G.add_edge(cluster2[i], cluster2[j])
    # Connect brokers to both clusters
    for broker in brokers:
        # Connect to 3-5 random nodes in each cluster
        c1_neighbors = random.sample(cluster1, k=random.randint(3, 5))
        c2_neighbors = random.sample(cluster2, k=random.randint(3, 5))
        for n in c1_neighbors + c2_neighbors:
            G.add_edge(broker, n)
    return G


def render_sna_mini_graph_panel(state: Dict[str, Any]) -> None:
    """
    Render the Mini-Graph Preview panel for the Social Network Analysis (SNA) tab.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
    """
    st.subheader("Mini-Graph Preview")
    G = generate_clustered_broker_graph()
    net = Network(height="400px", width="100%", bgcolor="#ffffff", font_color="#222222")
    net.barnes_hut()
    # Add nodes and edges to pyvis network
    for node, data in G.nodes(data=True):
        group = data.get("group", "Other")
        # Hide label, show as tooltip
        net.add_node(node, label="", title=node, group=group)
    for u, v in G.edges():
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
    "Cluster 1": { "color": { "background": "#6fa8dc", "border": "#3d85c6" } },
    "Cluster 2": { "color": { "background": "#f6b26b", "border": "#e69138" } },
    "Broker": { "color": { "background": "#93c47d", "border": "#38761d" } }
  },
  "physics": { "barnesHut": { "gravitationalConstant": -20000, "springLength": 120 } }
}
""")
    net.save_graph("sna_mini_graph.html")
    with open("sna_mini_graph.html", "r", encoding="utf-8") as f:
        html = f.read()
    components.html(html, height=420, scrolling=False)
    st.caption(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")
