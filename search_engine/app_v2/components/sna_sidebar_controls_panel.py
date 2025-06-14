import streamlit as st
from typing import Any, Dict, List


def render_sna_sidebar_controls_panel(state: Dict[str, Any]) -> None:
    """
    Render the sidebar controls panel for the Social Network Analysis (SNA) tab.
    Includes dropdowns, sliders, checkbox groups, filter chips, and submit button.
    Args:
        state (Dict[str, Any]): The state object for managing filter selections and UI state.
    """
    st.header("SNA Sidebar Controls")
    # Dropdowns
    edge_type = st.selectbox(
        "Edge Type",
        options=["retweet", "reply", "mention", "follow"],
        key="sna_edge_type",
        help="Type of connection between users (e.g., retweet, reply, mention, follow).",
    )
    community_algo = st.selectbox(
        "Community Algorithm",
        options=["Louvain", "Label Propagation", "Girvan-Newman"],
        key="sna_community_algo",
        help="Algorithm for detecting communities/groups in the network.",
    )
    centrality_metric = st.selectbox(
        "Centrality Metric",
        options=["Degree", "Betweenness", "Closeness", "Eigenvector"],
        key="sna_centrality_metric",
        help="Metric for ranking node importance (e.g., Degree = most connections, Betweenness = most bridging paths).",
    )
    # Sliders
    hop_depth = st.slider(
        "Hop Depth",
        min_value=1,
        max_value=5,
        value=2,
        key="sna_hop_depth",
        help="How many steps away from a node to include in the subgraph.",
    )
    time_range = st.slider(
        "Time Range (days)",
        min_value=1,
        max_value=14,
        value=(1, 14),
        key="sna_time_range",
        help="Select the range of days to include in the analysis.",
    )
    # Checkbox groups
    st.subheader("Content Filters")
    toxic = st.checkbox(
        "Toxic", key="sna_toxic", help="Include posts/nodes marked as toxic."
    )
    not_toxic = st.checkbox(
        "Not Toxic",
        key="sna_not_toxic",
        help="Include posts/nodes not marked as toxic.",
    )
    valence = st.multiselect(
        "Valence",
        options=["positive", "neutral", "negative"],
        key="sna_valence",
        help="Emotional tone of content: positive, neutral, or negative.",
    )
    political = st.checkbox(
        "Political",
        key="sna_political",
        help="Include posts/nodes marked as political.",
    )
    not_political = st.checkbox(
        "Not Political",
        key="sna_not_political",
        help="Include posts/nodes not marked as political.",
    )
    slant = st.multiselect(
        "Political Slant",
        options=["left", "center", "right", "unclear"],
        key="sna_slant",
        help="Political orientation: left, center, right, or unclear. Only relevant if 'Political' is selected.",
    )

    # Store selections in state
    state["edge_type"] = edge_type
    state["community_algo"] = community_algo
    state["centrality_metric"] = centrality_metric
    state["hop_depth"] = hop_depth
    state["time_range"] = time_range
    state["toxic"] = toxic
    state["not_toxic"] = not_toxic
    state["valence"] = valence
    state["political"] = political
    state["not_political"] = not_political
    state["slant"] = slant

    # Build selected filters list
    selected_filters: List[str] = []
    selected_filters.append(f"Edge: {edge_type}")
    selected_filters.append(f"Community: {community_algo}")
    selected_filters.append(f"Centrality: {centrality_metric}")
    selected_filters.append(f"Hops: {hop_depth}")
    selected_filters.append(f"Time: {time_range[0]}-{time_range[1]}")
    if toxic:
        selected_filters.append("Toxic")
    if not_toxic:
        selected_filters.append("Not Toxic")
    if valence:
        selected_filters.append(f"Valence: {', '.join(valence)}")
    if political:
        selected_filters.append("Political")
    if not_political:
        selected_filters.append("Not Political")
    if slant:
        selected_filters.append(f"Slant: {', '.join(slant)}")

    # Chips UI for selected filters (deselectable)
    st.markdown("**Selected Filters:**")
    for i, filt in enumerate(selected_filters):
        col1, col2 = st.columns([8, 1])
        col1.markdown(f"- {filt}")
        if col2.button("x", key=f"sna_chip_{i}"):
            # Remove the filter from state (simple logic for demo)
            # In a real app, map filter text to state keys for removal
            st.warning(f"[Demo] Deselect '{filt}' (not implemented)")

    # Submit button
    if st.button("Submit", key="sna_submit_btn"):
        summary = (
            f"Edge: {edge_type}, Community: {community_algo}, Centrality: {centrality_metric}, "
            f"Hops: {hop_depth}, Time: {time_range[0]}-{time_range[1]}, "
            f"Toxic: {toxic}, Not Toxic: {not_toxic}, Valence: {valence}, "
            f"Political: {political}, Not Political: {not_political}, Slant: {slant}"
        )
        st.success(f"Parameters chosen: {summary}")
