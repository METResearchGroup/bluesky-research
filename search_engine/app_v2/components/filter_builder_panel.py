import streamlit as st
from filter_state import FilterState
import datetime


def render_filter_builder_panel(filter_state: FilterState) -> None:
    """
    Renders the Filter Builder Panel with all filter accordions and add logic.

    Args:
        filter_state: The FilterState object managing current filters.
    """
    st.header("Filter Builder Panel")

    # --- Temporal ---
    with st.expander("Temporal", expanded=False):
        today = datetime.date.today()
        start_date = st.date_input(
            "Start Date",
            value=today - datetime.timedelta(days=7),
            key="temporal_start_date",
        )
        end_date = st.date_input("End Date", value=today, key="temporal_end_date")
        if st.button("Set Date Range"):
            filter_state.add_filter(
                "Temporal", "date_range", f"{start_date} to {end_date}"
            )
            st.rerun()

    # --- Content ---
    with st.expander("Content", expanded=False):
        if "keywords" not in st.session_state:
            st.session_state["keywords"] = []
        keyword_input = st.text_input(
            "Keyword (free text)", key="content_keyword_input"
        )
        if st.button("Add Keyword") and keyword_input:
            keywords = st.session_state["keywords"]
            if keyword_input not in keywords:
                keywords.append(keyword_input)
                st.session_state["keywords"] = keywords
                filter_state.add_filter("Content", "keywords", keywords)
                st.rerun()
        keywords = st.session_state["keywords"]
        if keywords:
            st.write("Current keywords:")
            st.write(", ".join(keywords))
            for i, kw in enumerate(keywords):
                if st.button(f"Remove '{kw}'", key=f"remove_keyword_{i}"):
                    keywords.pop(i)
                    st.session_state["keywords"] = keywords
                    if keywords:
                        filter_state.add_filter("Content", "keywords", keywords)
                    else:
                        filter_state.remove_filter("Content", "keywords")
                    st.rerun()
        else:
            st.write("No keywords added.")

    # --- Hashtags ---
    with st.expander("Hashtags", expanded=False):
        if "hashtags" not in st.session_state:
            st.session_state["hashtags"] = []
        hashtag_input = st.text_input(
            "Add hashtag (with or without #)", key="hashtag_input"
        )
        if st.button("Add Hashtag") and hashtag_input:
            hashtags = st.session_state["hashtags"]
            tag = (
                hashtag_input if hashtag_input.startswith("#") else f"#{hashtag_input}"
            )
            if tag not in hashtags:
                hashtags.append(tag)
                st.session_state["hashtags"] = hashtags
                filter_state.add_filter("Content", "hashtags", hashtags)
                st.rerun()
        hashtags = st.session_state["hashtags"]
        if hashtags:
            st.write("Current hashtags:")
            st.write(", ".join(hashtags))
            for i, tag in enumerate(hashtags):
                if st.button(f"Remove {tag}", key=f"remove_hashtag_{i}"):
                    hashtags.pop(i)
                    st.session_state["hashtags"] = hashtags
                    if hashtags:
                        filter_state.add_filter("Content", "hashtags", hashtags)
                    else:
                        filter_state.remove_filter("Content", "hashtags")
                    st.rerun()
        else:
            st.write("No hashtags added.")

    # --- Sentiment ---
    with st.expander("Sentiment", expanded=False):
        valence = st.radio(
            "Valence",
            options=["Any", "positive", "neutral", "negative"],
            key="sentiment_valence",
        )
        toxicity = st.radio(
            "Toxicity",
            options=["Any", "Toxic", "Not Toxic", "Uncertain"],
            key="sentiment_toxicity",
        )
        if st.button("Submit Sentiment Filters"):
            if valence != "Any":
                filter_state.add_filter("Sentiment", "valence", valence)
            else:
                filter_state.remove_filter("Sentiment", "valence")
            if toxicity != "Any":
                filter_state.add_filter("Sentiment", "toxicity", toxicity)
            else:
                filter_state.remove_filter("Sentiment", "toxicity")
            st.rerun()

    # --- Political ---
    with st.expander("Political", expanded=False):
        political = st.radio(
            "Political Content", options=["Any", "Yes", "No"], key="political_content"
        )
        slant = st.radio(
            "Slant",
            options=["Any", "left", "center", "right", "unclear"],
            key="political_slant",
        )
        if st.button("Submit Political Filters"):
            if political != "Any":
                filter_state.add_filter("Political", "political", political)
            else:
                filter_state.remove_filter("Political", "political")
            if slant != "Any":
                filter_state.add_filter("Political", "slant", slant)
            else:
                filter_state.remove_filter("Political", "slant")
            st.rerun()

    # --- User ---
    with st.expander("User", expanded=False):
        if "user_handles" not in st.session_state:
            st.session_state["user_handles"] = []
        user_input = st.text_input("Add user handle", key="user_handle_input")
        if st.button("Add User Handle") and user_input:
            handles = st.session_state["user_handles"]
            if user_input not in handles:
                handles.append(user_input)
                st.session_state["user_handles"] = handles
                filter_state.add_filter("User", "handles", handles)
                st.rerun()
        handles = st.session_state["user_handles"]
        if handles:
            st.write("Current user handles:")
            st.write(", ".join(handles))
            for i, handle in enumerate(handles):
                if st.button(f"Remove {handle}", key=f"remove_handle_{i}"):
                    handles.pop(i)
                    st.session_state["user_handles"] = handles
                    if handles:
                        filter_state.add_filter("User", "handles", handles)
                    else:
                        filter_state.remove_filter("User", "handles")
                    st.rerun()
        else:
            st.write("No user handles added.")

    # --- Engagement ---
    with st.expander("Engagement", expanded=False):
        engagement_type = st.radio(
            "Engagement Type",
            options=["Any", "post", "like", "repost", "comment", "follow", "block"],
            key="engagement_type",
        )
        if st.button("Submit Engagement Filter"):
            if engagement_type != "Any":
                filter_state.add_filter("Engagement", "type", engagement_type)
            else:
                filter_state.remove_filter("Engagement", "type")
            st.rerun()

    # --- Network ---
    with st.expander("Network", expanded=False):
        hops = st.selectbox(
            "Graph Distance (hops)", options=["Any", 1, 2], key="network_hops"
        )
        if hops != "Any":
            filter_state.add_filter("Network", "hops", hops)
        else:
            filter_state.remove_filter("Network", "hops")

    # --- Max Results ---
    max_results = st.number_input(
        "Max Results",
        min_value=1,
        max_value=100000,
        value=1000,
        step=100,
        key="max_results",
    )
    filter_state.add_filter("General", "max_results", max_results)

    # --- Submit Filters ---
    if st.button("Submit Filters"):
        summary = build_human_readable_summary(filter_state)
        st.success(summary)


def build_human_readable_summary(filter_state: FilterState) -> str:
    """
    Build a human-readable summary string of the selected filters.
    Args:
        filter_state: The FilterState object.
    Returns:
        str: Human-readable summary.
    """
    f = filter_state.filters
    parts = []
    # Date range
    if "Temporal" in f and "date_range" in f["Temporal"]:
        parts.append(f"in the date range {f['Temporal']['date_range']}")
    # Content
    if "Content" in f and "keywords" in f["Content"]:
        parts.append(f"containing the keywords: {', '.join(f['Content']['keywords'])}")
    if "Content" in f and "hashtags" in f["Content"]:
        parts.append(f"with hashtags: {', '.join(f['Content']['hashtags'])}")
    # Sentiment
    if "Sentiment" in f:
        if "valence" in f["Sentiment"]:
            parts.append(f"with valence '{f['Sentiment']['valence']}'")
        if "toxicity" in f["Sentiment"]:
            parts.append(f"with toxicity '{f['Sentiment']['toxicity']}'")
    # Political
    if "Political" in f:
        if "political" in f["Political"]:
            parts.append(f"that are political: {f['Political']['political']}")
        if "slant" in f["Political"]:
            parts.append(f"with slant '{f['Political']['slant']}'")
    # User
    if "User" in f and "handles" in f["User"]:
        handles = f["User"]["handles"]
        if handles:
            parts.append(f"by users: {', '.join(handles)}")
    # Engagement
    if "Engagement" in f and "type" in f["Engagement"]:
        parts.append(f"with engagement type '{f['Engagement']['type']}'")
    # Network
    if "Network" in f and "hops" in f["Network"]:
        parts.append(f"within {f['Network']['hops']} hops in the network")
    # Max results
    if "General" in f and "max_results" in f["General"]:
        parts.append(f"(max results: {f['General']['max_results']})")
    if not parts:
        return "No filters selected."
    return "You've asked for data " + ", ".join(parts) + "."
