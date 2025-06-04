import streamlit as st
import pandas as pd


def main() -> None:
    """
    Render the MVP Streamlit UI for the Bluesky Semantic Search Engine.
    Features:
    - Title, subtitle, description
    - Query input with explicit submit button (in a form)
    - Clickable example queries (showing actual query text)
    - Results area with dummy response and expandable table
    - Feedback section (text, thumbs up/down emojis, submit button)
    """
    st.title("Bluesky Semantic Search")
    st.markdown(
        "A semantic search engine for Bluesky data.\n\n"
        "This tool allows you to submit freeform queries and explore Bluesky posts using semantic understanding. "
        "Built for social scientists and data explorers."
    )
    st.subheader("Search the Bluesky dataset")

    # Session state for query and feedback
    if "query" not in st.session_state:
        st.session_state["query"] = ""
    if "show_results" not in st.session_state:
        st.session_state["show_results"] = False
    if "feedback_text" not in st.session_state:
        st.session_state["feedback_text"] = ""
    if "feedback_sentiment" not in st.session_state:
        st.session_state["feedback_sentiment"] = None

    # Example queries
    example_queries = [
        "What are the most liked posts this week?",
        "Summarize what people are saying about climate.",
        "What did @user123 post about the election?",
    ]

    st.markdown("**Example queries:**")
    cols = st.columns(len(example_queries))
    for i, (col, ex_query) in enumerate(zip(cols, example_queries)):
        if col.button(ex_query, key=f"exq_{i}"):
            st.session_state["query"] = ex_query
            st.session_state["show_results"] = False

    # Query input and submit button in a form
    with st.form(key="query_form"):
        query = st.text_input(
            "Enter your query:", value=st.session_state["query"], key="query_input"
        )
        submit = st.form_submit_button("Submit")
        if submit:
            st.session_state["query"] = query
            st.session_state["show_results"] = True

    st.markdown("### Results")
    if st.session_state.get("show_results", False) and st.session_state["query"]:
        st.markdown(
            f"Dummy response: Example result for your query '{st.session_state['query']}'."
        )
        dummy_data = pd.DataFrame(
            {
                "user": ["@alice", "@bob", "@carol"],
                "post": ["Great week!", "Climate is trending.", "Election talk."],
                "likes": [42, 37, 29],
            }
        )
        with st.expander("Show example results table"):
            st.dataframe(dummy_data)
    else:
        st.markdown("Results will appear here after you submit a query.")

    # Feedback section
    st.markdown("---")
    st.markdown("#### Feedback")
    feedback_text = st.text_input(
        "Leave feedback:", value=st.session_state["feedback_text"], key="feedback_text"
    )
    sentiment = st.radio(
        "Feedback sentiment:",
        ("👍 Thumbs Up", "👎 Thumbs Down"),
        key="feedback_sentiment",
        horizontal=True,
    )
    submit_feedback = st.button("Submit Feedback", key="submit_feedback")
    if submit_feedback:
        st.session_state["feedback_text"] = feedback_text
        st.session_state["feedback_sentiment"] = sentiment
        st.success("Thank you for your feedback!")


if __name__ == "__main__":
    main()
