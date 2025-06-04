import streamlit as st
import pandas as pd
import re
import time
import requests
import hashlib
from datetime import datetime
import io


def is_sql_injection(text: str) -> bool:
    """Detects SQL injection-like patterns."""
    sql_patterns = [
        r"select .* from",
        r"drop table",
        r"insert into",
        r"delete from",
        r"--",
        r";",
    ]
    text_lower = text.lower()
    return any(re.search(pat, text_lower) for pat in sql_patterns)


def contains_profanity(text: str) -> bool:
    """Detects basic profanity (expand as needed)."""
    profanities = ["damn", "hell", "shit", "fuck"]
    text_lower = text.lower()
    return any(word in text_lower for word in profanities)


def is_too_long(text: str, cap: int = 1000) -> bool:
    return len(text) > cap


def anti_spam_guard() -> bool:
    """Returns True if user is spamming (more than 5 queries in 60s)."""
    now = time.time()
    if "query_timestamps" not in st.session_state:
        st.session_state["query_timestamps"] = []
    # Remove timestamps older than 60s
    st.session_state["query_timestamps"] = [
        t for t in st.session_state["query_timestamps"] if now - t < 60
    ]
    if len(st.session_state["query_timestamps"]) >= 5:
        return True
    st.session_state["query_timestamps"].append(now)
    return False


def main() -> None:
    """
    Render the MVP Streamlit UI for the Bluesky Semantic Search Engine.
    Features:
    - Title, subtitle, description
    - Query input with explicit submit button (in a form)
    - Clickable example queries (showing actual query text)
    - Results area with dummy response and expandable table
    - Feedback section (text, thumbs up/down emojis, submit button)
    - Input validation and anti-spam guard
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

    error_message = None
    # Query input and submit button in a form
    with st.form(key="query_form"):
        query = st.text_input(
            "Enter your query:", value=st.session_state["query"], key="query_input"
        )
        submit = st.form_submit_button("Submit")
        if submit:
            st.session_state["query"] = query
            st.session_state["show_results"] = False
            # Input validation
            if not query.strip():
                error_message = "Error: Query cannot be empty."
            elif is_sql_injection(query):
                error_message = "Error: Query contains invalid or dangerous text."
            elif contains_profanity(query):
                error_message = "Error: Query contains profanity."
            elif is_too_long(query):
                error_message = "Error: Query is too long (max 1000 characters)."
            elif anti_spam_guard():
                error_message = "Error: Too many queries submitted. Please wait before trying again."
            else:
                st.session_state["show_results"] = True

    if error_message:
        st.error(error_message)

    st.markdown("### Results")
    if (
        st.session_state.get("show_results", False)
        and st.session_state["query"]
        and not error_message
    ):
        # Call FastAPI backend
        try:
            response = requests.get("http://localhost:8000/fetch-results")
            if response.status_code == 200:
                data = response.json()
                posts = data.get("posts", [])
                total_count = data.get("total_count", 0)
                # Placeholder answer
                st.markdown("**[Answer]:** <The user's answer would go here>")
                # De-emphasized total count
                st.markdown(
                    f"<span style='color: grey; font-style: italic;'>Found {total_count} results related to your query.</span>",
                    unsafe_allow_html=True,
                )
                if posts:
                    df = pd.DataFrame(posts)
                    with st.expander("Show results table"):
                        st.dataframe(df)
                    # Export Results button
                    today = datetime.now().strftime("%Y-%m-%d")
                    timestamp = str(datetime.now().timestamp())
                    hash_digest = hashlib.sha256(timestamp.encode()).hexdigest()[:8]
                    filename = f"{today}_results_{hash_digest}.csv"
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    st.download_button(
                        label="Export Results",
                        data=csv_buffer.getvalue(),
                        file_name=filename,
                        mime="text/csv",
                    )
                else:
                    st.info("No results found.")
            else:
                st.error(f"Backend error: {response.status_code}")
        except Exception as e:
            st.error(f"Failed to fetch results from backend: {e}")
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
