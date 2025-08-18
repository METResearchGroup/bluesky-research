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
    - Results area with backend integration and expandable table
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
        # Call FastAPI backend: classify intent, then route query
        try:
            with st.spinner("Processing your query and generating results..."):
                # Step 1: Classify intent
                intent_response = requests.get(
                    "http://localhost:8000/get-query-intent",
                    params={"query": st.session_state["query"]},
                    timeout=30,
                )
                if intent_response.status_code != 200:
                    st.error("Could not classify query intent (backend error).")
                    return
                intent_data = intent_response.json()
                intent = intent_data.get("intent", "unknown")
                reason = intent_data.get("reason", "")

                # Step 2: Route query
                route_payload = {
                    "intent": intent,
                    "query": st.session_state["query"],
                    "kwargs": {},
                }
                route_response = requests.post(
                    "http://localhost:8000/route-query",
                    json=route_payload,
                    timeout=60,
                )
                if route_response.status_code != 200:
                    st.error(f"Backend error: {route_response.status_code}")
                    return
                data = route_response.json()
                posts = data.get("posts", [])
                total_count = data.get("total_count", 0)
                source = data.get("source", "unknown")

                if not posts:
                    st.info("No results found.")
                    st.markdown("---")
                    st.markdown("#### Classified intent and data source")
                    st.markdown(f"**Intent:** `{intent}`")
                    st.markdown(f"**Reason:** {reason}")
                    st.markdown(f"**Data source:** `{source}`")
                    return

                # Step 3: Summarize results
                summarize_payload = {"posts": posts}
                summarize_response = requests.post(
                    "http://localhost:8000/summarize-results",
                    json=summarize_payload,
                    timeout=60,
                )
                if summarize_response.status_code != 200:
                    st.error(
                        f"Summarizer backend error: {summarize_response.status_code}"
                    )
                    return
                summarizer_output = summarize_response.json()

                # Step 4: Compose answer
                compose_payload = {
                    "summarizer_output": summarizer_output,
                    "posts": posts,
                }
                compose_response = requests.post(
                    "http://localhost:8000/compose-answer",
                    json=compose_payload,
                    timeout=60,
                )
                if compose_response.status_code != 200:
                    st.error(
                        f"Answer composer backend error: {compose_response.status_code}"
                    )
                    return
                answer = compose_response.json()

            # Display AI-generated response
            ai_text = answer.get("text", "")
            # If the text contains lines with 'YYYY-MM-DD: <count>', format as code block
            import re

            date_count_lines = re.findall(r"(\d{4}-\d{2}-\d{2}:\s*\d+)", ai_text)
            if date_count_lines:
                # Split the text into lines, keep the header, then code block for the rest
                header, *rest = ai_text.split("\n")
                st.markdown(f"**[AI-generated response]:** {header}")
                st.code("\n".join(rest))
            else:
                st.markdown(f"**[AI-generated response]:** {ai_text}")

            # Display .head() of DataFrame
            df = pd.DataFrame(answer.get("df", []))
            st.markdown(
                f"<span style='color: grey; font-style: italic;'>Found {total_count} total results.</span>",
                unsafe_allow_html=True,
            )
            if not df.empty:
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
            # Display visuals
            visuals = answer.get("visuals", [])
            for visual in visuals:
                st.markdown(f"**{visual.get('type', '').capitalize()} plot:**")
                st.image(visual.get("path", ""), use_container_width=True)

            # Classified intent and data source section
            st.markdown("---")
            st.markdown("#### Classified intent and data source")
            st.markdown(f"**Intent:** `{intent}`")
            st.markdown(f"**Reason:** {reason}")
            st.markdown(f"**Data source:** `{source}`")
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
