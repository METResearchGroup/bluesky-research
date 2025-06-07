import streamlit as st
from typing import Dict, Any
import pandas as pd
import io
import datetime
from search_engine.app_v2.filter_state import FilterState
from search_engine.app_v2.sample_data import get_sample_posts

TEMPLATES = [
    {
        "name": "Toxic Posts Last Month",
        "filters": {
            "Temporal": {"date_range": "2024-05-01 to 2024-05-31"},
            "Sentiment": {"toxicity": "Toxic"},
        },
    },
    {
        "name": "Political Posts by Left-Leaners",
        "filters": {
            "Political": {"political": "Yes", "slant": "left"},
        },
    },
    {
        "name": "Climate Hashtag in June",
        "filters": {
            "Temporal": {"date_range": "2024-06-01 to 2024-06-10"},
            "Content": {"hashtags": ["#climate"]},
        },
    },
]

EXPORT_FORMATS = ["CSV", "Parquet"]


def render_export_templates_panel(filter_state: FilterState) -> None:
    """
    Renders the Export Panel with export format selector and export button.
    Only allows export after a query has been submitted.
    Args:
        filter_state: The FilterState object managing current filters.
    """
    st.subheader("Export Results Panel")

    # Only allow export if a query has been submitted
    if not st.session_state.get("show_query_preview", False):
        st.info("Submit a query to enable export.")
        return

    # --- Export Format Selector ---
    export_format = st.radio(
        "Export Format", EXPORT_FORMATS, horizontal=True, key="export_format_radio"
    )

    # --- Estimate Record Count and Data Size (mocked) ---
    sample_posts = get_sample_posts()

    # Apply current filters to sample data
    def row_matches(row: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        # Content: keywords
        keywords = filters.get("Content", {}).get("keywords", [])
        if keywords and not any(kw.lower() in row["text"].lower() for kw in keywords):
            return False
        # Content: hashtags
        hashtags = filters.get("Content", {}).get("hashtags", [])
        if hashtags and not any(tag in row["hashtags"] for tag in hashtags):
            return False
        # Temporal: date_range
        date_range = filters.get("Temporal", {}).get("date_range")
        if date_range:
            try:
                parts = date_range.split(" to ")
                start, end = parts[0].strip(), parts[-1].strip()
                if not (start <= row["date"] <= end):
                    return False
            except Exception:
                return False
        # User: handles
        handles = filters.get("User", {}).get("handles", [])
        if handles and row["user"] not in handles:
            return False
        # Sentiment: valence
        valence = filters.get("Sentiment", {}).get("valence")
        if valence and row.get("valence") != valence:
            return False
        # Sentiment: toxic
        toxicity = filters.get("Sentiment", {}).get("toxicity")
        if toxicity:
            if toxicity == "Toxic" and row.get("toxic") is not True:
                return False
            if toxicity == "Not Toxic" and row.get("toxic") is not False:
                return False
            if toxicity == "Uncertain" and row.get("toxic") is not None:
                return False
        # Political: political
        political = filters.get("Political", {}).get("political")
        if political:
            if political == "Yes" and row.get("political") is not True:
                return False
            if political == "No" and row.get("political") is not False:
                return False
        # Political: slant
        slant = filters.get("Political", {}).get("slant")
        if slant and row.get("slant") != slant:
            return False
        return True

    filtered = [row for row in sample_posts if row_matches(row, filter_state.filters)]
    record_count = len(filtered)
    # Mock data size: assume 1KB per record
    data_size_kb = record_count
    st.write(f"**Estimated records:** {record_count}")
    st.write(f"**Estimated data size:** {data_size_kb} KB")

    # --- Export Button ---
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results_{now}.{export_format.lower()}"
    df = pd.DataFrame(filtered)
    if export_format == "CSV":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        data = buf.getvalue().encode("utf-8")
        mime = "text/csv"
    else:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        data = buf.getvalue()
        mime = "application/octet-stream"
    st.download_button(
        label=f"Export as {export_format}",
        data=data,
        file_name=filename,
        mime=mime,
        key="export_download_button",
    )
