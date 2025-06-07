import streamlit as st
from typing import List, Dict, Any
from datetime import datetime, timedelta, date
import re
import pandas as pd
import altair as alt


def generate_daily_counts_from_filtered(
    filtered_data: List[Dict[str, Any]], filters: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Generates a daily time series of post counts from already filtered data.
    Args:
        filtered_data: List of post dicts that match the filters.
        filters: The filters dict, used to determine the date range.
    Returns:
        List of dicts: [{"date": str, "count": int}, ...] for each date in range.
    """
    if not filtered_data:
        # Determine date range from filters or default to June 1-10
        if "Temporal" in filters and "date_range" in filters["Temporal"]:
            parts = re.split(r"\s*to\s*", filters["Temporal"]["date_range"])
            start_str, end_str = parts[0].strip(), parts[-1].strip()
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        else:
            start_date = date(2024, 6, 1)
            end_date = date(2024, 6, 10)
        num_days = (end_date - start_date).days + 1
        all_dates = [
            (start_date + timedelta(days=i)).isoformat() for i in range(num_days)
        ]
        return [{"date": d, "count": 0} for d in all_dates]
    # Get all dates in filtered data
    dates = [row["date"] for row in filtered_data]
    min_date = min(dates)
    max_date = max(dates)
    # Build list of all dates in range
    start_date = datetime.strptime(min_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(max_date, "%Y-%m-%d").date()
    num_days = (end_date - start_date).days + 1
    all_dates = [(start_date + timedelta(days=i)).isoformat() for i in range(num_days)]
    # Count posts per date
    counts = {d: 0 for d in all_dates}
    for row in filtered_data:
        if row["date"] in counts:
            counts[row["date"]] += 1
    return [{"date": d, "count": counts[d]} for d in all_dates]


def render_visualization_quicklook_panel(
    filters: Dict[str, Any], data: List[Dict[str, Any]]
) -> None:
    """
    Renders the Visualization Quick-Look panel with a line chart of daily post counts for the filtered data.
    Args:
        filters: Current filter dict.
        data: List of post dicts.
    """
    st.subheader("Visualization Quick-Look")

    # Use the same filtering logic as the Query Preview, but no row limit
    def filter_all_sample_data(
        filters: Dict[str, Any], data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        # Remove the 5-row limit from filter_and_preview_sample_data
        def row_matches(row: Dict[str, Any]) -> bool:
            # Content: keywords
            keywords = filters.get("Content", {}).get("keywords", [])
            if keywords and not any(
                kw.lower() in row["text"].lower() for kw in keywords
            ):
                return False
            # Content: hashtags
            hashtags = filters.get("Content", {}).get("hashtags", [])
            if hashtags and not any(tag in row["hashtags"] for tag in hashtags):
                return False
            # Temporal: date_range
            date_range = filters.get("Temporal", {}).get("date_range")
            if date_range:
                try:
                    parts = re.split(r"\s*to\s*", date_range)
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

        return [row for row in data if row_matches(row)]

    filtered_data = filter_all_sample_data(filters, data)
    daily_counts = generate_daily_counts_from_filtered(filtered_data, filters)
    if not daily_counts:
        st.info("No data to visualize for the current filters.")
        return
    df = pd.DataFrame(daily_counts)
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="date"),
            y=alt.Y("count:Q", title="Number of Records"),
            tooltip=["date:T", "count:Q"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)
