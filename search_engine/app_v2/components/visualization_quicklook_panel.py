import streamlit as st
from typing import List, Dict, Any
from datetime import datetime, timedelta
import re


def generate_daily_counts(
    filters: Dict[str, Any], data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Generates a daily time series of post counts based on filters and sample data.
    Args:
        filters: Nested dict of filters (e.g., {'Temporal': {'date_range': ...}})
        data: List of post dicts to filter.
    Returns:
        List of dicts: [{"date": str, "count": int}, ...] for each date in range.
    """
    # Determine date range
    if "Temporal" in filters and "date_range" in filters["Temporal"]:
        date_range = filters["Temporal"]["date_range"]
        parts = re.split(r"\s*to\s*", date_range)
        start_str, end_str = parts[0].strip(), parts[-1].strip()
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
    else:
        # Use min/max date in data
        if not data:
            return []
        dates = [datetime.strptime(row["date"], "%Y-%m-%d").date() for row in data]
        start_date, end_date = min(dates), max(dates)

    # Build list of all dates in range
    num_days = (end_date - start_date).days + 1
    all_dates = [(start_date + timedelta(days=i)).isoformat() for i in range(num_days)]

    # Filter data using same logic as filter_and_preview_sample_data
    def row_matches(row: Dict[str, Any]) -> bool:
        # Content: hashtags
        hashtags = filters.get("Content", {}).get("hashtags", [])
        if hashtags and not any(tag in row.get("hashtags", []) for tag in hashtags):
            return False
        # Temporal: date_range
        if "Temporal" in filters and "date_range" in filters["Temporal"]:
            if not (start_str <= row["date"] <= end_str):
                return False
        return True

    filtered = [row for row in data if row_matches(row)]
    # Count posts per date
    counts = {d: 0 for d in all_dates}
    for row in filtered:
        if row["date"] in counts:
            counts[row["date"]] += 1
    return [{"date": d, "count": counts[d]} for d in all_dates]


def render_visualization_quicklook_panel(
    filters: Dict[str, Any], data: List[Dict[str, Any]]
) -> None:
    """
    Renders the Visualization Quick-Look panel with a line chart of daily post counts.
    Args:
        filters: Current filter dict.
        data: List of post dicts.
    """
    st.subheader("Visualization Quick-Look")
    daily_counts = generate_daily_counts(filters, data)
    if not daily_counts:
        st.info("No data to visualize for the current filters.")
        return
    import pandas as pd
    import altair as alt

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
