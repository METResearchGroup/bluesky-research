from typing import Any, Dict, Tuple
import streamlit as st
import datetime


def render_sna_time_slider_panel(state: Dict[str, Any]) -> None:
    """
    Render the Time Slider Animation panel for the SNA tab.
    Renders a date range slider and updates graph/metrics when the slider is moved.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
    """
    st.subheader("Time Slider Animation")
    start_date = datetime.date(2024, 6, 1)
    end_date = datetime.date(2024, 6, 14)
    date_range = st.slider(
        "Select Date Range",
        min_value=start_date,
        max_value=end_date,
        value=(start_date, end_date),
        format="YYYY-MM-DD",
    )
    update_graph_and_metrics(state, date_range)


def update_graph_and_metrics(
    state: Dict[str, Any], date_range: Tuple[datetime.date, datetime.date]
) -> None:
    """
    Update the state with the selected date range. In a real app, this would update the graph and metrics.
    Args:
        state (Dict[str, Any]): The current filter state for the SNA tab.
        date_range (Tuple[datetime.date, datetime.date]): The selected date range.
    """
    state["date_range"] = date_range
